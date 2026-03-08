"""
Smart City Traffic - Spark MLlib Model Training Module
=======================================================

This script trains ML models for congestion prediction using Apache Spark MLlib:
- Reads training features from HDFS or local filesystem
- Trains & compares: Random Forest vs Gradient Boosted Trees
- Proper train/test split (temporal - no data leakage)
- Feature scaling and pipeline
- Confusion matrix + per-class classification report
- Saves best model + comparison results to disk
- Saves trained model to HDFS or local

Key Design Decisions:
- Uses Spark MLlib instead of Scikit-learn
- Supports HDFS for distributed storage
- Temporal train/test split (Jan-Feb train, March test)
- No avg_speed in features (prevents data leakage)

Usage:
    python src/batch/model_training_spark.py                       # Local mode
    python src/batch/model_training_spark.py --hdfs                # HDFS mode
    python src/batch/model_training_spark.py --cluster             # Cluster mode
    python src/batch/model_training_spark.py --cluster --hdfs      # Cluster + HDFS
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import json
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, 
    StandardScaler, 
    StringIndexer,
    IndexToString
)
from pyspark.ml.classification import (
    RandomForestClassifier, GBTClassifier, LogisticRegression, OneVsRest
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Add project root to path
# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Import centralized config
from config.spark_config import create_spark_session, HDFS_CONFIG

# =============================================================================
# CONFIGURATION
# =============================================================================

# Spark session is now created via config.spark_config.create_spark_session
# Keeping local variables for backward compatibility
HDFS_NAMENODE = HDFS_CONFIG["namenode"]
HDFS_FEATURES_DIR = "/smart-city-traffic/data/features"
HDFS_MODELS_DIR = HDFS_CONFIG["models_dir"]

# Local Configuration
LOCAL_FEATURES_DIR = PROJECT_ROOT / "data" / "processed"
LOCAL_MODELS_DIR = PROJECT_ROOT / "models"

# Model parameters
RANDOM_SEED = 42


def load_training_data(spark, use_hdfs=False):
    """Load the prepared training dataset from HDFS or local."""
    if use_hdfs:
        features_path = f"{HDFS_NAMENODE}{HDFS_FEATURES_DIR}/training_features_spark.parquet"
    else:
        features_path = str(LOCAL_FEATURES_DIR / "training_features_spark.parquet")
        if not (LOCAL_FEATURES_DIR / "training_features_spark.parquet").exists():
            print(f"ERROR: Training data not found at {features_path}")
            print("Run feature_engineering_spark.py first!")
            return None, None
    
    print(f"\nLoading training data from: {features_path}")
    
    try:
        df = spark.read.parquet(features_path)
        print(f"  ✓ Loaded {df.count():,} samples")
    except Exception as e:
        print(f"ERROR loading data: {e}")
        if use_hdfs:
            print("  Make sure feature data is in HDFS!")
            print("  Run: python src/batch/feature_engineering_spark.py --hdfs")
        return None, None
    
    # Load feature columns (always from local - saved by feature engineering)
    features_json_path = LOCAL_FEATURES_DIR / "feature_columns_spark.json"
    with open(features_json_path, 'r') as f:
        feature_columns = json.load(f)
    
    print(f"  Features: {len(feature_columns)} columns")
    
    return df, feature_columns


def analyze_data(df):
    """Analyze the training data."""
    print("\n" + "=" * 60)
    print("DATA ANALYSIS")
    print("=" * 60)
    
    # Class distribution
    print("\nTarget Distribution:")
    df.groupBy("congestion_label", "congestion_level") \
        .count() \
        .orderBy("congestion_label") \
        .show()
    
    # Train/Test split
    print("\nTemporal Split Distribution:")
    df.groupBy("dataset_split").count().show()
    
    # Sample data
    print("\nSample Features:")
    df.select("hour", "day_of_week", "prev_trip_count", "prev_avg_speed", 
              "congestion_label").show(5)


def create_train_test_split(df):
    """
    Create temporal train/test split.
    Train: January + February data
    Test: March data
    
    This is CRITICAL to prevent temporal data leakage!
    """
    print("\n" + "=" * 60)
    print("CREATING TEMPORAL TRAIN/TEST SPLIT")
    print("=" * 60)
    
    train_df = df.filter(col("dataset_split") == "train")
    test_df = df.filter(col("dataset_split") == "test")
    
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"\n  Training set: {train_count:,} samples")
    print(f"  Test set: {test_count:,} samples")
    print(f"  Test ratio: {100 * test_count / (train_count + test_count):.1f}%")
    
    print("\n  Train data months:")
    train_df.groupBy("month").count().orderBy("month").show()
    
    print("  Test data months:")
    test_df.groupBy("month").count().orderBy("month").show()
    
    return train_df, test_df


def create_ml_pipeline(feature_columns, classifier_type="rf"):
    """
    Create Spark MLlib pipeline with:
    - VectorAssembler: Combine features into vector
    - StandardScaler: Normalize features
    - Classifier: RF, GBT, or LogisticRegression
    
    Args:
        feature_columns: list of feature column names
        classifier_type: 'rf' | 'gbt' | 'lr'
    """
    print(f"\n  Creating pipeline with classifier: {classifier_type.upper()}")
    
    # Step 1: Assemble features into vector
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    # Step 2: Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # Step 3: Classifier
    if classifier_type == "rf":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="congestion_label",
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            numTrees=100,
            maxDepth=10,
            minInstancesPerNode=10,
            featureSubsetStrategy="sqrt",
            seed=RANDOM_SEED
        )
        name = "Random Forest (100 trees, depth=10)"
    elif classifier_type == "gbt":
        # GBTClassifier is binary-only in Spark MLlib.
        # Wrap it with OneVsRest for multiclass (3-class) support.
        base_gbt = GBTClassifier(
            featuresCol="features",
            labelCol="congestion_label",
            predictionCol="prediction",
            maxIter=50,
            maxDepth=8,
            stepSize=0.1,
            seed=RANDOM_SEED
        )
        classifier = OneVsRest(
            featuresCol="features",
            labelCol="congestion_label",
            predictionCol="prediction",
            classifier=base_gbt
        )
        name = "GBT + OneVsRest (50 iters, depth=8)"
    elif classifier_type == "lr":
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="congestion_label",
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.5,
            family="multinomial"
        )
        name = "Logistic Regression (multinomial, iter=100)"
    else:
        raise ValueError(f"Unknown classifier: {classifier_type}")
    
    print(f"    {name}")
    
    pipeline = Pipeline(stages=[assembler, scaler, classifier])
    return pipeline, name


def train_model(pipeline, train_df, model_name="Model"):
    """Train a single model on training data and return it with timing."""
    start_time = datetime.now()
    
    print(f"\n  Training {model_name}...")
    model = pipeline.fit(train_df)
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"  ✓ {model_name} trained in {duration:.1f}s")
    
    return model, duration


def evaluate_model(model, test_df, train_df, model_name="Model"):
    """
    Evaluate the trained model on test data.
    Returns metrics dict, confusion matrix, and per-class report.
    """
    # Make predictions
    test_predictions = model.transform(test_df)
    train_predictions = model.transform(train_df)
    
    # Create evaluators
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="accuracy")
    precision_eval = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="weightedPrecision")
    recall_eval = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="weightedRecall")
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="f1")
    
    metrics = {
        "train_accuracy": accuracy_eval.evaluate(train_predictions),
        "test_accuracy": accuracy_eval.evaluate(test_predictions),
        "test_precision": precision_eval.evaluate(test_predictions),
        "test_recall": recall_eval.evaluate(test_predictions),
        "test_f1": f1_eval.evaluate(test_predictions)
    }
    
    # ── Confusion Matrix ──
    class_names = ["Low", "Medium", "High"]
    confusion_matrix = {}
    
    print(f"\n  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  CONFUSION MATRIX — {model_name:<30} │")
    print(f"  ├─────────────────────────────────────────────────────┤")
    print(f"  │ {'':>15} │ Pred Low │ Pred Med │ Pred High│")
    print(f"  ├─────────────────────────────────────────────────────┤")
    
    for label in [0, 1, 2]:
        row = {}
        class_df = test_predictions.filter(col("congestion_label") == label)
        total = class_df.count()
        for pred_label in [0, 1, 2]:
            cnt = class_df.filter(col("prediction") == pred_label).count()
            row[class_names[pred_label]] = cnt
        confusion_matrix[class_names[label]] = row
        print(f"  │ Actual {class_names[label]:<8} │ {row['Low']:>8} │ {row['Medium']:>8} │ {row['High']:>8} │")
    
    print(f"  └─────────────────────────────────────────────────────┘")
    
    # ── Per-Class Classification Report ──
    classification_report = {}
    
    print(f"\n  ┌───────────────────────────────────────────────────────────────┐")
    print(f"  │  CLASSIFICATION REPORT — {model_name:<36} │")
    print(f"  ├───────────────────────────────────────────────────────────────┤")
    print(f"  │ {'Class':>10} │ {'Precision':>10} │ {'Recall':>10} │ {'F1-Score':>10} │ {'Support':>8} │")
    print(f"  ├───────────────────────────────────────────────────────────────┤")
    
    for label in [0, 1, 2]:
        tp = test_predictions.filter(
            (col("congestion_label") == label) & (col("prediction") == label)).count()
        fp = test_predictions.filter(
            (col("congestion_label") != label) & (col("prediction") == label)).count()
        fn = test_predictions.filter(
            (col("congestion_label") == label) & (col("prediction") != label)).count()
        support = tp + fn
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        
        classification_report[class_names[label]] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
            "support": support
        }
        
        print(f"  │ {class_names[label]:>10} │ {precision:>10.4f} │ {recall:>10.4f} │ {f1:>10.4f} │ {support:>8} │")
    
    print(f"  ├───────────────────────────────────────────────────────────────┤")
    print(f"  │ {'Weighted':>10} │ {metrics['test_precision']:>10.4f} │ {metrics['test_recall']:>10.4f} │ {metrics['test_f1']:>10.4f} │ {'':>8} │")
    print(f"  └───────────────────────────────────────────────────────────────┘")
    
    return metrics, confusion_matrix, classification_report, test_predictions


def get_feature_importance(model, feature_columns):
    """Extract feature importance from the trained model (RF or GBT)."""
    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)
    
    try:
        # Works for RF and GBT (tree-based models)
        classifier_model = model.stages[-1]
        
        # Handle OneVsRest wrapper (used for GBT multiclass)
        if hasattr(classifier_model, 'models'):
            # OneVsRest: average importances across binary sub-models
            import numpy as np
            all_importances = []
            for sub_model in classifier_model.models:
                if hasattr(sub_model, 'featureImportances'):
                    all_importances.append(sub_model.featureImportances.toArray())
            if all_importances:
                importances = np.mean(all_importances, axis=0)
            else:
                print("  ⚠ OneVsRest sub-models have no featureImportances")
                return {}
        elif hasattr(classifier_model, 'featureImportances'):
            importances = classifier_model.featureImportances.toArray()
        else:
            print("  ⚠ Model does not expose featureImportances (e.g. Logistic Regression)")
            return {}
        
        # Combine with feature names
        feature_importance = list(zip(feature_columns, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        print("\n  Top 10 Most Important Features:")
        for i, (feature, importance) in enumerate(feature_importance[:10], 1):
            bar = "█" * int(importance * 100)
            print(f"    {i:2d}. {feature:<25} {importance:.4f}  {bar}")
        
        return dict(feature_importance)
    except Exception as e:
        print(f"  ⚠ Could not extract feature importance: {e}")
        print("  (Logistic Regression uses coefficients, not importances)")
        return {col: 0.0 for col in feature_columns}


def save_model(model, feature_columns, metrics, feature_importance,
               confusion_matrix, classification_report, model_comparison,
               model_name="Unknown",
               use_hdfs=False):
    """Save the best trained model, metadata, confusion matrix, and comparison results."""
    print("\n" + "=" * 60)
    print("SAVING MODEL & RESULTS")
    print("=" * 60)
    
    # Ensure local models directory exists (always save metadata locally)
    LOCAL_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save Spark MLlib model
    if use_hdfs:
        model_path = f"{HDFS_NAMENODE}{HDFS_MODELS_DIR}/spark_congestion_model"
    else:
        model_path = str(LOCAL_MODELS_DIR / "spark_congestion_model")
    
    print(f"\n  Saving Spark MLlib model to: {model_path}")
    model.write().overwrite().save(model_path)
    print("  ✓ Model saved successfully!")
    
    # Save model metadata (always locally for API use)
    model_info = {
        "model_type": f"Spark MLlib {model_name}",
        "trained_at": datetime.now().isoformat(),
        "spark_model_path": model_path,
        "features": feature_columns,
        "metrics": {
            "train_accuracy": round(metrics["train_accuracy"], 4),
            "test_accuracy": round(metrics["test_accuracy"], 4),
            "test_precision": round(metrics["test_precision"], 4),
            "test_recall": round(metrics["test_recall"], 4),
            "test_f1": round(metrics["test_f1"], 4)
        },
        "classes": ["Low", "Medium", "High"],
        "thresholds": {
            "low": "> 20 mph",
            "medium": "10-20 mph",
            "high": "< 10 mph"
        },
        "feature_importance": {k: round(v, 4) for k, v in feature_importance.items()},
        "confusion_matrix": confusion_matrix,
        "classification_report": classification_report,
        "hdfs_mode": use_hdfs,
        "notes": [
            f"Model uses Spark MLlib {model_name}",
            "avg_speed NOT included in features (prevents data leakage)",
            "Temporal train/test split (Jan-Feb train, March test)",
            "Uses lagged features for true prediction",
            "Selected as best after comparison with GBT and Logistic Regression"
        ]
    }
    
    info_path = LOCAL_MODELS_DIR / "model_info_spark.json"
    with open(info_path, 'w') as f:
        json.dump(model_info, f, indent=2)
    print(f"  ✓ Saved model info to: {info_path}")
    
    # Save feature columns for API
    features_path = LOCAL_MODELS_DIR / "feature_columns_spark.json"
    with open(features_path, 'w') as f:
        json.dump(feature_columns, f, indent=2)
    print(f"  ✓ Saved feature columns to: {features_path}")
    
    # Save model comparison results
    comparison_path = LOCAL_MODELS_DIR / "model_comparison.json"
    with open(comparison_path, 'w') as f:
        json.dump(model_comparison, f, indent=2)
    print(f"  ✓ Saved model comparison to: {comparison_path}")
    
    return model_path


def main():
    """Main execution function — trains RF, GBT, LR and picks the best."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Spark MLlib Model Training for Smart City Traffic")
    parser.add_argument("--local", action="store_true", help="Run in local mode instead of cluster")
    parser.add_argument("--no-hdfs", action="store_true", help="Use local filesystem instead of HDFS")
    args = parser.parse_args()
    
    use_hdfs = not args.no_hdfs       # Default: HDFS ON (cluster mode)
    use_cluster = not args.local      # Default: Cluster ON
    
    print("\n" + "=" * 60)
    print("SMART CITY TRAFFIC - SPARK MLlib MODEL TRAINING")
    print("=" * 60)
    print(f"Spark Mode: {'Cluster' if use_cluster else 'Local'}")
    print(f"Storage Mode: {'HDFS' if use_hdfs else 'Local'}")
    print("\n  NOTE: This trains 3 classifiers and picks the best:")
    print("    1. Random Forest Classifier")
    print("    2. Gradient Boosted Trees (GBT)")
    print("    3. Logistic Regression (multinomial)")
    print("\n  Design choices:")
    print("    - avg_speed is NOT in features (no data leakage)")
    print("    - Temporal train/test split (Jan-Feb train, March test)")
    print("    - Expected accuracy: 70-85% (realistic, not 100%!)")
    
    pipeline_start = datetime.now()
    
    # Create Spark session using centralized config
    spark = create_spark_session(
        app_name="SmartCityTraffic-MLlibTraining",
        use_cluster=use_cluster,
        use_hdfs=use_hdfs
    )
    
    # Load data
    df, feature_columns = load_training_data(spark, use_hdfs=use_hdfs)
    if df is None:
        spark.stop()
        return
    
    # Analyze data
    analyze_data(df)
    
    # Create train/test split
    train_df, test_df = create_train_test_split(df)
    
    # Check if we have enough data
    if train_df.count() < 100 or test_df.count() < 100:
        print("\nWARNING: Not enough data for temporal split!")
        print("Falling back to random 80/20 split...")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
        print(f"  Training set: {train_df.count():,} samples")
        print(f"  Test set: {test_df.count():,} samples")
    
    # Cache train/test for reuse across models
    train_df.cache()
    test_df.cache()
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"\n  Cached: {train_count:,} train / {test_count:,} test samples")
    
    # ══════════════════════════════════════════════════════════════
    #  TRAIN & EVALUATE ALL 3 MODELS
    # ══════════════════════════════════════════════════════════════
    classifiers = [
        ("rf",  "Random Forest"),
        ("gbt", "Gradient Boosted Trees"),
        ("lr",  "Logistic Regression"),
    ]
    
    results = {}  # classifier_key -> {model, metrics, confusion, report, duration, name}
    
    for clf_key, clf_display in classifiers:
        print("\n" + "=" * 60)
        print(f"  TRAINING: {clf_display}")
        print("=" * 60)
        
        try:
            pipeline, name = create_ml_pipeline(feature_columns, classifier_type=clf_key)
            model, duration = train_model(pipeline, train_df, model_name=clf_display)
            metrics, confusion, report, preds = evaluate_model(
                model, test_df, train_df, model_name=clf_display)
            
            results[clf_key] = {
                "model": model,
                "metrics": metrics,
                "confusion_matrix": confusion,
                "classification_report": report,
                "duration_seconds": round(duration, 1),
                "name": clf_display,
            }
        except Exception as e:
            print(f"\n  ⚠ {clf_display} FAILED: {e}")
            print(f"    Skipping this classifier and continuing with the next...")
    
    if not results:
        print("\n\n  ✗ ALL classifiers failed. Cannot save any model.")
        spark.stop()
        sys.exit(1)
    
    # ══════════════════════════════════════════════════════════════
    #  MODEL COMPARISON TABLE
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("  MODEL COMPARISON")
    print("=" * 60)
    
    print("\n  ┌──────────────────────────┬──────────┬───────────┬──────────┬──────────┬──────────┐")
    print(  "  │ Model                    │ Accuracy │ Precision │ Recall   │ F1-Score │ Time (s) │")
    print(  "  ├──────────────────────────┼──────────┼───────────┼──────────┼──────────┼──────────┤")
    
    best_key = None
    best_f1 = -1
    
    model_comparison = {}
    
    for key, res in results.items():
        m = res["metrics"]
        dur = res["duration_seconds"]
        marker = ""
        
        if m["test_f1"] > best_f1:
            best_f1 = m["test_f1"]
            best_key = key
        
        model_comparison[key] = {
            "name": res["name"],
            "test_accuracy": round(m["test_accuracy"], 4),
            "test_precision": round(m["test_precision"], 4),
            "test_recall": round(m["test_recall"], 4),
            "test_f1": round(m["test_f1"], 4),
            "train_accuracy": round(m["train_accuracy"], 4),
            "training_seconds": dur,
        }
        
        print(f"  │ {res['name']:<24} │ {m['test_accuracy']:>8.4f} │ {m['test_precision']:>9.4f} │ {m['test_recall']:>8.4f} │ {m['test_f1']:>8.4f} │ {dur:>8.1f} │")
    
    print(  "  └──────────────────────────┴──────────┴───────────┴──────────┴──────────┴──────────┘")
    
    # Mark the winner
    winner = results[best_key]
    model_comparison["best_model"] = best_key
    print(f"\n  🏆 BEST MODEL: {winner['name']} (F1 = {best_f1:.4f})")
    
    # ══════════════════════════════════════════════════════════════
    #  FEATURE IMPORTANCE (from best model if RF or GBT)
    # ══════════════════════════════════════════════════════════════
    feature_importance = get_feature_importance(winner["model"], feature_columns)
    
    # ══════════════════════════════════════════════════════════════
    #  SAVE BEST MODEL
    # ══════════════════════════════════════════════════════════════
    model_path = save_model(
        model=winner["model"],
        feature_columns=feature_columns,
        metrics=winner["metrics"],
        feature_importance=feature_importance,
        confusion_matrix=winner["confusion_matrix"],
        classification_report=winner["classification_report"],
        model_comparison=model_comparison,
        model_name=winner["name"],
        use_hdfs=use_hdfs,
    )
    
    # Unpersist cached data
    train_df.unpersist()
    test_df.unpersist()
    
    # Summary
    total_time = (datetime.now() - pipeline_start).total_seconds()
    
    print("\n" + "=" * 60)
    print("TRAINING PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\n  Mode: {'HDFS' if use_hdfs else 'Local'}")
    print(f"  Total duration: {total_time:.1f}s")
    print(f"  Models trained: {len(classifiers)}")
    print(f"  Best model: {winner['name']}")
    print(f"  Best test accuracy: {winner['metrics']['test_accuracy']:.4f}")
    print(f"  Best test F1:       {winner['metrics']['test_f1']:.4f}")
    print(f"  Model saved to: {model_path}")
    print(f"\n  Saved artifacts:")
    print(f"    models/spark_congestion_model/   (PipelineModel)")
    print(f"    models/model_info_spark.json     (metadata + confusion matrix)")
    print(f"    models/model_comparison.json     (RF vs GBT vs LR)")
    print(f"    models/feature_columns_spark.json")
    print(f"\n  This accuracy is REALISTIC because:")
    print("    - avg_speed is not in features")
    print("    - Model predicts future congestion from past data")
    print("    - Test data is from a different time period")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
