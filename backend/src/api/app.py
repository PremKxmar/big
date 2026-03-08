"""
Smart City Traffic - Flask API Server
======================================

REST API backend for the traffic dashboard:
- Serves current traffic state
- Provides ML predictions using the actual Spark MLlib model
- WebSocket for real-time updates

Usage:
    python src/api/app.py
"""

import os
import sys
from pathlib import Path
import json
import random
from datetime import datetime, timedelta
from threading import Thread, Lock
from collections import defaultdict
import time

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# Prometheus metrics (optional - install with: pip install prometheus-flask-exporter)
try:
    from prometheus_flask_exporter import PrometheusMetrics
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    print("⚠ prometheus_flask_exporter not installed. Run: pip install prometheus-flask-exporter")

import math
import pandas as pd
import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Flask app setup
app = Flask(__name__, static_folder='../../dashboard')
CORS(app, resources={r"/api/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize Prometheus metrics if available
if PROMETHEUS_AVAILABLE:
    metrics = PrometheusMetrics(app)
    # Add custom metrics info
    metrics.info('smart_city_traffic', 'Smart City Traffic API', version='1.0.0')
    print("✓ Prometheus metrics enabled at /metrics")

# Configuration
DATA_DIR = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

# NYC Coordinate mapping (from cell indices to real coordinates)
# Cell indices range: lat 0-44, lon 0-55
# Real coordinates: lat 40.4855-40.9175, lon -74.2567 to -73.7007
NYC_LAT_MIN = 40.4855
NYC_LAT_STEP = 0.009600
NYC_LON_MIN = -74.2567
NYC_LON_STEP = 0.009928

def cell_index_to_coords(cell_lat_idx, cell_lon_idx):
    """Convert cell indices to real geographic coordinates."""
    real_lat = NYC_LAT_MIN + (cell_lat_idx + 0.5) * NYC_LAT_STEP  # +0.5 for cell center
    real_lon = NYC_LON_MIN + (cell_lon_idx + 0.5) * NYC_LON_STEP  # +0.5 for cell center
    return real_lat, real_lon

# Global state
traffic_state = {}
spark_session = None
spark_model = None
feature_columns = None
model_type = None  # 'spark' or 'rule-based'
model_info_data = {}
spark_lock = Lock()  # Serialize all PySpark operations (not thread-safe)

# Kafka integration state
kafka_consumer = None
kafka_connected = False
kafka_events_buffer = defaultdict(list)  # cell_id -> list of events in current window
kafka_events_lock = Lock()
kafka_stats = {
    'total_events_received': 0,
    'last_event_time': None,
    'events_per_second': 0,
    'active_cells': 0,
    'last_window_update': None,
    'consumer_status': 'disconnected'
}

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'traffic-events'
KAFKA_WINDOW_SECONDS = 5  # Aggregate every 5 seconds to match frontend polling

def init_spark():
    """Initialize a Spark session for model inference."""
    global spark_session

    try:
        # Windows-specific Hadoop workaround
        if os.name == 'nt':
            hadoop_home = r"C:\hadoop"
            os.environ['HADOOP_HOME'] = hadoop_home
            os.environ['hadoop.home.dir'] = hadoop_home
            # Add hadoop\bin to PATH so JVM can find hadoop.dll (NativeIO)
            if hadoop_home not in os.environ.get('PATH', ''):
                os.environ['PATH'] = os.environ.get('PATH', '') + f";{hadoop_home}\\bin"

        from pyspark.sql import SparkSession

        spark_session = SparkSession.builder \
            .appName("SmartCityTraffic-API") \
            .master("local[1]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.python.worker.reuse", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()

        spark_session.sparkContext.setLogLevel("ERROR")
        print("✓ Spark session initialized for model inference")
        return True
    except Exception as e:
        print(f"✗ Could not initialize Spark: {e}")
        return False


def load_model():
    """
    Load the trained Spark MLlib model for real predictions.
    Falls back to rule-based if Spark is unavailable.
    """
    global spark_model, feature_columns, model_type, model_info_data

    features_path = MODELS_DIR / "feature_columns_spark.json"
    model_info_path = MODELS_DIR / "model_info_spark.json"
    spark_model_path = MODELS_DIR / "spark_congestion_model"

    # Load feature column names
    if features_path.exists():
        with open(features_path, 'r') as f:
            feature_columns = json.load(f)
        print(f"✓ Loaded feature columns: {len(feature_columns)} features")
    else:
        feature_columns = [
            "hour", "day_of_week", "month", "is_weekend", "is_rush_hour", "is_night",
            "cell_lat", "cell_lon", "is_manhattan_int",
            "prev_trip_count", "prev_avg_speed", "prev_congestion_label",
            "prev_2h_trip_count", "prev_2h_avg_speed",
            "historical_avg_trips", "historical_avg_speed"
        ]
        print(f"⚠ Using default feature columns ({len(feature_columns)})")

    # Load model metadata
    if model_info_path.exists():
        with open(model_info_path, 'r') as f:
            model_info_data = json.load(f)
        print(f"✓ Model info: {model_info_data.get('model_type', 'Unknown')}, "
              f"Accuracy: {model_info_data.get('metrics', {}).get('test_accuracy', 'N/A')}")

    # Attempt to load the actual Spark MLlib PipelineModel
    if spark_model_path.exists() and spark_session is not None:
        try:
            from pyspark.ml import PipelineModel

            spark_model = PipelineModel.load(str(spark_model_path))
            model_type = 'spark'
            print(f"✓ Spark MLlib PipelineModel loaded from: {spark_model_path}")
            return
        except Exception as e:
            print(f"⚠ Failed to load Spark model: {e}")

    # Fallback
    model_type = 'rule-based'
    print("⚠ Using rule-based prediction fallback (Spark model not available)")


def predict_congestion(features_dict):
    """
    Make a congestion prediction using the loaded model.

    If the Spark MLlib model is loaded, it creates a single-row DataFrame,
    runs it through the pipeline, and returns the prediction + probability.
    Otherwise falls back to a rule-based heuristic.

    Args:
        features_dict: dict mapping feature column names to their values

    Returns:
        dict with 'prediction' (int), 'level' (str), 'confidence' (float)
    """
    level_map = {0: 'Low', 1: 'Medium', 2: 'High'}

    # ---- Spark MLlib model (thread-safe with lock) ----
    if spark_model is not None and spark_session is not None:
        try:
            with spark_lock:
                row = {col: float(features_dict.get(col, 0)) for col in feature_columns}
                input_df = spark_session.createDataFrame([row])
                predictions_df = spark_model.transform(input_df)

                available_cols = predictions_df.columns
                result = predictions_df.select("prediction").first()
                prediction = int(result["prediction"])

                confidence = 0.80
                if "probability" in available_cols:
                    prob_result = predictions_df.select("probability").first()
                    confidence = float(prob_result["probability"].toArray().max())
                elif "rawPrediction" in available_cols:
                    raw_result = predictions_df.select("rawPrediction").first()
                    raw_vals = raw_result["rawPrediction"].toArray()
                    exp_vals = [math.exp(min(v, 10)) for v in raw_vals]
                    total = sum(exp_vals)
                    confidence = max(exp_vals) / total if total > 0 else 0.80

            return {
                'prediction': prediction,
                'level': level_map.get(prediction, 'Unknown'),
                'confidence': round(confidence, 3),
                'model_used': 'spark-mllib'
            }
        except Exception as e:
            pass

    # ---- Rule-based fallback ----
    return _rule_based_prediction(features_dict)


def predict_congestion_batch(features_list):
    """
    Batch prediction for multiple cells in ONE Spark transform call.
    
    Much more efficient than calling predict_congestion() in a loop because
    PySpark is NOT thread-safe and creating N DataFrames + N transforms
    causes worker crashes.
    
    Args:
        features_list: list of (cell_id, features_dict) tuples
    
    Returns:
        dict mapping cell_id -> prediction result dict
    """
    level_map = {0: 'Low', 1: 'Medium', 2: 'High'}
    results = {}

    if spark_model is not None and spark_session is not None and features_list:
        try:
            with spark_lock:
                # Build ALL rows at once
                rows = []
                cell_ids = []
                for cell_id, features_dict in features_list:
                    row = {col: float(features_dict.get(col, 0)) for col in feature_columns}
                    rows.append(row)
                    cell_ids.append(cell_id)

                # ONE createDataFrame + ONE transform for all cells
                input_df = spark_session.createDataFrame(rows)
                predictions_df = spark_model.transform(input_df)
                available_cols = predictions_df.columns

                # Collect results
                if "probability" in available_cols:
                    collected = predictions_df.select("prediction", "probability").collect()
                    for i, result_row in enumerate(collected):
                        prediction = int(result_row["prediction"])
                        confidence = float(result_row["probability"].toArray().max())
                        results[cell_ids[i]] = {
                            'prediction': prediction,
                            'level': level_map.get(prediction, 'Unknown'),
                            'confidence': round(confidence, 3),
                            'model_used': 'spark-mllib'
                        }
                elif "rawPrediction" in available_cols:
                    collected = predictions_df.select("prediction", "rawPrediction").collect()
                    for i, result_row in enumerate(collected):
                        prediction = int(result_row["prediction"])
                        raw_vals = result_row["rawPrediction"].toArray()
                        exp_vals = [math.exp(min(v, 10)) for v in raw_vals]
                        total = sum(exp_vals)
                        confidence = max(exp_vals) / total if total > 0 else 0.80
                        results[cell_ids[i]] = {
                            'prediction': prediction,
                            'level': level_map.get(prediction, 'Unknown'),
                            'confidence': round(confidence, 3),
                            'model_used': 'spark-mllib'
                        }
                else:
                    collected = predictions_df.select("prediction").collect()
                    for i, result_row in enumerate(collected):
                        prediction = int(result_row["prediction"])
                        results[cell_ids[i]] = {
                            'prediction': prediction,
                            'level': level_map.get(prediction, 'Unknown'),
                            'confidence': 0.80,
                            'model_used': 'spark-mllib'
                        }

            return results
        except Exception as e:
            print(f"⚠ Batch Spark prediction failed: {e}")
            # Fall through to rule-based for all cells

    # Rule-based fallback for all cells
    for cell_id, features_dict in features_list:
        results[cell_id] = _rule_based_prediction(features_dict)
    return results


def _rule_based_prediction(features_dict):
    """Rule-based congestion prediction fallback."""
    level_map = {0: 'Low', 1: 'Medium', 2: 'High'}
    avg_speed = features_dict.get('prev_avg_speed', 15)
    is_rush = features_dict.get('is_rush_hour', 0)
    is_manhattan = features_dict.get('is_manhattan_int', 0)

    if avg_speed < 10 or (avg_speed < 15 and is_rush and is_manhattan):
        prediction, confidence = 2, 0.82
    elif avg_speed < 20:
        prediction, confidence = 1, 0.75
    else:
        prediction, confidence = 0, 0.80

    return {
        'prediction': prediction,
        'level': level_map.get(prediction, 'Unknown'),
        'confidence': confidence,
        'model_used': 'rule-based'
    }


def load_cell_data():
    """Load cell statistics for API responses."""
    global traffic_state
    
    # Try to load Spark training features (actual data from feature engineering)
    features_spark_path = DATA_DIR / "training_features_spark.parquet"
    features_path = DATA_DIR / "training_features.parquet"
    cells_path = DATA_DIR / "cell_statistics.parquet"
    
    # Priority: Spark features > legacy features > cell statistics
    data_loaded = False
    
    if features_spark_path.exists():
        print(f"Loading from training_features_spark.parquet (Spark model data)...")
        df = pd.read_parquet(features_spark_path)
        data_loaded = True
        
        # Group by cell to get unique cells with their average stats (fast vectorized)
        cell_stats = df.groupby(['cell_lat', 'cell_lon']).agg(
            avg_speed=('historical_avg_speed', 'mean'),
            avg_trip_count=('prev_trip_count', 'mean'),
            avg_hour=('hour', 'mean'),
            is_manhattan=('is_manhattan_int', 'first')
        ).reset_index()
        
        for _, row in cell_stats.iterrows():
            cell_id = f"cell_{int(row['cell_lat'])}_{int(row['cell_lon'])}"
            
            # Convert cell indices to real geographic coordinates
            real_lat, real_lon = cell_index_to_coords(int(row['cell_lat']), int(row['cell_lon']))
            
            avg_speed = float(row['avg_speed'])
            
            # Calculate congestion based on speed
            if avg_speed > 20:
                congestion_level = 'low'
                congestion_index = 0.3 - (avg_speed - 20) * 0.01
            elif avg_speed > 10:
                congestion_level = 'medium'
                congestion_index = 0.7 - (avg_speed - 10) * 0.04
            else:
                congestion_level = 'high'
                congestion_index = 1.0 - avg_speed * 0.03
            
            congestion_index = max(0.1, min(0.95, congestion_index))
            
            traffic_state[cell_id] = {
                'cell_id': cell_id,
                'latitude': round(real_lat, 6),
                'longitude': round(real_lon, 6),
                'congestion_index': round(congestion_index, 3),
                'congestion_level': congestion_level,
                'vehicle_count': max(1, int(row['avg_trip_count'])),
                'avg_speed': round(avg_speed, 1),
                'hour': int(row['avg_hour']),
                'is_manhattan': bool(row['is_manhattan']),
                'last_update': datetime.utcnow().isoformat()
            }
        print(f"✓ Loaded {len(traffic_state)} cells from Spark training data")
    
    elif features_path.exists():
        print(f"⚠ Loading from legacy training_features.parquet (consider regenerating with Spark)...")
        df = pd.read_parquet(features_path)
        data_loaded = True
        
        # Group by cell to get unique cells with their stats
        for _, row in df.iterrows():
            cell_id = f"cell_{int(row['cell_lat'])}_{int(row['cell_lon'])}"
            
            # Convert cell indices to real geographic coordinates
            real_lat, real_lon = cell_index_to_coords(int(row['cell_lat']), int(row['cell_lon']))
            
            # Calculate congestion based on speed
            avg_speed = float(row['avg_speed'])
            if avg_speed > 20:
                congestion_level = 'low'
                congestion_index = 0.3 - (avg_speed - 20) * 0.01
            elif avg_speed > 10:
                congestion_level = 'medium'
                congestion_index = 0.7 - (avg_speed - 10) * 0.04
            else:
                congestion_level = 'high'
                congestion_index = 1.0 - avg_speed * 0.03
            
            congestion_index = max(0.1, min(0.95, congestion_index))
            
            traffic_state[cell_id] = {
                'cell_id': cell_id,
                'latitude': round(real_lat, 6),
                'longitude': round(real_lon, 6),
                'congestion_index': round(congestion_index, 3),
                'congestion_level': congestion_level,
                'vehicle_count': int(row['trip_count']),
                'avg_speed': round(avg_speed, 1),
                'hour': int(row['hour']),
                'is_manhattan': bool(row['is_manhattan']),
                'last_update': datetime.utcnow().isoformat()
            }
        print(f"✓ Loaded {len(traffic_state)} cells from legacy training data")
        data_loaded = True
    
    if not data_loaded and cells_path.exists():
        print(f"Loading from cell_statistics.parquet...")
        df = pd.read_parquet(cells_path)
        for _, row in df.iterrows():
            traffic_state[row['cell_id']] = {
                'cell_id': row['cell_id'],
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'congestion_index': float(row['typical_congestion']),
                'congestion_level': row['dominant_congestion'],
                'vehicle_count': int(row['typical_vehicle_count']),
                'avg_speed': float(row['typical_speed']),
                'last_update': datetime.utcnow().isoformat()
            }
        print(f"✓ Loaded {len(traffic_state)} cells from cell_statistics")
        data_loaded = True
    
    if not data_loaded:
        # Generate sample data if no real data exists
        print("⚠ No training data found, generating sample data...")
        generate_sample_data()


def generate_sample_data():
    """Generate sample traffic data for demo."""
    global traffic_state
    
    print("Generating sample traffic data...")
    
    # NYC Manhattan bounds
    lat_min, lat_max = 40.70, 40.82
    lon_min, lon_max = -74.02, -73.93
    
    cell_size = 0.005
    
    for lat in np.arange(lat_min, lat_max, cell_size):
        for lon in np.arange(lon_min, lon_max, cell_size):
            cell_id = f"cell_{int(lat/cell_size)}_{int(lon/cell_size)}"
            
            # Random congestion
            congestion = random.uniform(0.1, 0.95)
            speed = 50 * (1 - congestion) + random.uniform(-5, 5)
            
            traffic_state[cell_id] = {
                'cell_id': cell_id,
                'latitude': float(lat + cell_size/2),
                'longitude': float(lon + cell_size/2),
                'congestion_index': round(congestion, 3),
                'congestion_level': 'high' if congestion > 0.7 else 'medium' if congestion > 0.4 else 'low',
                'vehicle_count': int(congestion * 150),
                'avg_speed': round(max(5, speed), 1),
                'last_update': datetime.utcnow().isoformat()
            }
    
    print(f"Generated {len(traffic_state)} sample cells")


# =============================================================================
# REST API Endpoints
# =============================================================================

@app.route('/')
def index():
    """Serve the dashboard."""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'version': '2.0.0',
        'model_type': model_type or 'rule-based',
        'spark_model_loaded': spark_model is not None,
        'spark_session_active': spark_session is not None,
        'cells_loaded': len(traffic_state),
        'kafka_streaming': {
            'connected': kafka_connected,
            'status': kafka_stats.get('consumer_status', 'disconnected'),
            'events_per_second': kafka_stats.get('events_per_second', 0),
            'total_events': kafka_stats.get('total_events_received', 0),
            'active_cells': kafka_stats.get('active_cells', 0),
            'last_update': kafka_stats.get('last_window_update')
        }
    })


@app.route('/api/kafka-status', methods=['GET'])
def kafka_status():
    """Get Kafka streaming pipeline status."""
    # Count cells by source
    live_cells = sum(1 for c in traffic_state.values() if c.get('source') == 'kafka-live')
    batch_cells = sum(1 for c in traffic_state.values() if c.get('source') != 'kafka-live')

    return jsonify({
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'kafka_connected': kafka_connected,
        'consumer_status': kafka_stats.get('consumer_status', 'disconnected'),
        'topic': KAFKA_TOPIC,
        'window_size_seconds': KAFKA_WINDOW_SECONDS,
        'total_events_received': kafka_stats.get('total_events_received', 0),
        'events_per_second': kafka_stats.get('events_per_second', 0),
        'active_cells_this_window': kafka_stats.get('active_cells', 0),
        'last_window_update': kafka_stats.get('last_window_update'),
        'cells_breakdown': {
            'kafka_live': live_cells,
            'batch_data': batch_cells,
            'total': len(traffic_state)
        }
    })


@app.route('/api/current-traffic', methods=['GET'])
def get_current_traffic():
    """Get current traffic state for all cells."""
    limit = request.args.get('limit', 500, type=int)
    min_congestion = request.args.get('min_congestion', 0.0, type=float)
    
    # Filter and sort by congestion
    cells = [
        cell for cell in traffic_state.values()
        if cell['congestion_index'] >= min_congestion
    ]
    cells = sorted(cells, key=lambda x: x['congestion_index'], reverse=True)[:limit]
    
    return jsonify({
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'total_cells': len(traffic_state),
        'returned_cells': len(cells),
        'data': cells
    })


@app.route('/api/predictions', methods=['GET'])
def get_predictions():
    """Get ML predictions for congestion using Spark MLlib model (batched)."""
    horizon = request.args.get('horizon_minutes', 15, type=int)
    min_confidence = request.args.get('min_confidence', 0.7, type=float)

    current_hour = datetime.now().hour
    now = datetime.now()

    # Build feature vectors for ALL cells first, then predict in ONE batch
    features_list = []  # list of (cell_id, features_dict)

    for cell_id, cell in list(traffic_state.items())[:100]:  # Limit for performance
        features = {
            'hour': current_hour,
            'day_of_week': now.weekday() + 1,
            'month': now.month,
            'is_weekend': 1 if now.weekday() >= 5 else 0,
            'is_rush_hour': 1 if current_hour in [7, 8, 9, 17, 18, 19] else 0,
            'is_night': 1 if current_hour >= 22 or current_hour <= 6 else 0,
            'cell_lat': int(cell_id.split('_')[1]) if '_' in cell_id else 0,
            'cell_lon': int(cell_id.split('_')[2]) if '_' in cell_id else 0,
            'is_manhattan_int': 1 if cell.get('is_manhattan', False) else 0,
            'prev_trip_count': cell.get('vehicle_count', 10),
            'prev_avg_speed': cell.get('avg_speed', 15),
            'prev_congestion_label': (
                0 if cell['congestion_level'] == 'low'
                else 1 if cell['congestion_level'] == 'medium'
                else 2
            ),
            'prev_2h_trip_count': cell.get('vehicle_count', 10) * 0.9,
            'prev_2h_avg_speed': cell.get('avg_speed', 15) * 1.1,
            'historical_avg_trips': cell.get('vehicle_count', 10),
            'historical_avg_speed': cell.get('avg_speed', 15)
        }
        features_list.append((cell_id, features))

    # ONE Spark transform for all cells (prevents worker crashes from concurrent jobs)
    batch_results = predict_congestion_batch(features_list)

    predictions = []
    for cell_id, features in features_list:
        cell = traffic_state.get(cell_id)
        if not cell:
            continue
        result = batch_results.get(cell_id)
        if not result:
            continue

        if result['confidence'] >= min_confidence:
            predictions.append({
                'cell_id': cell_id,
                'latitude': cell['latitude'],
                'longitude': cell['longitude'],
                'current_level': cell['congestion_level'],
                'current_index': round(cell['congestion_index'], 3),
                'predicted_level': result['level'].lower(),
                'predicted_index': round(result['prediction'] / 2.0, 3),
                'confidence': result['confidence'],
                'change_percent': round((result['prediction'] / 2.0 - cell['congestion_index']) * 100, 1),
                'model_used': result['model_used']
            })

    # Sort by predicted congestion
    predictions = sorted(predictions, key=lambda x: x['predicted_index'], reverse=True)

    # Model accuracy from metadata
    ml_metrics = model_info_data.get('metrics', {})
    accuracy = ml_metrics.get('test_accuracy', 0.786)

    return jsonify({
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'prediction_horizon': f'{horizon} minutes',
        'model_type': model_type or 'rule-based',
        'model_accuracy': round(accuracy, 4),
        'predictions': predictions
    })


@app.route('/api/hotspots', methods=['GET'])
def get_hotspots():
    """Get top congested zones."""
    limit = request.args.get('limit', 10, type=int)
    
    # Sort by congestion and get top N
    cells = sorted(
        traffic_state.values(),
        key=lambda x: x['congestion_index'],
        reverse=True
    )[:limit]
    
    hotspots = []
    for rank, cell in enumerate(cells, 1):
        hotspots.append({
            'rank': rank,
            'cell_id': cell['cell_id'],
            'latitude': cell['latitude'],
            'longitude': cell['longitude'],
            'congestion_index': cell['congestion_index'],
            'congestion_level': cell['congestion_level'],
            'vehicle_count': cell['vehicle_count'],
            'avg_speed': cell['avg_speed'],
            'trend': random.choice(['increasing', 'stable', 'decreasing'])
        })
    
    return jsonify({
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'hotspots': hotspots
    })


@app.route('/api/cell/<cell_id>', methods=['GET'])
def get_cell_details(cell_id):
    """Get detailed info for a specific cell with ML prediction."""
    if cell_id not in traffic_state:
        return jsonify({'error': 'Cell not found', 'cell_id': cell_id}), 404
    
    cell = traffic_state[cell_id]
    current_hour = datetime.now().hour

    # Build features and predict
    features = {
        'hour': current_hour,
        'day_of_week': datetime.now().weekday() + 1,
        'month': datetime.now().month,
        'is_weekend': 1 if datetime.now().weekday() >= 5 else 0,
        'is_rush_hour': 1 if current_hour in [7, 8, 9, 17, 18, 19] else 0,
        'is_night': 1 if current_hour >= 22 or current_hour <= 6 else 0,
        'cell_lat': int(cell_id.split('_')[1]) if '_' in cell_id else 0,
        'cell_lon': int(cell_id.split('_')[2]) if '_' in cell_id else 0,
        'is_manhattan_int': 1 if cell.get('is_manhattan', False) else 0,
        'prev_trip_count': cell.get('vehicle_count', 10),
        'prev_avg_speed': cell.get('avg_speed', 15),
        'prev_congestion_label': (
            0 if cell['congestion_level'] == 'low'
            else 1 if cell['congestion_level'] == 'medium'
            else 2
        ),
        'prev_2h_trip_count': cell.get('vehicle_count', 10) * 0.9,
        'prev_2h_avg_speed': cell.get('avg_speed', 15) * 1.1,
        'historical_avg_trips': cell.get('vehicle_count', 10),
        'historical_avg_speed': cell.get('avg_speed', 15)
    }

    result = predict_congestion(features)
    
    return jsonify({
        'cell_id': cell_id,
        'location': {
            'latitude': cell['latitude'],
            'longitude': cell['longitude']
        },
        'current': {
            'congestion_index': cell['congestion_index'],
            'congestion_level': cell['congestion_level'],
            'vehicle_count': cell['vehicle_count'],
            'avg_speed': cell['avg_speed']
        },
        'prediction': {
            'predicted_level': result['level'].lower(),
            'predicted_index': round(result['prediction'] / 2.0, 3),
            'confidence': result['confidence'],
            'model_used': result['model_used'],
            'horizon': '15 minutes'
        },
        'last_update': cell['last_update']
    })


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall statistics."""
    cells = list(traffic_state.values())
    
    if not cells:
        return jsonify({'error': 'No data available'}), 500
    
    congestion_values = [c['congestion_index'] for c in cells]
    vehicle_counts = [c['vehicle_count'] for c in cells]
    speeds = [c['avg_speed'] for c in cells]
    
    levels = [c['congestion_level'] for c in cells]
    
    # Use actual model metadata
    ml_metrics = model_info_data.get('metrics', {})
    
    return jsonify({
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'overview': {
            'total_cells': len(cells),
            'total_vehicles': sum(vehicle_counts),
            'avg_congestion': round(np.mean(congestion_values), 3),
            'avg_speed': round(np.mean(speeds), 1),
            'data_processed': '7+ GB',
            'total_trips': '46M+'
        },
        'congestion_breakdown': {
            'low': levels.count('low'),
            'medium': levels.count('medium'),
            'high': levels.count('high')
        },
        'ml_model': {
            'name': 'Random Forest Classifier (Spark MLlib)',
            'type': model_type or 'rule-based',
            'accuracy': ml_metrics.get('test_accuracy', 0.786),
            'precision': ml_metrics.get('test_precision', 0.787),
            'recall': ml_metrics.get('test_recall', 0.786),
            'f1_score': ml_metrics.get('test_f1', 0.768)
        }
    })


@app.route('/api/geojson/cells', methods=['GET'])
def get_geojson_cells():
    """Get cell data in GeoJSON format for Kepler.gl."""
    features = []
    
    for cell in traffic_state.values():
        # Create hexagon-like polygon (simplified to rectangle)
        lat, lon = cell['latitude'], cell['longitude']
        size = 0.0025  # Half cell size
        
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[
                    [lon - size, lat - size],
                    [lon + size, lat - size],
                    [lon + size, lat + size],
                    [lon - size, lat + size],
                    [lon - size, lat - size]
                ]]
            },
            'properties': {
                'cell_id': cell['cell_id'],
                'congestion_index': cell['congestion_index'],
                'congestion_level': cell['congestion_level'],
                'vehicle_count': cell['vehicle_count'],
                'avg_speed': cell['avg_speed'],
                'height': int(cell['congestion_index'] * 1000)  # For 3D extrusion
            }
        }
        features.append(feature)
    
    return jsonify({
        'type': 'FeatureCollection',
        'features': features
    })


@app.route('/api/geojson/vehicles', methods=['GET'])
def get_geojson_vehicles():
    """Get simulated vehicle positions in GeoJSON format."""
    limit = request.args.get('limit', 500, type=int)
    
    features = []
    
    for cell in random.sample(list(traffic_state.values()), min(limit, len(traffic_state))):
        # Generate random vehicles within cell
        for _ in range(random.randint(1, 5)):
            lat = cell['latitude'] + random.uniform(-0.002, 0.002)
            lon = cell['longitude'] + random.uniform(-0.002, 0.002)
            speed = max(0, cell['avg_speed'] + random.uniform(-5, 5))
            
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat]
                },
                'properties': {
                    'vehicle_id': f"taxi_{random.randint(10000, 99999)}",
                    'speed': round(speed, 1),
                    'heading': random.randint(0, 359),
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            features.append(feature)
    
    return jsonify({
        'type': 'FeatureCollection',
        'features': features[:limit]
    })


# =============================================================================
# WebSocket Events
# =============================================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    print(f"Client connected: {request.sid}")
    emit('connected', {'status': 'connected', 'timestamp': datetime.utcnow().isoformat()})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    print(f"Client disconnected: {request.sid}")


@socketio.on('subscribe')
def handle_subscribe(data):
    """Handle subscription to updates."""
    print(f"Client subscribed: {data}")
    emit('subscribed', {'status': 'subscribed', 'channel': data.get('channel', 'traffic')})


def kafka_consumer_thread():
    """Background thread: consume events from Kafka and buffer them by cell."""
    global kafka_consumer, kafka_connected, kafka_stats

    try:
        from kafka import KafkaConsumer
        print(f"\n📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
        kafka_consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='smart-city-api',
            consumer_timeout_ms=1000,  # Poll timeout
            max_poll_records=500
        )
        kafka_connected = True
        kafka_stats['consumer_status'] = 'connected'
        print(f"✓ Kafka consumer connected — reading from topic '{KAFKA_TOPIC}'")
    except Exception as e:
        kafka_stats['consumer_status'] = f'failed: {e}'
        print(f"✗ Kafka consumer failed to connect: {e}")
        print("  → Running without live streaming (batch data only)")
        return

    # Continuously read events and buffer them
    event_count_window = 0
    window_start = time.time()

    while True:
        try:
            # Poll for messages (non-blocking with timeout)
            records = kafka_consumer.poll(timeout_ms=500, max_records=1000)

            for tp, messages in records.items():
                for message in messages:
                    event = message.value
                    cell_id = event.get('cell_id', 'unknown')

                    with kafka_events_lock:
                        kafka_events_buffer[cell_id].append(event)

                    kafka_stats['total_events_received'] += 1
                    kafka_stats['last_event_time'] = datetime.utcnow().isoformat()
                    event_count_window += 1

            # Calculate events/sec every second
            elapsed = time.time() - window_start
            if elapsed >= 1.0:
                kafka_stats['events_per_second'] = round(event_count_window / elapsed)
                event_count_window = 0
                window_start = time.time()

        except Exception as e:
            print(f"⚠ Kafka consumer error: {e}")
            time.sleep(1)


def kafka_aggregator_thread():
    """Background thread: every KAFKA_WINDOW_SECONDS, aggregate buffered events
       and update traffic_state with live data from Kafka."""
    global kafka_stats

    print(f"⏱  Kafka aggregator started — window size: {KAFKA_WINDOW_SECONDS}s")

    while True:
        time.sleep(KAFKA_WINDOW_SECONDS)

        # Swap out the buffer atomically
        with kafka_events_lock:
            current_buffer = dict(kafka_events_buffer)
            kafka_events_buffer.clear()

        if not current_buffer:
            continue  # No events this window

        cells_updated = 0

        for cell_id, events in current_buffer.items():
            if not events:
                continue

            # Aggregate: avg speed, count, avg hour, location
            speeds = [e.get('speed', 15) for e in events]
            hours = [e.get('hour', 12) for e in events]
            avg_speed = sum(speeds) / len(speeds)
            trip_count = len(events)
            avg_hour = sum(hours) / len(hours)

            # Get location from first event
            lat = events[0].get('latitude', 40.75)
            lon = events[0].get('longitude', -73.98)
            cell_lat = events[0].get('cell_lat', 0)
            cell_lon = events[0].get('cell_lon', 0)
            is_manhattan = events[0].get('is_manhattan', 0)

            # Calculate congestion index from live speed
            if avg_speed > 20:
                congestion_level = 'low'
                congestion_index = max(0.05, 0.3 - (avg_speed - 20) * 0.01)
            elif avg_speed > 10:
                congestion_level = 'medium'
                congestion_index = 0.7 - (avg_speed - 10) * 0.04
            else:
                congestion_level = 'high'
                congestion_index = min(0.98, 1.0 - avg_speed * 0.03)

            congestion_index = max(0.05, min(0.98, congestion_index))

            # Update traffic_state with LIVE data
            traffic_state[cell_id] = {
                'cell_id': cell_id,
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'congestion_index': round(congestion_index, 3),
                'congestion_level': congestion_level,
                'vehicle_count': trip_count,
                'avg_speed': round(avg_speed, 1),
                'hour': int(avg_hour),
                'is_manhattan': bool(is_manhattan),
                'last_update': datetime.utcnow().isoformat(),
                'source': 'kafka-live'  # Mark as live data
            }
            cells_updated += 1

        kafka_stats['active_cells'] = cells_updated
        kafka_stats['last_window_update'] = datetime.utcnow().isoformat()

        # Emit real-time update to frontend via WebSocket
        socketio.emit('traffic_update', {
            'type': 'kafka_live_update',
            'timestamp': datetime.utcnow().isoformat(),
            'updated_cells': cells_updated,
            'events_in_window': sum(len(v) for v in current_buffer.values()),
            'events_per_second': kafka_stats['events_per_second']
        })

        if cells_updated > 0:
            print(f"  📊 Kafka window: {sum(len(v) for v in current_buffer.values())} events → "
                  f"{cells_updated} cells updated | Rate: {kafka_stats['events_per_second']} evt/s")


def background_updater():
    """Background thread: fallback updater when Kafka is not connected.
       Only makes small random changes to keep the UI alive."""
    while True:
        time.sleep(5)

        # If Kafka is feeding live data, skip random noise
        if kafka_connected and kafka_stats.get('events_per_second', 0) > 0:
            continue

        # Fallback: small random perturbations so UI doesn't look frozen
        for cell_id in random.sample(list(traffic_state.keys()), min(30, len(traffic_state))):
            cell = traffic_state[cell_id]
            change = random.uniform(-0.02, 0.02)
            new_congestion = min(1.0, max(0.0, cell['congestion_index'] + change))
            cell['congestion_index'] = round(new_congestion, 3)
            cell['congestion_level'] = 'high' if new_congestion > 0.7 else 'medium' if new_congestion > 0.4 else 'low'
            cell['avg_speed'] = round(50 * (1 - new_congestion) + random.uniform(-2, 2), 1)
            cell['last_update'] = datetime.utcnow().isoformat()
            cell['source'] = 'simulated'

        socketio.emit('traffic_update', {
            'type': 'fallback_update',
            'timestamp': datetime.utcnow().isoformat(),
            'updated_cells': 30
        })


# =============================================================================
# Main
# =============================================================================

def main():
    """Main execution function."""
    print("="*60)
    print("SMART CITY TRAFFIC - API SERVER")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize Spark session (for Spark MLlib model inference)
    init_spark()

    # Load model (tries Spark MLlib first, falls back to rule-based)
    load_model()

    # Load cell data
    load_cell_data()
    
    # Start Kafka consumer + aggregator threads (non-blocking, graceful fallback)
    kafka_thread = Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()
    print("📡 Kafka consumer thread started")

    aggregator_thread = Thread(target=kafka_aggregator_thread, daemon=True)
    aggregator_thread.start()
    print("⏱  Kafka aggregator thread started")

    # Start fallback background updater (only active when Kafka is not connected)
    updater_thread = Thread(target=background_updater, daemon=True)
    updater_thread.start()
    print("🔄 Fallback updater started (active only when Kafka is offline)")
    
    # Run server
    print("\n" + "="*60)
    print(f"Model Type: {model_type}")
    print(f"Spark Model Loaded: {spark_model is not None}")
    print(f"Cells Loaded: {len(traffic_state)}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Kafka Window: {KAFKA_WINDOW_SECONDS}s")
    print("="*60)
    print("API Server starting on http://localhost:5000")
    print("="*60)
    print("\nEndpoints:")
    print("  GET  /api/health          - Health check + Kafka status")
    print("  GET  /api/current-traffic - Current congestion (live from Kafka)")
    print("  GET  /api/predictions     - ML predictions (Spark MLlib + live data)")
    print("  GET  /api/hotspots        - Top congested zones")
    print("  GET  /api/kafka-status    - Kafka streaming pipeline status")
    print("  GET  /api/cell/<id>       - Cell details + prediction")
    print("  GET  /api/stats           - Statistics")
    print("  GET  /api/geojson/cells   - GeoJSON for Kepler.gl")
    print("  GET  /api/geojson/vehicles - Vehicle positions")
    print("  WS   /socket.io           - Real-time updates")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Server error: {e}")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
