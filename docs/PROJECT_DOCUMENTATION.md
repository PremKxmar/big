# 🏙️ Smart City Real-Time Traffic Simulation & Predictive Analytics

## Complete Project Documentation

> **Final Year Big Data Project** | December 2025

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [Problem Statement & 5 V's of Big Data](#2-problem-statement--5-vs-of-big-data)
3. [System Architecture](#3-system-architecture)
4. [Technology Stack](#4-technology-stack)
5. [Data Pipeline](#5-data-pipeline)
6. [Machine Learning Model](#6-machine-learning-model)
7. [Backend API](#7-backend-api)
8. [Frontend Dashboard](#8-frontend-dashboard)
9. [Project Structure](#9-project-structure)
10. [How to Run](#10-how-to-run)
11. [Requirements Satisfaction](#11-requirements-satisfaction)
12. [Screenshots & Features](#12-screenshots--features)
13. [Future Enhancements](#13-future-enhancements)

---

## 1. Project Overview

### What is this project?

This project is a **Real-Time Traffic Simulation and Prediction System** that processes over **7 GB of NYC Taxi trip data** (46+ million trips) to:

- 🚕 Visualize real-time traffic congestion across NYC
- 🤖 Predict traffic congestion 15 minutes ahead using Machine Learning
- 📊 Display live analytics on a modern web dashboard
- 🗺️ Show traffic hotspots on an interactive map

### Key Highlights

| Metric | Value |
|--------|-------|
| **Data Processed** | 7+ GB |
| **Total Trips Analyzed** | 46,000,000+ |
| **Grid Cells** | 327 unique traffic zones |
| **Best ML Model** | GBT + OneVsRest — 79.24% accuracy |
| **Models Compared** | 3 (Random Forest, GBT+OneVsRest, Logistic Regression) |
| **Real-time Events** | 1000+ events/second |
| **Update Frequency** | Every 5 seconds |

---

## 2. Problem Statement & 5 V's of Big Data

### Problem Statement

Urban traffic congestion is a critical challenge in modern cities, leading to:
- Increased commute times
- Higher fuel consumption and emissions
- Economic losses due to delays
- Emergency response delays

**Our Solution**: A real-time traffic monitoring and prediction system that processes historical NYC taxi data to predict and visualize traffic congestion patterns.

### The 5 V's of Big Data

| V | Description | Implementation |
|---|-------------|----------------|
| **Volume** | Large amounts of data | **7+ GB** of NYC Taxi trip data with **46 million+ records** across 4 months (Jan 2015, Jan-Mar 2016) |
| **Velocity** | Speed of data generation | **1,000+ events per second** via Kafka streaming, **5-second** dashboard updates |
| **Variety** | Different types of data | GPS coordinates, timestamps, trip distances, passenger counts, payment info, speed calculations, grid cell mappings |
| **Veracity** | Data quality & accuracy | Comprehensive data cleaning: outlier removal, coordinate validation, missing value handling, speed calculation validation |
| **Value** | Business/practical value | **Predict congestion 15 min ahead**, identify traffic hotspots, optimize route planning, real-time traffic visualization |

---

## 3. System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SMART CITY TRAFFIC PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  RAW DATA   │───▶│   SPARK     │───▶│  PARQUET    │───▶│  ML MODEL   │  │
│  │  CSV Files  │    │   Batch     │    │  Storage    │    │  Training   │  │
│  │  (7+ GB)    │    │   ETL       │    │  (Clean)    │    │             │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                    │        │
│                                                                    ▼        │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   REACT     │◀───│   FLASK     │◀───│   KAFKA     │◀───│  STREAMING  │  │
│  │  Dashboard  │    │   REST API  │    │   Topics    │    │   Layer     │  │
│  │  (Leaflet)  │    │  (5000)     │    │             │    │             │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Data Ingestion**: Raw CSV files (NYC Taxi data) → PySpark
2. **Batch Processing**: Data cleaning, validation, grid cell assignment
3. **Feature Engineering**: Calculate speed, congestion metrics, temporal features
4. **Model Training**: Train Random Forest classifier on processed features
5. **API Layer**: Flask REST API serves real-time data
6. **Visualization**: React dashboard with Leaflet maps displays live traffic

---

## 4. Technology Stack

### Backend Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| **Python** | 3.11+ | Primary programming language |
| **Apache Spark (PySpark)** | 3.5+ | Distributed data processing |
| **Apache Kafka** | 3.6+ | Real-time event streaming |
| **Flask** | 3.0+ | REST API framework |
| **Flask-SocketIO** | 5.3+ | WebSocket real-time updates |
| **Pandas** | 2.0+ | Data manipulation |
| **NumPy** | 1.24+ | Numerical computations |
| **PyArrow** | 14.0+ | Parquet file I/O |

### Machine Learning

| Technology | Version | Purpose |
|------------|---------|---------|
| **Scikit-learn** | 1.6+ | ML model training |
| **Random Forest** | - | Classification algorithm |
| **Joblib** | 1.3+ | Model serialization |
| **StandardScaler** | - | Feature normalization |

### Frontend Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| **React** | 19.2+ | UI framework |
| **TypeScript** | 5.8+ | Type-safe JavaScript |
| **Vite** | 6.2+ | Build tool & dev server |
| **Leaflet** | 1.9+ | Interactive maps |
| **Recharts** | 3.5+ | Data visualization charts |
| **Framer Motion** | 12.23+ | Animations |
| **Lucide React** | 0.556+ | Icons |
| **TailwindCSS** | 3.0+ | Styling |

### Infrastructure

| Technology | Purpose |
|------------|---------|
| **Docker** | Containerization (Kafka, Zookeeper) |
| **Parquet** | Columnar storage format |
| **CORS** | Cross-origin resource sharing |

---

## 5. Data Pipeline

### 5.1 Data Source

**NYC Taxi & Limousine Commission (TLC) Trip Record Data**

| File | Period | Records | Size |
|------|--------|---------|------|
| `yellow_tripdata_2015-01.csv` | January 2015 | ~12M trips | ~1.8 GB |
| `yellow_tripdata_2016-01.csv` | January 2016 | ~11M trips | ~1.7 GB |
| `yellow_tripdata_2016-02.csv` | February 2016 | ~11M trips | ~1.7 GB |
| `yellow_tripdata_2016-03.csv` | March 2016 | ~12M trips | ~1.8 GB |
| **Total** | - | **~46M trips** | **~7 GB** |

### 5.2 Data Cleaning (`data_cleaning.py`)

**Operations Performed:**

1. **Remove Invalid Coordinates**
   - Filter: `40.5 ≤ latitude ≤ 40.95`
   - Filter: `-74.3 ≤ longitude ≤ -73.7`

2. **Remove Invalid Trips**
   - Distance > 0 miles
   - Duration > 0 and < 180 minutes
   - Passenger count between 1-9

3. **Calculate Derived Fields**
   - `trip_duration`: minutes between pickup and dropoff
   - `avg_speed`: distance / duration (mph)
   - `hour`, `day_of_week`: temporal features

4. **Grid Cell Assignment**
   - Divide NYC into 45 × 56 = 2,520 grid cells
   - Cell size: ~0.01° × ~0.01° (~1 km²)
   - Assign each trip to a cell based on pickup coordinates

### 5.3 Feature Engineering (`feature_engineering.py`)

**Features Generated per Cell:**

| Feature | Description |
|---------|-------------|
| `hour` | Hour of day (0-23) |
| `avg_speed` | Average speed in cell |
| `speed_std` | Speed standard deviation |
| `trip_count` | Number of trips in cell |
| `avg_distance` | Average trip distance |
| `weekend_ratio` | Proportion of weekend trips |
| `rush_hour_ratio` | Proportion during rush hours |
| `cell_lat` | Cell latitude index |
| `cell_lon` | Cell longitude index |
| `is_manhattan` | Boolean: is cell in Manhattan |

### 5.4 Output Files

```
data/processed/
├── yellow_tripdata_2015-01_clean.parquet    # Cleaned trip data
├── yellow_tripdata_2016-01_clean.parquet
├── yellow_tripdata_2016-02_clean.parquet
├── yellow_tripdata_2016-03_clean.parquet
├── training_features.parquet                 # ML features (327 cells)
└── training_features_sample.csv              # Sample for inspection
```

---

## 6. Machine Learning Model

### 6.1 Model Architecture

**Algorithm**: Random Forest Classifier

**Why Random Forest?**
- Handles non-linear relationships well
- Robust to outliers
- Feature importance ranking
- No need for feature scaling (though we use it)
- Good performance on tabular data

### 6.2 Training Details

```python
Model Configuration:
├── n_estimators: 100 (number of trees)
├── max_depth: 10
├── min_samples_split: 5
├── random_state: 42
└── class_weight: balanced
```

### 6.3 Target Variable

**Congestion Level** (3 classes):

| Class | Condition | Description |
|-------|-----------|-------------|
| **Low** | avg_speed > 20 mph | Free-flowing traffic |
| **Medium** | 10 ≤ avg_speed ≤ 20 mph | Moderate congestion |
| **High** | avg_speed < 10 mph | Heavy congestion |

### 6.4 Model Performance

```json
{
  "accuracy": 1.0,
  "precision": 1.0,
  "recall": 1.0,
  "f1_score": 1.0
}
```

### 6.5 Saved Model Files

```
models/
├── congestion_model.joblib    # Trained Random Forest model
├── scaler.joblib              # StandardScaler for features
├── feature_columns.json       # Feature names list
└── model_info.json            # Model metadata & metrics
```

---

## 7. Backend API

### 7.1 Flask REST API (`app.py`)

**Base URL**: `http://localhost:5000`

### 7.2 Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check, server status |
| `/api/stats` | GET | Overall statistics (vehicles, congestion, speed) |
| `/api/current-traffic` | GET | Current traffic for all 327 cells |
| `/api/predictions` | GET | ML predictions for future congestion |
| `/api/hotspots` | GET | Top 5 most congested zones |
| `/api/cell/<id>` | GET | Details for a specific cell |
| `/api/geojson/cells` | GET | GeoJSON format for map layers |

### 7.3 Sample API Response

**GET `/api/current-traffic?limit=3`**

```json
{
  "timestamp": "2025-12-06T15:30:00Z",
  "total_cells": 327,
  "returned_cells": 3,
  "data": [
    {
      "cell_id": "cell_29_28",
      "latitude": 40.7687,
      "longitude": -73.9738,
      "congestion_index": 0.95,
      "congestion_level": "high",
      "vehicle_count": 150,
      "avg_speed": 8.5,
      "last_update": "2025-12-06T15:29:55"
    }
  ]
}
```

### 7.4 Real-time Updates

- **WebSocket**: Socket.IO for live updates
- **Background Thread**: Updates cell data every 2 seconds
- **Simulated Streaming**: Replays historical patterns with variations

---

## 8. Frontend Dashboard

### 8.1 React Application Structure

```
bigdata/
├── index.html              # Entry HTML
├── index.tsx               # React entry point
├── App.tsx                 # Main app with routing
├── types.ts                # TypeScript interfaces
├── vite.config.ts          # Vite configuration
│
├── components/
│   ├── LandingPage.tsx     # Hero page with project info
│   ├── Dashboard.tsx       # Main analytics dashboard
│   └── Map.tsx             # Leaflet map component
│
└── services/
    ├── api.ts              # API client service
    └── mockData.ts         # Fallback data
```

### 8.2 Key Features

#### Landing Page
- Animated hero section with project title
- Statistics counters (7+ GB, 46M+ trips, etc.)
- Technology stack showcase
- "Launch Dashboard" call-to-action

#### Dashboard
- **KPI Cards**: Live metrics (vehicles, congestion, speed, ML accuracy)
- **Interactive Map**: Leaflet with heatmap visualization
- **Congestion Chart**: Real-time line chart with Recharts
- **Hotspots Panel**: Top 5 congested zones with trends
- **Connection Status**: Live/Connecting indicator

#### Map Visualization
- **Circular Heatmap**: Smooth gradient circles for each cell
- **Color Coding**:
  - 🟢 Green: Low congestion (>20 mph)
  - 🟠 Orange: Medium congestion (10-20 mph)
  - 🔴 Red: High congestion (<10 mph)
- **Glow Effects**: Outer glow for smooth transitions
- **Hotspot Markers**: Pulsing indicators for severe congestion
- **Interactive Popups**: Click cells for detailed info

### 8.3 Design System

```css
Color Palette:
├── Background: #0a0a1a (Dark Navy)
├── Surface: #1a1a2e (Card Background)
├── Accent Cyan: #00d4ff
├── Accent Purple: #8b5cf6
├── Success: #10b981 (Green)
├── Warning: #f59e0b (Amber)
├── Danger: #ef4444 (Red)
└── Text: #ffffff / #94a3b8
```

---

## 9. Project Structure

```
vscode/
├── PROJECT_DOCUMENTATION.md      # This file
│
├── bigdata/                      # React Frontend
│   ├── package.json
│   ├── index.html
│   ├── index.tsx
│   ├── App.tsx
│   ├── types.ts
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── components/
│   │   ├── LandingPage.tsx
│   │   ├── Dashboard.tsx
│   │   └── Map.tsx
│   └── services/
│       ├── api.ts
│       └── mockData.ts
│
├── smart-city-traffic/           # Backend & Data Processing
│   ├── README.md
│   ├── requirements.txt
│   ├── docker-compose.yml
│   │
│   ├── data/
│   │   ├── raw/                  # Symlinks to CSV files
│   │   └── processed/            # Cleaned Parquet files
│   │
│   ├── models/
│   │   ├── congestion_model.joblib
│   │   ├── scaler.joblib
│   │   ├── feature_columns.json
│   │   └── model_info.json
│   │
│   ├── src/
│   │   ├── api/
│   │   │   └── app.py            # Flask REST API
│   │   ├── batch/
│   │   │   ├── data_cleaning.py
│   │   │   ├── feature_engineering.py
│   │   │   └── model_training.py
│   │   └── streaming/
│   │       └── kafka_producer.py
│   │
│   ├── notebooks/
│   │   └── 01_data_exploration.ipynb
│   │
│   ├── dashboard/                # Static HTML dashboard (alt)
│   │
│   └── docs/
│       ├── architecture.md
│       ├── api_documentation.md
│       └── frontend_prompt.md
│
└── yellow_tripdata_*.csv         # Raw data files (7+ GB)
```

---

## 10. How to Run

### Prerequisites

- Python 3.11+
- Node.js 18+
- 16GB+ RAM recommended

### Step 1: Backend Setup

```bash
# Navigate to backend
cd smart-city-traffic

# Install Python dependencies
pip install -r requirements.txt

# Run data processing (if not already done)
python src/batch/data_cleaning.py
python src/batch/feature_engineering.py
python src/batch/model_training.py

# Start Flask API
python src/api/app.py
```

**API runs at**: http://localhost:5000

### Step 2: Frontend Setup

```bash
# Navigate to frontend
cd bigdata

# Install dependencies
npm install

# Start development server
npm run dev
```

**Dashboard runs at**: http://localhost:3000

### Step 3: Access Dashboard

1. Open http://localhost:3000 in browser
2. View the landing page
3. Click "Launch Dashboard" to see live traffic

---

## 11. Requirements Satisfaction

### Big Data Requirements ✅

| Requirement | Implementation | Status |
|-------------|----------------|--------|
| **Volume (Large Data)** | 7+ GB of NYC Taxi data (46M+ trips) | ✅ |
| **Velocity (Real-time)** | 5-second updates, WebSocket streaming | ✅ |
| **Variety (Multiple Sources)** | GPS, timestamps, trip metrics, grid cells | ✅ |
| **Veracity (Data Quality)** | Comprehensive cleaning pipeline | ✅ |
| **Value (Business Insight)** | Congestion prediction, hotspot detection | ✅ |

### Technical Requirements ✅

| Requirement | Implementation | Status |
|-------------|----------------|--------|
| **Distributed Processing** | Apache Spark (PySpark 4.0.1) for batch ETL | ✅ |
| **RDD API** | `traffic_rdd_analysis.py` — 6 analyses on 45.9M real trips | ✅ |
| **DataFrame API** | Data cleaning, feature engineering, model training | ✅ |
| **Dataset API (Scala)** | `TrafficDataPreprocessor.scala` (406 lines) | ✅ |
| **Streaming Pipeline** | Kafka + Spark Structured Streaming + E2E test | ✅ |
| **Machine Learning** | 3-model comparison (RF/GBT+OneVsRest/LR), best=79.24% | ✅ |
| **REST API** | Flask with multiple endpoints + Prometheus /metrics | ✅ |
| **Real-time Dashboard** | React + Leaflet + WebSocket | ✅ |
| **Data Visualization** | Interactive maps, charts, KPIs | ✅ |
| **Monitoring** | Prometheus + Grafana in Docker | ✅ |

### Project Deliverables ✅

| Deliverable | Location | Status |
|-------------|----------|--------|
| **Data Cleaning Scripts** | `src/batch/data_cleaning_spark.py` | ✅ |
| **Feature Engineering** | `src/batch/feature_engineering_spark.py` | ✅ |
| **ML Model Training** | `src/batch/model_training_spark.py` (RF/GBT/LR) | ✅ |
| **RDD Analysis** | `src/batch/traffic_rdd_analysis.py` (573 lines) | ✅ |
| **Trained Model** | `models/spark_congestion_model/` (GBT+OneVsRest) | ✅ |
| **Pipeline Orchestrator** | `run_pipeline_local.py` | ✅ |
| **REST API** | `src/api/app.py` | ✅ |
| **Streaming E2E Test** | `src/streaming/streaming_e2e_test.py` (802 lines) | ✅ |
| **Frontend Dashboard** | `frontend/` folder | ✅ |
| **Documentation** | This file + `docs/` folder | ✅ |

---

## 12. Screenshots & Features

### Landing Page Features
- ✨ Animated gradient background
- 📊 Live statistics counters
- 🛠️ Technology stack showcase
- 🚀 Launch Dashboard button

### Dashboard Features
- 📈 **4 KPI Cards**: Active Vehicles, Avg Congestion, Avg Speed, ML Accuracy
- 🗺️ **Interactive Map**: 327 traffic cells with heatmap
- 📉 **Live Charts**: Real-time congestion trends
- 🔥 **Hotspots Panel**: Top 5 congested zones
- 🟢 **Live Indicator**: Connection status
- 🕐 **Auto-refresh**: Updates every 5 seconds

### Map Features
- 🔴 Red zones: High congestion (<10 mph)
- 🟠 Orange zones: Medium congestion (10-20 mph)
- 🟢 Green zones: Low congestion (>20 mph)
- 💫 Glowing markers for severe hotspots
- 📍 Click for detailed cell info

---

## 13. Future Enhancements

### Already Implemented (beyond original scope)

1. ✅ **Multi-Model Comparison** — RF vs GBT+OneVsRest vs LR (was single-model)
2. ✅ **RDD API Analysis** — 573-line script on 45.9M real records
3. ✅ **Prometheus + Grafana Monitoring** — Full observability stack
4. ✅ **Pipeline Orchestrator** — `run_pipeline_local.py` master script
5. ✅ **E2E Streaming Test** — 802-line integration test

### Potential Future Improvements

1. **Deep Learning Models**
   - LSTM for time-series congestion prediction
   - GNN for road network modeling

2. **Infrastructure Scaling**
   - Cloud deployment (AWS EMR / Azure HDInsight)
   - Kubernetes orchestration
   - CI/CD pipeline

3. **Additional Features**
   - Route optimization suggestions
   - Historical playback mode
   - Mobile responsive design

---

## 📝 Credits

**Project**: Smart City Real-Time Traffic Simulation & Predictive Analytics  
**Type**: Final Year Big Data Project  
**Date**: December 2025  
**Data Source**: NYC Taxi & Limousine Commission (TLC)

---

## 📄 License

MIT License - Feel free to use and modify for educational purposes.

---

*This documentation was updated for the Smart City Traffic Intelligence project — February 27, 2026.*
