# ⚡Real-Time IMDB Data Pipeline & Sentiment Analysis

## 📌 Introduction

This project builds a **real-time IMDB data processing pipeline**, integrating multiple Big Data technologies:

- **Apache NiFi** – data ingestion from web, MongoDB, JSON  
- **Apache Kafka** – streaming data across 3 topics: `movie`, `actor`, `review`  
- **HDFS** – distributed raw file storage  
- **Apache Spark Structured Streaming** – parallel processing of 3 data streams  
- **PostgreSQL** – storing real-time analytical results  
- **NLP Model (TF-IDF + Classifier)** – user sentiment prediction  
- **Apache Airflow** – orchestration workflow  
- **Prometheus + Grafana** – real-time monitoring  

---
## 🏗 Pipeline Architecture
```
NiFi → Kafka (movie, actor, review topics)
         ↓
       HDFS (/IMDB/movie, /IMDB/actor, /IMDB/review)
         ↓
 Spark Structured Streaming (parallel processing)
         ↓
   Analytics & NLP Sentiment Model
         ↓
      PostgreSQL (Realtime)
         ↓
  Grafana Dashboards & BI
```
## 🧠 System Objectives

- Collect IMDB movie, actor, and review data using NiFi → Kafka

- Store streaming raw data into HDFS inside /IMDB/movie, /IMDB/actor, /IMDB/review

- Process 3 streams in parallel with Spark Structured Streaming

- Perform 5 real-time analytical tasks:

        1. Top ratings, revenue, and profit of films by country

        2. User sentiment analysis & classification model

        3. Top directors by rating & revenue

        4. Rating trend analysis by year

        5. Predict user sentiment using NLP model

- Load processed data into PostgreSQL (real-time update)

- Monitor all components using Prometheus & Grafana

- Run & control workflows using Apache Airflow

---

##  📁 Directory Structure

```
project/
│
├── check_points/
│   ├── top_country
│   ├── top_director_rate
│   ├── top_sentiment
│   ├── rating_per_year
│   ├── top_user_sentiment   # Spark streaming checkpoints
│
├── data/
│   ├── movies.json
│   ├── actors.json
│   └── reviews.json
│
├── dags/
│   ├── airflow_kafka.py        # NiFi → Kafka ingestion DAG
│   └── airflow_spark_psql.py   # Spark → PostgreSQL DAG
│
├── kafka/
│   ├── crawl_data.py
│   ├── push_kafka.py
│   └── crawl_data_test.ipynb
│
├── models/
│   └── tf_idf_model/
│       ├── metadata/
│       └── stages/
│
├── monitoring/
│   ├── config/
│   ├── grafana/
│   └── prometheus.yml
│
├── nifi/
│   └── IMDB_nifi.xml          # NiFi flow template
│
├── spark/
│   ├── analysis.py
│   ├── clean_data.py
│   ├── configuration.py
│   ├── load_data.py
│   ├── main.py
│   ├── orchestration.py
│   └── spark_test.ipynb
│
├── web/
│   ├── static/
│   └── templates/
│       ├── index.html
│       ├── charts.html
│       └── about.html
│   └── web.py
│
├── postgresql-42.7.3.jar
├── .gitignore
└── README.md
```

---

## ⚙️ Data Ingestion Pipeline (NiFi → Kafka → HDFS)

NiFi Flow Description:

- Extract JSON data from API, MongoDB, or static files  
- Route into Kafka with 3 independent streams  
- Apply validation + transformation processors  
- Push into topics:
  - `movie_topic`
  - `actor_topic`
  - `review_topic`

---

## 🚀 Spark Structured Streaming

- Reads 3 HDFS directories (`movie/`, `actor/`, `review/`)  
- Cleans, normalizes and standardizes schema  
- Joins multi-source datasets  
- Runs analytics:
  - yearly performance  
  - rating distribution  
  - country-level insights  
  - director performance  
  - sentiment classification  

---

## 🗄PostgreSQL Output Tables

| Table Name        | Description                          |
|------------------|--------------------------------------|
| top_country     | Top ratings, revenue & profit by country                    |
| top_director_rate     | Best directors by rating & revenue                    |
| top_sentiment    | Average user sentiment per movie               |
| rating_per_year| predicted sentiment labels            |
| top_user_sentiment      |Predicted sentiment & counts                  |


---

## 🗄 PostgreSQL Real-Time Output Tables (SQL Schema)

```
CREATE TABLE top_country (
    country VARCHAR(100),
    language VARCHAR(100),
    movie_ts TIMESTAMP,
    avg_rating DOUBLE PRECISION,
    total_revenue BIGINT,
    total_movie INTEGER,
    total_budget BIGINT,
    avg_profit DOUBLE PRECISION
);

CREATE TABLE top_director_rate (
    director VARCHAR(255),
    avg_rating DOUBLE PRECISION,
    total_revenue BIGINT,
    movie_count INTEGER,
    analysis_ts TIMESTAMP
);

CREATE TABLE top_sentiment (
    title VARCHAR(255),
    avg_sentiment DOUBLE PRECISION,
    total_review INTEGER,
    sentiment_ts TIMESTAMP
);

CREATE TABLE rating_per_year (
    year INTEGER,
    avg_rating DOUBLE PRECISION,
    total_movie INTEGER,
    rating_ts TIMESTAMP
);

CREATE TABLE top_user_sentiment (
    title VARCHAR(255),
    rating DOUBLE PRECISION,
    review_ts TIMESTAMP,
    total_review INTEGER,
    positive_review INTEGER,
    negative_review INTEGER,
    neutral_review INTEGER
);


```
## 🤖 Sentiment Analysis Model (TF-IDF + Linear Classifier)

This system integrates a real-time **Sentiment Analysis Model** built from IMDB review data and deployed in the streaming pipeline.

### Model Overview
- Preprocessed dataset of **45,000 IMDB user reviews**
- Text processing pipeline:
  Tokenization → Stopword Removal → Lemmatization → TF-IDF Vectorization
- Trained **Linear Classifier**, achieving **~92% accuracy**
- Integrated with **Spark Structured Streaming** to classify live review text
- Output sentiment is appended to streaming review results and stored in PostgreSQL

### Supported Sentiment Categories

| Label | Meaning |
|--------|---------|
| **positive** | Positive feedback |
| **negative** | Negative feedback |
| **neutral** | Mixed / neutral sentiment |

---

## 🌐 Web Demo — Real-Time Sentiment & Data Mining

This project includes a **web dashboard** to visualize and explore extracted insights and real-time predictions.

### Key Features

✔ Real-time sentiment prediction from streaming review text  
✔ Visualization of rating trends & sentiment distribution  
✔ Interactive data mining features:

- Filter movies by **rating**, **country**, **director**, **sentiment**
- Compare movies by **revenue vs rating correlation**
- Search and analyze **actor & director performance insight**

---

### 🔧 Run the Web Demo

    python web/web.py

Access via browser:

    http://localhost:5000

---
"""

## 📊 Monitoring

- **Prometheus** collects Spark & Kafka metrics  
- **Grafana** dashboards show:
  - Kafka consumer lag  
  - Spark batch latency  
  - PostgreSQL throughput  
  - Airflow DAG performance  

---

## ⚙️ Airflow Orchestration

DAGs included:

1. `airflow_kafka.py`  
2. `airflow_spark_psql.py`  

---

## Installation

```
git clone https://github.com/yourname/imdb-realtime-pipeline.git
cd imdb-realtime-pipeline
```

---

## 🚀 How to Run

### 1. Create Kafka Topics
```
kafka-topics.sh --create --topic movie_topic ...
```

### 2. Run Kafka 
```
kafka-start-server.sh config/server.properties
```

### 3. Run NiFi  
Import file: `IMDB_nifi.xml`

### 4. Start Spark Streaming  
```
spark-submit --packages org.postgresql:postgresql-42.7.3.jar spark/main.py
```

### 5. Start Airflow  
```
airflow scheduler &
airflow api-server
```

---

## License
MIT License — Free for academic & research purposes.

## 🎤 Author

**Hoàng Minh Hải - minhhaiit1k68@gmail.com**  
📅 Project: Real-Time IMDB Data Pipeline & Sentiment Analysis
