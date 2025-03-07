# Real-Time Fraud Detection Pipeline

## Overview
This repository contains a complete Kafka and Spark-based streaming pipeline for real-time fraud detection. The pipeline simulates realistic user behavior and transaction events and analyzes them to detect fraudulent activities immediately.

## Project Components

### 1. Fraud Data Spark Streaming
**Purpose:**
Processes streaming data using Apache Spark Structured Streaming to identify fraudulent activities in real-time.

**Key Features:**
- Structured data ingestion with explicit schema definitions.
- Real-time data processing integrated with Kafka.
- Robust configuration with memory optimization and fault tolerance.

### 2. Fraud Detection Producer
**Purpose:**
Simulates realistic data streams representing user browsing behavior and transactions, publishing these streams into Kafka topics.

**Key Features:**
- Kafka producer for continuous, scalable data streaming.
- Random batch sizes to imitate real-world user interaction variability.
- Timestamped data events for realistic simulation.

## Integration Workflow
1. **Data Simulation:** The producer notebook simulates real-world data and publishes it to Kafka.
2. **Streaming Analysis:** Spark Streaming consumes and processes these Kafka data streams to detect potential fraud in real-time.

## Applications
- **Real-Time Fraud Detection**
- **Behavioral and Transactional Analytics**
- **System Performance Testing and Validation**

## Getting Started
### Prerequisites
- Kafka
- Apache Spark (PySpark)
- Python 3

### Usage
1. Start Kafka and Spark services.
2. Run `Fraud_Detection_Producer.ipynb` to initiate data streaming.
3. Run `Fraud_Data_Spark_Streaming.ipynb` to process data in real-time.

## Contributing
Contributions are welcome. Please submit pull requests for new features or improvements.

## License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.

