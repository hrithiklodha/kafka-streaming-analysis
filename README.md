# Kafka Streaming Projects

## kafka-stream.py

### What it does

`kafka-stream.py` - produces streaming row data from csv onto a kafka topic port

### How to run it

1. Ensure you have Kafka and PySpark installed.
2. Start your Kafka server.
3. Run the script using the following command:
   ```sh
   python kafka-stream.py
   ```

# stream-analysis.py - PySpark Streaming with Kafka

## What It Does

`stream-analysis.py` processes streaming data from the kafka topic. The application:

- Reads data from a specified Kafka topic.
- Applies various transformations and aggregations (depending on your specific code logic).
- Outputs the results to the console for debugging or visualization.

This application uses **Structured Streaming** in PySpark to perform real-time stream processing.

## How to Run It

### Prerequisites

1. **Kafka**: Ensure that Kafka is installed and running on your machine. You can download Kafka from [here](https://kafka.apache.org/downloads).
2. **PySpark**: Ensure that PySpark is installed. You can install it using `pip`:
   ```bash
   pip install pyspark
   ```
