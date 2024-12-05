# BigData-Processing-Pipeline

The aim of the present project is to develop a comprehensive pipeline for handling, processing, and storing large volumes of real-time data using modern big data technologies. This pipeline includes data generation via simulation, real-time processing with Apache Spark, and efficient storage in MongoDB. The project was developed as part of the course "Big Data Management Systems" at the University of Patras, organized to mimic industry practices.

## Authors

- [@CallMeJasonYT](https://github.com/CallMeJasonYT)
- [@Roumpini21](https://github.com/Roumpini21)

## Features
- Data Simulation: Uses the UXSIM traffic simulator to generate realistic vehicle movement data in JSON format.
- Real-Time Data Ingestion: Sends vehicle position data periodically to a Kafka broker, ensuring timely updates.
- Data Processing: Implements Apache Spark for processing streaming data, producing valuable insights such as vehicle counts and average speeds for specific road segments.
- NoSQL Data Storage: Stores raw and processed data in MongoDB collections for easy querying and further analysis.
- Analytics Queries: Supports querying MongoDB to answer complex questions, such as identifying the busiest or fastest road segments during specific time intervals.
- Industry-Grade Tools: Utilizes open-source technologies like Kafka, Spark, and MongoDB for scalable and efficient big data processing.

## Tech Stack

Core Technologies: Python, Apache Kafka, Apache Spark, MongoDB

## Installation
Install required tools:
- Python 3.8+
- Kafka, Spark, MongoDB
- kafka-python and pyspark Python libraries

Clone this repository and navigate to the project folder:
```
git clone https://github.com/YourGitHubUsername/BigData-Processing-Pipeline.git
cd BigData-Processing-Pipeline
```

## Usage
- Run the UXSIM simulator to generate traffic data, by running simulation.py.
- Use the provided Python scripts (producer.py and consumer.py) to send data to Kafka.
- Process the data using Spark streaming, by running spark.py and save results in MongoDB.
- Query the data using MongoDB to extract meaningful insights, by running mongoQueries.py.
- 
