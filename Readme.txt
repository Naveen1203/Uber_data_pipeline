Uber Data Pipeline:
-------------------
Uber.csv --> Dataset
Architecture.PNG --> consists of Architecture Diagram of Project.
Data_model.PNG --> consists of the structure of tables.
CSV_to_Kafka.py --> Spark script to load the data into kafka topics from csv file.
Kafka_to_SSMS.py --> Spark script to read data from topics, transforming the data and loading it into SQL Sever Management Studio.
