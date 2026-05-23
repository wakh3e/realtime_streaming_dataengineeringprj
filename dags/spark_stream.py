import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- 1. SPARK INITIALIZATION ---
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("KafkaCassandraStreaming") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                          "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print("✓ Connected to Spark Cluster successfully!")
        return spark
    except Exception as e:
        logging.error(f"✗ Failed to create Spark session: {e}")
        return None

# --- 2. STREAM CONSUMPTION ---
def connect_to_kafka(spark_conn):
    try:
        # We read from the internal docker listener port 2902
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:2902") \
            .option("subscribe", "user_events") \
            .option("startingOffsets", "earliest") \
            .load()
        print("✓ Successfully connected to Kafka stream!")
        return spark_df
    except Exception as e:
        logging.error(f"✗ Kafka dataframe could not be created: {e}")
        return None

# --- 3. TRANSFORMATION LAYER ---
def create_selection_df_from_kafka(spark_df):
    # Define a clear schema matching your inbound payload json structure
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])
    
    # Extract binary value to payload string, then parse out elements
    parsed_df = spark_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")
        
    return parsed_df

# --- 4. ORCHESTRATION PIPELINE ---
if __name__ == "__main__":
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        # Ingest
        kafka_raw_df = connect_to_kafka(spark_conn)
        
        if kafka_raw_df is not None:
            # Transform
            transformed_df = create_selection_df_from_kafka(kafka_raw_df)
            
            # Sink (Streaming directly into Cassandra)
            print("⚡ Starting real-time data streaming to Cassandra storage...")
            streaming_query = transformed_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/spark_kafka_checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "users") \
                .start()
                
            streaming_query.awaitTermination()