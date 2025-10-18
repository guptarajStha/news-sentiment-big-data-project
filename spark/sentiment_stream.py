from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StringType, StructType, StructField
from textblob import TextBlob

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimeNewsSentiment") \
    .getOrCreate()
    # .config("spark.mongodb.output.uri", "mongodb://mongo:27017/newsdb.sentiments") \
    # .master("local[*]") \

schema = StructType([
    StructField("title", StringType()),
    StructField("link", StringType()),
    StructField("source", StringType())
])

# Sentiment function using TextBlob
def get_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0:
        return "Positive"
    elif polarity < 0:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(get_sentiment, StringType())

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "news_topic") \
    .load()
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

sentiment_df = json_df.withColumn("sentiment", sentiment_udf(col("title")))
sentiment_df = sentiment_df.dropDuplicates(["title"])


# Write stream to MongoDB
# query = sentiment_df.writeStream \
#     .outputMode("append") \
#     .format("mongodb") \
#     .option("uri", "mongodb://localhost:27017/newsdb.sentiments") \
#     .start()
    
# query = sentiment_df.writeStream \
#     .outputMode("append") \
#     .format("mongodb") \
#     .option("uri", "mongodb://mongo:27017/") \
#     .option("database", "newsdb") \
#     .option("collection", "sentiments") \
#     .option("checkpointLocation", "/tmp/spark-checkpoint") \
#     .start()

query = sentiment_df.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://mongo:27017/newsdb.sentiments") \
    .option("spark.mongodb.output.uri", "mongodb://mongo:27017/newsdb.sentiments") \
    .option("database", "newsdb") \
    .option("collection", "sentiments") \
    .option("checkpointLocation", "/tmp/spark-checkpoint")\
    .start()
query.awaitTermination()
