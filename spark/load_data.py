import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import time
import traceback

from configuration import SparkConfig , IMDB_Schema


class DataLoader :
    def __init__(selt) :
        selt.spark = SparkConfig.create_sparksession()
        
    def movie_load(selt) :
        print("\n" + "="*60)
        print("=== Starting load movie ===")
        print("="*60)

        movie = selt.spark.readStream.format("kafka" ).option("kafka.bootstrap.servers" , "localhost:9092" ) \
                                                .option("subscribe" , "movie") \
                                                .option("startingOffsets" , "earliest") \
                                                .load()
        
        movie_generate = movie.withColumn("movie_ts" , col("timestamp")) \
                                        .withWatermark("movie_ts" , "10 minutes")
        
        movie_generate = movie_generate.withColumn("value" , explode(
                                                    from_json(col("value").cast("string") , IMDB_Schema.movie_schema()))) \
                                                    .select("value.*" , "movie_ts")
        
        
        print("=== Loaded movie successfully ===")
        return movie_generate
    
    def actor_load(selt) :
        print("\n" + "="*60)
        print("=== Starting load actor ===")
        print("="*60)
        actor = selt.spark.readStream.format("kafka" ).option("kafka.bootstrap.servers" , "localhost:9092" ) \
                                                .option("subscribe" , "actor") \
                                                .option("startingOffsets" , "earliest") \
                                                .load()
        
        actor_generate = actor.withColumn("actor_ts" , col("timestamp")) \
                                        .withWatermark("actor_ts" , "10 minutes")
        
        actor_generate = actor_generate.withColumn("value" , explode(
                                                    from_json(col("value").cast("string") , IMDB_Schema.actor_schema()))) \
                                                    .select("value.*" , "actor_ts")
        
        
        print("=== Loaded actor successfully ===")
        return actor_generate    
    
    def review_load(selt) :
        print("\n" + "="*60)
        print("=== Starting review actor ===")
        print("="*60)

        review = selt.spark.readStream.format("kafka" ).option("kafka.bootstrap.servers" , "localhost:9092" ) \
                                                .option("subscribe" , "review") \
                                                .option("startingOffsets" , "earliest") \
                                                .load()
        
        review_generate = review.withColumn("review_ts" , col("timestamp")) \
                                        .withWatermark("review_ts" , "10 minutes")
        
        review_generate = review_generate.withColumn("value" , explode(
                                                    from_json(col("value").cast("string") , IMDB_Schema.review_schema()))) \
                                                    .select("value.*" , "review_ts")
        
        
        print("=== Loaded review successfully ===")
        return review_generate