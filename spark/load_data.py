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
        selt.host = "hdfs://localhost:9000"
        
    def movie_load(selt) :
        print("\n" + "="*60)
        print("=== Starting load movie ===")
        print("="*60)

        movie = selt.spark.readStream.format("json" ) \
                                    .schema(IMDB_Schema.movie_schema()) \
                                    .option("maxFilePerTrigger" , 1) \
                                    .load(f"{selt.host}/IMDB/movie")
        
        movie_generate = movie.withColumn("movie_ts" , current_timestamp()) \
                                        .withWatermark("movie_ts" , "10 minutes")
        
        
        print("=== Loaded movie successfully ===")
        return movie_generate
    
    def actor_load(selt) :
        print("\n" + "="*60)
        print("=== Starting load actor ===")
        print("="*60)
        actor = selt.spark.readStream.format("json") \
                                    .schema(IMDB_Schema.actor_schema()) \
                                    .option("maxFilePerTrigger" , 1) \
                                    .load(f"{selt.host}/IMDB/actor")
        
        actor_generate = actor.withColumn("actor_ts" , current_timestamp()) \
                                        .withWatermark("actor_ts" , "10 minutes")
        
        
        
        print("=== Loaded actor successfully ===")
        return actor_generate    
    
    def review_load(selt) :
        print("\n" + "="*60)
        print("=== Starting review actor ===")
        print("="*60)

        review = selt.spark.readStream.format("json") \
                                    .schema(IMDB_Schema.review_schema()) \
                                    .option("maxFilePerTrigger" , 1) \
                                    .load(f"{selt.host}/IMDB/review")
        
        review_generate = review.withColumn("review_ts" , current_timestamp()) \
                                        .withWatermark("review_ts" , "10 minutes")
        
        
        print("=== Loaded review successfully ===")
        return review_generate