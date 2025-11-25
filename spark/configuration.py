import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.ml.fpm import FPGrowth
import time
import traceback



class SparkConfig :
    def create_sparksession() :
        spark = SparkSession.builder.appName("IMDB movie") \
                                    .config("spark.driver.memory" , "8g") \
                                    .config("spark.executor.memory" , "4g") \
                                    .config('spark.streaming.stopGracefullyOnShutdown' , True) \
                                    .config("spark.sql.shuffle.partitions" , "20") \
                                    .config("spark.jars.packages" , "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1," \
                                                                    "com.johnsnowlabs.nlp:spark-nlp_2.12:5.4.0") \
                                    .config("spark.jars" , "/home/enovo/prj/test/postgresql-42.7.3.jar") \
                                    .getOrCreate()
        return spark 
    
class IMDB_Schema :
    def movie_schema() :
        movie_schema = ArrayType(StructType([
            StructField('movie_id' , StringType() , True) , 
            StructField("title" , StringType() , True) ,
            StructField("rating" , FloatType() , True) , 
            StructField("year" , IntegerType() , True) ,
            StructField("vote_count" , StringType() , True) ,
            StructField("runtime" , StringType() , True) ,
            StructField("items" , StringType() , True ) ,
            StructField("country" , StringType() , True) ,
            StructField("language" , StringType() , True) ,
            StructField("company" , StringType() , True) ,
            StructField("budget" , StringType() , True) , 
            StructField("revenue" , StringType() , True) ,
            StructField("plot" , StringType() , True) ,
            StructField("poster" , StringType() , True) ,
            StructField("url" , StringType() , True)
        ]))
        return movie_schema
    
    def actor_schema()  :
        actor_schema = ArrayType(StructType([
            StructField("actor_id" , StringType() , True) ,
            StructField("director" , StringType() , True) ,
            StructField("writers" , StringType() , True) ,
            StructField("stars" , StringType() , True) ,
            StructField("movie_id" , StringType() , True)
        ]))
        return actor_schema 
    
    def review_schema() : 
        review_schema = ArrayType(StructType([
            StructField("review_id" , StringType() , True) , 
            StructField("title_review" , StringType() , True) ,
            StructField("comment" , StringType() , True) ,
            StructField("star" , StringType() , True) ,
            StructField("like" , StringType() , True) ,
            StructField("dislike" , StringType() , True) ,
            StructField("date" , StringType() , True) ,
            StructField("user_name" , StringType() , True) , 
            StructField("movie_id" , StringType() , True)
        ]))
        return review_schema