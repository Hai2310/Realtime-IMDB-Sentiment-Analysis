import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import time
import traceback



class DataClean :
    def movie_clean(movie) :
        # Lọc các record rác
        print("\n" + "="*60)
        print("=== Starting clean movie ===")
        print("="*60)  

        movie = movie.filter(~col("movie_id").startswith("{"))

        # Chuyển đổi kiểu dữ liệu , làm sạch cột revenue
        movie_revenue_clean = movie.withColumn("revenue" , regexp_replace(col("revenue") , "[\$,]" , "")) \
                                    .withColumn("revenue" , trim(col("revenue")) ) \
                                    .withColumn("revenue" , when(col("revenue") == "" , 0).otherwise(col("revenue"))) \
                                    .withColumn("revenue" , col("revenue").cast('int')) 
        
        # Chuyển đổi kiểu dữ liệu , làm sạch cột budget
        movie_budget_clean = movie_revenue_clean.withColumn("budget" , when(col("budget") == "" , "0").otherwise(col("budget"))) \
                                                    .withColumn("budget" , split(col("budget") , " ").getItem(0)) \
                                                    .withColumn("budget" , regexp_replace(col("budget") , "[\$,]" , "")) \
                                                    .withColumn("budget" , trim(col("budget")) ) \
                                                    .withColumn("budget" , col("budget").cast('int'))
        
        # Chuyển đổi kiểu dữ liệu , làm sạch cột vote_count
        movie_clean =  movie_budget_clean.withColumn("vote_count" ,
                            when(col("vote_count").endswith("M"),  regexp_replace(col("vote_count") , "M" , "").cast("double") * 1_000_000 ) \
                            .when(col("vote_count").endswith("K"), regexp_replace(col("vote_count") , "K" , "").cast("double") * 1_000  ) \
                            .otherwise(0) \
                            .cast("int"))
        print("=== Cleaned movie successfully ===")
        return movie_clean
    
    def actor_clean(actor) :
        # Lọc các record rác
        print("\n" + "="*60)
        print("=== Starting clean actor ===")
        print("="*60)  
        actor = actor.filter(~col("actor_id").startswith("{"))

        # Chuyển đổi kiểu dữ liệu , làm sạch cột writers và stars
        actor_clean = actor.withColumn("writers" , regexp_replace(col("writers") , '[\[\]"]' , "") ) \
                        .withColumn("stars" , regexp_replace(col("stars") , '[\[\]"]' , "") )
        print("=== Cleaned actor successfully ===")
        return actor_clean
    
    def review_clean(review) : 
        print("\n" + "="*60)
        print("=== Starting clean review ===")
        print("="*60)  

        # Chuyển đổi kiểu dữ liệu , làm sạch cột star
        review_star_clean = review.withColumn("star" , when(col("star") == "" , "0") \
                                                .otherwise(col("star")) \
                                                .cast("int") )
        
        # Chuyển đổi kiểu dữ liệu , làm sạch cột like
        review_like_clean = review_star_clean.withColumn("like" , 
                                            when(col("like").endswith("K") ,
                                                trim(regexp_replace(col("like") , "K" , "")).cast("double") * 1_000 ) \
                                            .when(col("like") == "" , "0") \
                                            .otherwise(trim(col("like"))) \
                                            .cast("int"))
        
        # Chuyển đổi kiểu dữ liệu , làm sạch cột dislike
        review_dislike_clean = review_like_clean.withColumn("dislike" , 
                                            when(col("dislike").endswith("K") , 
                                                trim(regexp_replace(col("dislike") , "K" , "")).cast("double") * 1_000 ) \
                                            .when(col("dislike") == "" , "0") \
                                            .otherwise(trim(col("dislike"))) \
                                            .cast("int"))
        
        # Xóa cột không cần thiết
        review_clean = review_dislike_clean.drop("date")
        print("=== Cleaned review successfully ===")

        return review_clean

    