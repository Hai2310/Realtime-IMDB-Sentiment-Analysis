import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud, STOPWORDS
from pyspark.ml.fpm import FPGrowth
import time
import traceback
from functools import partial

from configuration import SparkConfig
from load_data import DataLoader
from clean_data import DataClean
from analysis import AnalysisSentiment

class ImdbPipeline :
    def __init__(self):
        self.spark = SparkConfig.create_sparksession()
        self.load = DataLoader()
        self.clean = DataClean
        self.analytics = AnalysisSentiment
        self.result = {}

    def load_data(self) :
        print("\n" + "="*60)
        print("=== DATA LOADING PHASE ===")
        print("="*60)

        self.movie = self.clean.movie_clean(self.load.movie_load())
        self.actor = self.clean.actor_clean(self.load.actor_load())
        self.review = self.clean.review_clean(self.load.review_load())

        print("\n All data loaded successfully")

    def run_analysis_1(self) :
        try:
            result = self.analytics.top_country(self.movie)
            self.result["top_country"] = result
            return result
        except Exception as e :
            print("Error in Analysis 1 : " , str(e))
            raise

    def run_analysis_2(self) :
        try:
            result = self.analytics.rating_per_year(self.review , self.movie)
            self.result["rating_per_year"] = result
            return result
        except Exception as e :
            print("Error in Analysis 2 : " , str(e))
            raise

    def run_analysis_3(self) :
        try:
            result = self.analytics.top_user_sentiment(self.review , self.movie)
            self.result["top_user_sentiment"] = result
            return result
        except Exception as e :
            print("Error in Analysis 3 : " , str(e))
            raise

    def run_analysis_4(self) :
        try:
            result = self.analytics.top_director_rate(self.actor , self.movie)
            self.result["top_director_rate"] = result
            return result
        except Exception as e :
            print("Error in Analysis 4 : " , str(e))
            raise

    def run_analysis_5(self) :
        try:
            result = self.analytics.top_sentiment(self.review , self.movie)
            self.result["top_sentiment"] = result
            return result
        except Exception as e :
            print("Error in Analysis 5 : " , str(e))
            raise
        
    def run_all_analysis(self) :
        print("\n" + "="*60)
        print("=== ANALYSIS PHASE - RUNNING ALL ANALYSES ===")
        print("="*60)

        total_start = time.time()

        self.run_analysis_1()
        self.run_analysis_3()
        self.run_analysis_4()
        self.run_analysis_2()
        self.run_analysis_5()

        total_elapsed = time.time() - total_start

        print("\n" + "="*60)
        print(f"ALL ANALYSIS COMPLETED IN {total_elapsed:.2f}s")
        print("="*60)
    
    def save_sql(self) :
        print("\n" + "="*60)
        print("=== SAVING TO POSTGRESQL ===")
        print("="*60)

        def save_psql(df , batch_id , name):
            print("Batch id " , str(batch_id) , " :")
            print(f"=== Save {name} into postgresql ===")
            print("Count in batch =", df.count()) 
            df.show(5, truncate=False)
            
            try :
                df.write.format("jdbc").mode("append") \
                            .option("driver" ,"org.postgresql.Driver") \
                            .option("url" , "jdbc:postgresql://192.168.65.189:5432/imdb_sentiment") \
                            .option("user" , "postgres") \
                            .option("password" ,"minhhai123") \
                            .option("dbtable" , f"public.{name}") \
                            .save()
            except Exception as e :
                print(f"Error to save {name} : " , str(e))
            

        queries = []
        for name , df in self.result.items() :
            query =  df.writeStream.outputMode("append") \
                            .foreachBatch(partial(save_psql , name = name)) \
                            .option("checkpointLocation" , f"/home/enovo/prj/test/check_points/{name}/") \
                            .trigger(processingTime = "5 seconds") \
                            .start() 
            queries.append(query)
            print(f"Streming {name} started -> to postgreSQL")
        # self.spark.streams.awaitAnyTermination()
        for q in queries :
            q.awaitTermination()
    def spark_stop(self) :
        self.spark.stop()
        print("=== Spark session stop ===")

            
