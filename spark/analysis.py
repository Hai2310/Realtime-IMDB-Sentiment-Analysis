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




class AnalysisSentiment :    
    # ===================================================================
    # 1 : Top đánh giá ,doanh thu ,lợi nhuận của các phim trong từng nước 
    # ===================================================================

    def top_country(movie) :
        # Thêm cột lợi nhuận
        print("Analysis 1 : Top ratings, revenue, and profits of films in each country")
        movie_profit = movie.withColumn("profit" , when(col("revenue") > 0 , col("revenue") - col("budget")) \
                                            .otherwise(0))
        # Lấy ra top theo country và language
        top_country = movie_profit.groupBy("country" , "language","movie_ts").agg(avg("rating").alias("avg_rating") ,
                                                                        sum("revenue").alias("total_revenue") ,
                                                                        count("movie_id").alias("total_movie") ,
                                                                        sum("budget").alias("total_budget") ,
                                                                        avg("profit").alias("avg_profit"))
        return top_country

    # ===================================================================
    # 2 : Phân tích cảm xúc người dùng
    # ===================================================================
    def top_user_sentiment(review , movie) :
        print("Analysis 2 : Analytics user Sentiment")

        user_sentiment = review.join(broadcast(movie) , "movie_id" , "inner") 

        top_user_sentiment = user_sentiment.groupBy("title" , "rating" , "review_ts").agg(count("review_id").alias("total_review") ,
                                                                        sum("like").alias("total_like") ,
                                                                        sum("dislike").alias("total_dislike") ,
                                                                        when(sum("like")+sum("dislike") > 0 ,
                                                                            (sum("like")/(sum("like")+sum("dislike"))))
                                                                            .otherwise(0.0).alias("like_ratio")
                                                                        )

        return top_user_sentiment
    # ===================================================================
    # 3 : Top các đạo diễn có lượng rating , revenue cao nhất
    # ===================================================================

    def top_director_rate(actor , movie) :
        print("Analysis 3 : Top directors with the highest ratings and revenue.")

        director_rate = actor.join(broadcast(movie) , "movie_id" , "inner") 

        top_director_rate = director_rate.groupBy("director" , "actor_ts").agg(avg("rating").alias("avg_rating") ,
                                                        count("movie_id").alias("total_movies") ,
                                                        sum("revenue").alias("total_revenue"))
        
        return top_director_rate

    # ===================================================================
    # 4 : Xu hướng đánh giá theo năm 
    # ===================================================================
    def rating_per_year(review , movie) :
        print("Analysis 4 : Annual evaluation trends")

        review_join = review.join(broadcast(movie) , "movie_id" , "inner")

        review_dt = review_join.dropDuplicates(["review_id" , "movie_id"])

        rating_per_year = review_dt.groupBy("year" , "review_ts").agg(avg("rating").alias("avg_rating") ,
                                                        count("movie_id").alias("total_movie") ,
                                                        count("review_id").alias("total_review"))
        
        return rating_per_year

    # ===================================================================
    # 5 : Khai phá dữ liệu , dự đoán cảm xúc người dùng
    # ===================================================================
    def data_mining(review) :    
        
        review_label =  review.withColumn("sentiment" , when(col("star") >= 8 , "Positive") \
                                                            .when(col("star") >= 5 , "Neutral") \
                                                            .otherwise("Negative")) 
        pdf = review_label.toPandas()


        # ----------------------------
        # 1. Star distribution
        # ----------------------------
        plt.figure(figsize=(6,4))
        pdf["star"].hist(bins=12)
        plt.xlabel("Star")
        plt.ylabel("Count")
        plt.title("Star Distribution")
        plt.tight_layout()
        plt.savefig("../models/star_distribution.png")
        plt.close()

        # ----------------------------
        # 2. Sentiment distribution
        # ----------------------------
        plt.figure(figsize=(6,4))
        pdf["sentiment"].value_counts().plot(kind="bar")
        plt.xlabel("Sentiment")
        plt.ylabel("Count")
        plt.title("Sentiment Distribution")
        plt.tight_layout()
        plt.savefig("../models/sentiment_distribution.png")
        plt.close()

        # ----------------------------
        # 3. Scatter Like – Dislike
        # ----------------------------
        plt.figure(figsize=(6,4))
        plt.scatter(pdf["like"], pdf["dislike"])
        plt.xlabel("Like")
        plt.ylabel("Dislike")
        plt.title("Like vs Dislike")
        plt.tight_layout()
        plt.savefig("../models/like_dislike_scatter.png")
        plt.close()

        # ----------------------------
        # 4. Boxplot Likes theo sentiment
        # ----------------------------
        plt.figure(figsize=(6,4))
        sns.boxplot(x="sentiment", y="like", data=pdf)
        plt.title("Like Distribution by Sentiment")
        plt.tight_layout()
        plt.savefig("../models/like_by_sentiment.png")
        plt.close()

        print("✔ Ảnh đã lưu vào: models", )
        # 5. Lấy toàn bộ comment worldcount
        text_pdf = review_label.select("comment").toPandas()
        text = " ".join(text_pdf["comment"].fillna(""))

        wc = WordCloud(
            width=1200,
            height=800,
            background_color="white",
            stopwords=STOPWORDS,
            collocations=True
        ).generate(text)

        plt.figure(figsize=(12,8))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.tight_layout()
        path_wc = f"../models/wordcloud.png"
        plt.savefig(path_wc)
        plt.close()

        print("✔ Saved WordCloud:", path_wc)


        # 6 .Heatmap
        num_pdf = review_label.select("star", "like", "dislike").toPandas()
        corr = num_pdf.corr()

        plt.figure(figsize=(6,5))
        sns.heatmap(corr, annot=True, fmt=".2f", cmap="Blues")
        plt.title("Correlation Heatmap")
        plt.tight_layout()
        path_heat = f"../models/correlation_heatmap.png"
        plt.savefig(path_heat)
        plt.close()

        print("✔ Saved Heatmap:", path_heat)

        # ============================
        #  Chuẩn bị dữ liệu cho FP-Growth
        # ============================
        fp_df = review_label.select(
            array(
                "sentiment",
                col("star").cast("string"),
                when(col("like") > 500, "like_high").otherwise("like_low"),
                when(col("dislike") > 100, "dislike_high").otherwise("dislike_low")
            ).alias("items")
        )

        # ============================
        #  Train FP-Growth (Apriori)
        # ============================
        fp = FPGrowth(itemsCol="items", minSupport=0.1, minConfidence=0.1)
        model = fp.fit(fp_df)

        rules = model.associationRules
        rules_pdf = rules.toPandas()

        # Chuyển luật thành dạng chuỗi dễ đọc
        rules_pdf["rule"] = rules_pdf["antecedent"].apply(lambda x: ",".join(x)) + " → " + \
                            rules_pdf["consequent"].apply(lambda x: ",".join(x))

        # ============================
        #  biểu đồ 7: Biểu đồ cột Confidence theo luật (đã chỉnh sửa)
        # ============================

        # Rút gọn chuỗi rule nếu quá dài (ví dụ > 40 ký tự)
        def shorten(text, max_len=40):
            return text if len(text) <= max_len else text[:max_len] + "..."

        rules_pdf["rule_short"] = rules_pdf["rule"].apply(shorten)

        # Lấy top 20 luật theo confidence (không bị rối chữ)
        rules_top = rules_pdf.sort_values("confidence", ascending=False).head(20)

        plt.figure(figsize=(14, 10))   # ảnh lớn hơn cho dễ nhìn

        sns.barplot(
            y="rule_short",
            x="confidence",
            data=rules_top,
            palette="Blues_r"
        )

        plt.title("Association Rules – Confidence", fontsize=16)
        plt.xlabel("Confidence", fontsize=13)
        plt.ylabel("Rule", fontsize=13)

        # Tăng lề trái để nhãn không bị dính
        plt.subplots_adjust(left=0.40)

        # Giảm kích thước font nhãn
        plt.yticks(fontsize=10)

        plt.tight_layout()

        save_path1 = f"../models/association_rules_confidence.png"
        plt.savefig(save_path1, dpi=200, bbox_inches="tight")
        plt.close()

        print("✔ Saved:", save_path1)

        # ============================
        #  Vẽ biểu đồ 8: Scatter Support – Confidence
        # ============================
        plt.figure(figsize=(7,6))
        plt.scatter(rules_pdf["support"], rules_pdf["confidence"])
        plt.xlabel("Support")
        plt.ylabel("Confidence")
        plt.title("Support vs Confidence")
        plt.tight_layout()

        save_path2 = "../models/support_vs_confidence.png"
        plt.savefig(save_path2)
        plt.close()

        print("✔ Saved:", save_path2)

        # ============================
        #  Vẽ biểu đồ 9: Scatter Lift – Confidence
        # ============================
        plt.figure(figsize=(7,6))
        plt.scatter(rules_pdf["lift"], rules_pdf["confidence"])
        plt.xlabel("Lift")
        plt.ylabel("Confidence")
        plt.title("Lift vs Confidence")
        plt.tight_layout()

        save_path3 = "../models/lift_vs_confidence.png"
        plt.savefig(save_path3)
        plt.close()

        print("✔ Saved:", save_path3)

    def predict_sentiment(review) :
        review_label =  review.withColumn("label" , when(col("star") >= 8 , 1) \
                                                        .when(col("star") >= 5 , 2) \
                                                        .otherwise(0)) \
                                                        .drop("sentiment")
        #  Tokenize comment
        tokenizer = Tokenizer(inputCol="comment", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")

        # TF 
        vectorizer = CountVectorizer(inputCol="filtered", outputCol="raw_features")

        #  IDF
        idf = IDF(inputCol="raw_features", outputCol="features")

        # Classifier
        lr = LogisticRegression(featuresCol="features", labelCol="label")

        # Full pipeline
        pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, lr])

        train, test = review_label.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train)
        predictions = model.transform(test)

        predictions.select("comment", "label", "prediction", "probability").show(truncate=False)

    # ===================================================================
    # 6 : Tổng các đánh giá cảm xúc theo từng phim
    # ===================================================================

    def top_sentiment(review , movie) :
        print("Analysis 5 : Total ratings sentiment for each movie")

        review_sentiment =  review.withColumn("Positive" , when(col("star") >= 8 , 1).otherwise(0)) \
                                    .withColumn("Neutral" , when((col("star") >= 5) & (col("star") < 8) , 1).otherwise(0)) \
                                        .withColumn("Negative" , when(col("star") < 5 , 1).otherwise(0))

        review_join = review_sentiment.join(broadcast(movie) , "movie_id" , "inner")

        review_dt = review_join.dropDuplicates(["review_id"])

        top_sentiment = review_join.groupBy("title" , "year" , "rating" , "review_ts") \
                                    .agg(count("review_id").alias("total_review") ,
                                        sum("Positive").alias("Positive") ,
                                        sum("Negative").alias("Negative") ,
                                        sum("Neutral").alias("Neutral"))
        
        return top_sentiment
