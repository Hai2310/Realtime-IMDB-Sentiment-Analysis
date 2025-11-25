from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel

spark = SparkSession.builder \
    .appName("SentimentFlask") \
    .master("local[*]") \
    .getOrCreate()

model = PipelineModel.load("models/tf_idf_model")

app = Flask(__name__)

label_map = {0: "Positive", 1: "Negative" , 2: "Neutral"}

@app.route("/", methods=["GET", "POST"])
def home():
    prediction = None

    if request.method == "POST":
        comment = request.form["comment"]
        df = spark.createDataFrame([(comment,)], ["comment"])
        pred_row = model.transform(df).collect()[0]
        pred_index = int(pred_row["prediction"])
        prediction = label_map[pred_index]

    return render_template("index.html", prediction=prediction)

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/charts")
def charts():
    image_files = [
        ("Association Rules – Confidence", "static/association_rules_confidence.png"),
        ("Support vs Confidence", "static/support_vs_confidence.png"),
        ("Lift vs Confidence", "static/lift_vs_confidence.png"),
        ("Like by Sentiment", "static/like_by_sentiment.png"),
        ("Like/Dislike Scatter", "static/like_dislike_scatter.png"),
        ("Sentiment Distribution", "static/sentiment_distribution.png"),
        ("Star Distribution", "static/star_distribution.png"),
        ("Wordcloud", "static/wordcloud.png"),
    ]

    return render_template("charts.html", images=image_files)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
