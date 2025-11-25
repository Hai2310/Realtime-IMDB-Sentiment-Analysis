from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
import datetime
default_args = {
    "owner" : "airflow" ,
    "retry_delay" : datetime.timedelta(minutes = 5) ,
    "depends_on_past" : False ,
    "retries" : 3 ,
    "email_on_failure" : True , 
    "email_on_retry" : False
}

with DAG(
    dag_id = "spark_psql" , 
    start_date = datetime.datetime(2025,1,1) , 
    schedule = "@once" ,
    default_args = default_args ,
    catchup = False
) as dag :
    
    spark_processing = BashOperator(
        task_id = "spark_processing" ,
        bash_command = """spark-submit \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.johnsnowlabs.nlp:spark-nlp_2.12:5.4.0 \
                            --jars /home/enovo/prj/test/postgresql-42.7.3.jar \
                            --conf "spark.driver.extraJavaOptions=-javaagent:/opt/jmx_prometheus_javaagent-0.20.0.jar=8090:/opt/spark.yml" \
                            --conf "spark.executor.extraJavaOptions=-javaagent:/opt/jmx_prometheus_javaagent-0.20.0.jar=8091:/opt/spark.yml" \
                            /home/enovo/prj/test/spark/main.py"""
    )

    email_success = EmailOperator(
        task_id = "email_success" ,
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "SUCCESS : IMDB ETL PIPELINE",
        html_content = "<h3> Pipeline completed successfully </h3>",
        trigger_rule =  "all_success"
    )

    email_fail = EmailOperator(
        task_id = "email_fail" ,
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "FAIL : IMDB ETL PIPELINE",
        html_content = "<h3> Pipeline fail - Check airflow logs </h3>",
        trigger_rule =  "one_failed"
    )
    
    
    
    spark_processing >> [email_success , email_fail]