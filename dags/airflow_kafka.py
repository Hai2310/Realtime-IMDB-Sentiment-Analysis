from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime , timedelta 
from hdfs import InsecureClient

HDFS_URL = "http://localhost:9870"
HDFS_PATH = "/IMDB/data"

def check_file_hdfs(path) :
    try :
        cilent = InsecureClient(HDFS_URL , user = "hdfs") 
        files = cilent.list(path) 
        for file in files :
            if file.endswith("json") :
                return True
        return False
    except Exception as e :
        return False


default_args = {
    "owner" : "airflow" ,
    "retries" : 3 ,
    "email_on_failure" : True ,
    "email_on_retry" : False ,
    "depends_on_past" : False , 
    "retry_delay" : timedelta(minutes = 5) 
}
with DAG(
    dag_id = "hdfs_kafka" , 
    description = "This is IMDB ETL Pipeline " ,
    schedule = "@daily" , 
    start_date = datetime(2024,1,1) ,
    default_args = default_args ,
    catchup = False
) as dag :
    wait_file_movie_hdfs = PythonSensor(
        task_id = "wait_file_movie_hdfs" ,
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" , 
        op_kwargs = {"path" : "/IMDB/data/movie"}
    )

    wait_file_actor_hdfs = PythonSensor(
        task_id = "wait_file_actor_hdfs" ,
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" , 
        op_kwargs = {"path" : "/IMDB/data/actor"}
    )

    wait_file_review_hdfs = PythonSensor(
        task_id = "wait_file_review_hdfs" ,
        python_callable = check_file_hdfs ,
        poke_interval = 10 ,
        timeout = 300 ,
        mode = "poke" , 
        op_kwargs = {"path" : "/IMDB/data/review"}
    )

    push_hdfs_to_kafka = BashOperator(
        task_id = "push_hdfs_to_kafka" ,
        bash_command = "python3 /home/enovo/prj/test/kafka/push_kafka.py" 
    )

    email_success = EmailOperator(
        task_id = "email_success" ,
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "SUCCESS : Push data from HDFS to Kafka ",
        html_content = "<h3> Push data successfully </h3>",
        trigger_rule =  "all_success"
    )

    email_fail = EmailOperator(
        task_id = "email_fail" ,
        to = ["minhhaiit1k68@gmail.com"] ,
        subject = "FAIL :  Push data from HDFS to Kafka",
        html_content = "<h3> Push data fail - Check airflow logs </h3>",
        trigger_rule =  "one_failed"
    )
    
    
    
    [wait_file_movie_hdfs , wait_file_actor_hdfs , wait_file_review_hdfs] >> push_hdfs_to_kafka  >> [email_success , email_fail]