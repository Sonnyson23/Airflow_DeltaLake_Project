from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

def notify_email_on_failure(context):
    task_instance = context.get("task_instance")
    log_url = task_instance.log_url
    subject = f"[Airflow] Hata: {task_instance.task_id} başarısız oldu!"
    html_content = f"""
    <h3>Görev Hatası Bildirimi</h3>
    <p><strong>DAG:</strong> {task_instance.dag_id}</p>
    <p><strong>Görev:</strong> {task_instance.task_id}</p>
    <p><strong>Tarih:</strong> {context.get('execution_date')}</p>
    <p><strong>Log:</strong> <a href="{log_url}">Görev Logları</a></p>
    """
    send_email(
        to=["ademsonuvar@gmail.com"],
        subject=subject,
        html_content=html_content
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    on_failure_callback=notify_email_on_failure  # <-- burada olmalı
) as dag:

    t0 = BashOperator(
        task_id='ls_data',
        bash_command='adem naber',  # ← bilerek hatalı bırakılmış görev
        retries=2,
        retry_delay=timedelta(seconds=15)
    )
 # 'ls -l /tmp'

    t1 = BashOperator(task_id='download_data',
                      bash_command='curl -L -o /tmp/dirty_store_transactions.csv https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/churn-telecom/cell2celltrain.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t2 = BashOperator(task_id='check_file_exists', bash_command='sha256sum /tmp/dirty_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t0 >> t1 >> t2