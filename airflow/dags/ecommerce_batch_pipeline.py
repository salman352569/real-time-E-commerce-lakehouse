from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime ,timedelta

default_args ={
    "owner": "salman",
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)

}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="End-to-End Ecommerce batch pipeline",
    default_args= default_args,
    start_date =datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["spark","batch","ecommerce"]
) as dag:

     generate_large_data = BashOperator(
          task_id = "generate_large_data",
          bash_command = """
         echo "JAVA_HOME=$JAVA_HOME"
         which java
         java -version
         cd /opt/airflow/project &&
         python -m spark.generators.generate_large_data
"""
     )
     bronze_orders = BashOperator(
          task_id ="bronze_orders",
          bash_command ="""
          echo "JAVA_HOME=$JAVA_HOME"

          which java

          java -version

          cd /opt/airflow/project &&
          python -m spark.bronze.bronze_orders
"""
     )
     silver_orders=BashOperator(
          task_id ="silver_orders",
          bash_command="""
          export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
          cd /opt/airflow/project &&
          python -m spark.silver.silver_orders
"""  )
     customer_dimension_init =BashOperator(
           task_id ="customer_dimension_init",
           bash_command ="""
           export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
           cd /opt/airflow/project &&
           python -m spark.silver.customer_dimension_init
"""
     )
     customer_updates=BashOperator(
          task_id="customer_updates",
          bash_command="""
          export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
          cd /opt/airflow/project &&
          python -m spark.silver.customer_updates
"""
     )
     schema_evolution =BashOperator(
          task_id="schema_evolution",
          bash_command ="""
          export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
          cd /opt/airflow/project &&
          python -m spark.silver.schema_evolution
"""
     )
     gold_order_summary=BashOperator(
          task_id ="gold_order_summary",
          bash_command ="""
          export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
          cd /opt/airflow/project &&
          python -m spark.gold.gold_order_summary
"""
     )
     gold_order_status =BashOperator(
         task_id ="gold_order_status",
         bash_command="""
         export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
         cd /opt/airflow/project &&
         python -m spark.gold.gold_order_status
"""
      )
     gold_order_by_state = BashOperator(
         task_id ="gold_order_by_state",
         bash_command ="""
         export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
         cd /opt/airflow/project &&
         python -m spark.gold.gold_order_by_state   
"""
     )
     gold_top_customers = BashOperator(
          task_id ="gold_top_customers",
          bash_command ="""
          export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
          cd /opt/airflow/project &&
          python -m spark.gold.gold_top_customers
"""
     )


generate_large_data >> bronze_orders >> silver_orders

silver_orders >> customer_dimension_init

customer_dimension_init >> customer_updates 

customer_updates >> schema_evolution
schema_evolution >> gold_order_summary
schema_evolution >> gold_order_by_state
schema_evolution >> gold_top_customers
schema_evolution >> gold_order_status