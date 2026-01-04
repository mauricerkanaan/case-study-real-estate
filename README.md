# Case Study Real Estate

``` 
conda create -n redp python=3.12.12
```

**<u>To Do</u>**:
* ~~Create schema and import data~~ 
* ~~Airflow: SqlLite -> SqlLite (just remove the PK, and Not null from dwh for test)~~
* ~~Local LLM~~
* ~~I should review the encoding of location~~
* ~~Introduce the pipeline for SALES~~
* SCD2: 
    * ~~Extract~~
    * Load (update date + insert)
        * Let's start by the insert +  (src_id, dates) 
    * create index
* Put Scheduler to Daily
* Benchmark DE in Local LLM 
* CacheLLM: Correct the free text 
* Put parquet file in db/tmp folder 


``` python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task(a, b, **context):
    print(a + b)

with DAG(
    dag_id="pythonop_params_static",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="t1",
        python_callable=my_task,
        op_args=[2, 3],                 # positional args
        # or:
        # op_kwargs={"a": 2, "b": 3},    # keyword args
    )

```