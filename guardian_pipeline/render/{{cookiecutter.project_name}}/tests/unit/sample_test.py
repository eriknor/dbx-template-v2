from {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.tasks.sample_etl_task import SQLTask
from {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.tasks.sample_python_task import PythonTask

from pyspark.sql import SparkSession
from pathlib import Path
import logging

def test_jobs(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job")
    common_config = [
            {
                "filename":"test.sql"
            },
            
            {
            "filename":"example01.sql", 
            "schema": "",
            "catalog":"",
            "source_catalog": "",
            "source_schema": ""},
            {
            "filename":"example02.sql", 
            "schema": "",
            "catalog":"",
            "source_catalog": "",
            "source_schema": ""},

            ]
    test_etl_config = {"queries": common_config, "env":""}
    etl_job = SQLTask(spark, test_etl_config)
    etl_job.launch()
    table_name = f"example"
    _count = spark.table(table_name).count()
    assert _count > 0
    logging.info("Testing the ETL job - done")
    logging.info("Testing the python job")
    common_config = {
            "destination_schema": "",
            "destination_catalog":"",
            "source_catalog": "",
            "source_schema": ""}
            
    test_etl_config = {"conf": common_config, "env":""}
    etl_job = PythonTask(spark, test_etl_config)
    etl_job.launch()
    table_name = f"gold"
    _count = spark.table(table_name).count()
    assert _count > 0
    logging.info("Testing the ETL job - done")

