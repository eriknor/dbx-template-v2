from great_expectations.data_context import BaseDataContext
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.common import Task
import pkg_resources

class GreatExpectations(Task):

    def launch(self):
        self.logger.info("Launching expectations task")
        self._run_expectations()
        self.logger.info("python expectations finished!")

    def _run_expectations(self, df):
        conf = self.conf["conf"]

        # Load the "gold" table as a Spark DataFrame
        gold_df = self.spark.table(f"{self.conf['env']}{conf['source_catalog']}{conf['source_schema']}gold")

        # Convert the Spark DataFrame to a Great Expectations SparkDFDataset
        dataset = SparkDFDataset(gold_df)

        # Create a Great Expectations DataContext
        context = BaseDataContext()

        # Define the expectation to check for null values in the "fare_amount" column
        expectation_suite_name = "sample_etl_expectation_suite"
        context.create_expectation_suite(expectation_suite_name)
        context.expectation_suite(expectation_suite_name).expect_column_values_to_not_be_null("fare_amount")

        # Validate the dataset against the expectation suite
        results = dataset.validate(expectation_suite_name=expectation_suite_name, data_context=context)

        # Print the validation results
        print(results)

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = GreatExpectations()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()