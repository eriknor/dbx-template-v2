from {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.common import Task
import pkg_resources


class PythonTask(Task):
    def _execute_sql(self):
        self.logger.info("Launching python task")
        conf = self.conf["conf"]
        data = self.spark.read.table(f"{self.conf['env']}{conf['source_catalog']}{conf['source_schema']}silver")
        filtered_data = data.filter(data.trip_distance >= 3)
        filtered_data.write.saveAsTable(f"{self.conf['env']}{conf['destination_catalog']}{conf['destination_schema']}gold")

        
    def launch(self):
        self.logger.info("Launching python task")
        self._execute_sql()
        self.logger.info("python task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = PythonTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()