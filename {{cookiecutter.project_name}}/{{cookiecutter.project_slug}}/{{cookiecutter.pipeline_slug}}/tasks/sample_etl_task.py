from {{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}.common import Task
import pkg_resources
from time import time
import datetime
from datetime import date
from datetime import datetime

'''
Accepts/Expects the following parameters:
--conf-file
    YAML file which contains an ordered list of objects, each defining a filename containing a query.
    Additional parameters are defined in the object specific to the query within the file.
    Example Value:
        file:fuse://{{cookiecutter.project_slug}}/{{cookiecutter.pipeline_slug}}/conf/tasks/example.yml
--env
    Used to replace parameters within the sql file, but defined at the argument level so the conf-file can be the same between environments/
    Example Value:
        dev
--debug
    If defined as True, the class will print the queries and results in the output.
    Example Value:
        True
--overrides
    A json like dictionary of parameters which will override any value set within the conf-file for all queries.
    Example Value:
        {\"schema\":\"test\",\"table\":\"test\"}

'''

class SQLTask(Task):
    def _execute_sql(self):
        queries = self.conf["queries"]
        for query in queries:
            query["env"] = self.conf["env"]
            query.update(self.conf)
            if "filename" in query:
                query_path = pkg_resources.resource_filename("{{cookiecutter.project_slug}}.{{cookiecutter.pipeline_slug}}", f"resources/sql/{query.get('filename')}")
                q = ""
                with open(query_path) as file:
                    lineCountS = 0
                    lineCountE = 0
                    for line in file:
                        lineCountE += 1
                        l = f" {line.rstrip().format_map(query)}"
                        #hand comments
                        if "--" in l:
                            l = l[:l.rfind("--")]
                        if len(l.strip()) > 0:
                            q += l
                        #if query is ended via semi colon, execute query and reset q for next query
                        if ";" in q:
                            query(q=q, lineCountS=lineCountS, lineCountE=lineCountE, query_path=query_path)
                            q = ""
                            lineCountS = lineCountE +1
                    #run a dangling query which wasn't ended with a semi colon
                    if len(q.strip()) > 0:
                        query(q=q, lineCountS=lineCountS, lineCountE=lineCountE, query_path=query_path)
                        q = ""



    def query(self,q, lineCountS, lineCountE, query_path):
        if self.conf["debug"]:
            start_sqldata=time()
            print(f"Executing lines {lineCountS} to {lineCountE} of {query_path} - {q}")
        df = self.spark.sql(q)
        if self.conf["debug"]:
            stop_sqldata=time()
            print(f"Execution complete, duration = {stop_sqldata-start_sqldata} seconds.")
            df.select("*").show()
          
    def launch(self):
        self.logger.info("Launching SQL task")
        self._execute_sql()
        self.logger.info("SQL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SQLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
