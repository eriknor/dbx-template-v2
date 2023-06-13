from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Dict, Any
import yaml
import pathlib
from pyspark.sql import SparkSession
import sys
import json

def get_dbutils(
    spark: SparkSession,
):  # please note that this function is used in mocking by its name
    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._log_conf()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def get_dbutils(self):
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self):
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            result={}
        else:
            self.logger.info(f"Conf file was provided, reading configuration from {conf_file}")
            result=self._read_config(conf_file)
        result["env"] = self._provide_env()
        result["debug"] = self._provide_debug()
        result.update(self._provide_override())
        return result
        

    def _provide_env(self):
        self.logger.info("Reading env from --env job option")
        env = self._get_env()
        if not env:
            self.logger.info(
                "No env was provided, setting env to blank."
                "Please override configuration in subclass init method"
            )
            return ""
        else:
            self.logger.info(f"env was provided, set to {env}")
            return env
        
    def _provide_debug(self):
        self.logger.info("Reading debug from --debug job option")
        debug = self._get_debug()
        if not debug:
            self.logger.info(
                "No debug was provided, setting debug to blank."
                "Please override configuration in subclass init method"
            )
            return "False"
        else:
            self.logger.info(f"debug was provided, set to {debug}")
            return debug
        
    def _provide_override(self):
        self.logger.info("Reading overrides from --override job option")
        override = self._get_override()
        if not override:
            self.logger.info(
                "No override was provided, setting override to blank."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(f"override was provided, set to {override}")
            return override

    @staticmethod
    def _get_conf_file():
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _get_env():
        p = ArgumentParser()
        p.add_argument("--env", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.env


    @staticmethod
    def _get_debug():
        p = ArgumentParser()
        p.add_argument("--debug", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.env


    @staticmethod
    def _get_override():
        p = ArgumentParser()
        p.add_argument("--overrides", required=False, type=json.loads)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.env


    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self):
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self):
        """
        Main method of the job.
        :return:
        """
        pass
