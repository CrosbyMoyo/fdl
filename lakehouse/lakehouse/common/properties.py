# Databricks notebook source
# This Notebook is for common properties and settigns
# All code should be in Classes

# COMMAND ----------

import os
import logging
import json

# COMMAND ----------

#
# Classes
#

# COMMAND ----------

class JSONFormatter(logging.Formatter):
    def format(self, record):
        """The standard template for log messages"""
        log_record = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage()
        }

        return json.dumps(log_record)


# COMMAND ----------

class NotebookLogger:
    """Manages logging messgaes from Notebooks"""

    def __init__(self):
        self._logger = logging.getLogger('notebook_logger')
        self._logger.setLevel(logging.DEBUG)

        # TODO: add more handlers
        cell_output = logging.StreamHandler()
        cell_output.setLevel(logging.DEBUG)

        formatter = JSONFormatter(datefmt='%Y-%m-%d %H:%M:%S')
        cell_output.setFormatter(formatter)

        if not self._logger.hasHandlers():
            self._logger.addHandler(cell_output)

    @property
    def log(self) -> logging.Logger:
        return self._logger

# Logger
# NB: this is the bare-bones logger.  Ideally we need to integrate this with log4j.
# We also need to check with Vivo if they have corporate standards around logging (e.g. everything goes to Splunk?)

# COMMAND ----------

class EnVars:
    """Holds the environment variables"""

    def __init__(self):
        self._env = os.environ.get('ENV')
        self._bronze_catalog = os.environ.get('BRZ_CATALOG')
        self._silver_catalog = os.environ.get('SLV_CATALOG')
        self._gold_catalog = os.environ.get('GLD_CATALOG')
        self._meta_catalog = os.environ.get('META_CATALOG')
        self._general_catalog = os.environ.get('GENERAL_CATALOG')

    @property
    def env(self) -> str:
        return self._env

    @property
    def bronze_catalog(self) -> str:
        return self._bronze_catalog

    @property
    def silver_catalog(self) -> str:
        return self._silver_catalog

    @property
    def gold_catalog(self) -> str:
        return self._gold_catalog
    
    @property
    def meta_catalog(self) -> str:
        return self._meta_catalog

    @property
    def general_catalog(self) -> str:
        return self._general_catalog

# COMMAND ----------

class RuntimeContext:
    """Returns contextual info about the currently running Notebook"""

    def __init__(self):

        self._username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().getOrElse(None)

        _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        _path_parts = _notebook_path.split('/')
        self._notebook_name = _path_parts[-1]

    @property
    def username(self) -> str:
        return self._username

    @property
    def notebook_name(self) -> str:
        return self._notebook_name


# COMMAND ----------

#
# Instantiate local classes below
# (NB: logger needs to be first)
#

# COMMAND ----------

logger = NotebookLogger()
logger.log.info('logger set up')

env_vars = EnVars()
logger.log.info('env_vars set up')

runtime_context = RuntimeContext()
logger.log.info('runtime_context set up')