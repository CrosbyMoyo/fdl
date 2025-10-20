# Databricks notebook source
# MAGIC %run ./BaseCustomSQLExpectation

# COMMAND ----------

import great_expectations as gx
from datetime import datetime, timezone
from pyspark.sql.dataframe import DataFrame
from pathlib import Path
import yaml 

class ExpectationSuite:

    def __init__(self, data_asset_name: str, data_source_name: str='spark') -> None:

        # TODO: Context should be passed in ?
        self.context = gx.get_context(mode='ephemeral')

        self.data_asset_name = data_asset_name        
        self.data_source_name = data_source_name
        self.batch_definition_name = f"{data_asset_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        self.suite = gx.ExpectationSuite(data_asset_name)
        self.context.suites.add(self.suite)

        self.data_source = self.context.data_sources.add_spark(name=data_source_name, persist=False)
        self.data_asset = self.data_source.add_dataframe_asset(name=data_asset_name)

        self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(self.batch_definition_name)

        self.batch_parameters = {}
        self.result = []

    # Suite Validation

    def validate_expectation_suite(self, dataframe: DataFrame=None, table_name: str=None) -> dict:
        """Validate all of the expectations in the suite against the batch definition."""
        if dataframe is not None:
            self.batch_parameters = {'dataframe': dataframe}
        elif table_name is not None:
            self.batch_parameters = {'dataframe': spark.table(table_name)}
        else:
            raise ValueError("Either dataframe or table_name must be provided.")
        
        self.batch_parameters = {'dataframe': dataframe}

        for expectation in self.suite.expectations:
            print(f"Executing expectation: {expectation.__repr__()}", end=' ... ')
            batch = self.batch_definition.get_batch(batch_parameters=self.batch_parameters)
            result = batch.validate(expectation)
            self.result.append(result)

        self.evaluate_expectation_suite()

    # Expectation Loading

    def load_generic_expectation(self, name: str, **kwargs) -> None: 
        """Load a single expectation as a dict to the suite."""
        callable_expectation = getattr(gx.expectations, name)
        expectation = callable_expectation(**kwargs)
        self.suite.add_expectation(expectation)

    def load_custom_sql_expectation_notebook(self, path: str, **kwargs) -> None:
        """Load custom SQL expectation notebook to the suite."""
        with open(path, 'r') as file:
            file_content = file.read()

        # Pass in any locals or env vars 
        context = kwargs | {
            'expectation_name': None,
            'description': None,
            'query': None
        }
        
        try:
            # Execute file content in isolated context
            exec(file_content, context)
        except Exception as e:
            raise RuntimeError(f"Error processing {path}") from e

        CustomSQLExpectation = BaseCustomSQLExpectation.build(
            expectation_name=context.get('expectation_name'), 
            description=context.get('description'), 
            query=context.get('query')
        )

        custom_sql_expectation = CustomSQLExpectation()

        self.suite.add_expectation(custom_sql_expectation)

    # Expectation Validation - Not actually used just handy to have for testing 

    def validate_generic_expectation(self, expectation_name: str, expectation_parameters: dict={}) -> dict:
        """Validate a generic expectation that is available in the gx.expectations suite"""

        callable_expectation = getattr(gx.expectations, expectation_name)
        expectation = callable_expectation(**expectation_parameters)
        batch = self.batch_definition.get_batch(batch_parameters=self.batch_parameters)

        return batch.validate(expectation)

    def validate_custom_sql_expectation(self, expectation_name: str, description: str, query: str, **kwargs) -> dict:
        """
        Register and execute a custom SQL expectation to execute on this batch. 
        Args:
            expectation_name: The name of the expectation
            description: The description of the expectation 
            query: The custom SQL that will return the rows that are unexpected i.e fail the test
        Returns:
            Dictionary of batch validation results
        """
        # Register the custom SQL expectation with GE using the BaseCustomSQLExpectation class
        CustomSQLExpectation = BaseCustomSQLExpectation.build(expectation_name=expectation_name, description=description, query=query)
        custom_sql_expectation = CustomSQLExpectation()
        batch = self.batch_definition.get_batch(batch_parameters=self.batch_parameters)

        return batch.validate(custom_sql_expectation)