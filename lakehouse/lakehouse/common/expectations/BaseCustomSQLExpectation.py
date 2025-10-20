# Databricks notebook source
# This will allow us to wrap custom sql tests in the GE framework
# Why bother with this? This will enable people who are proficient in SQL to write expectations - without needing to know anything about the framework under the hood and let us still dynamically create expectations purely from a sql statement and a couple of params/variables rather than define the expectation from scratch (the expectation this is based on already contains the nuts and bolts of what we're wanting to do)

# COMMAND ----------

import great_expectations as gx

class BaseCustomSQLExpectation(gx.expectations.UnexpectedRowsExpectation):
    """Base for dynamic SQL expectations."""

    #TODO: Custom severity? 

    @classmethod
    def build(cls, expectation_name: str, query: str, description: str):
        # Dynamically create a new class with the given name and attributes
        return type(
            expectation_name,
            (cls,),
            {
                'unexpected_rows_query': query,
                'description': description,
                '__doc__': description,
            }
        )