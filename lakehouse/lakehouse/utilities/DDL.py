# Databricks notebook source
from pyspark.sql.types import StructType

class DDL:
    """
    Basic class that implements the factory pattern to help generate DDL.
    """
    def __init__(self, table_name: str, columns: list[dict], table_operation: str=None, additional_columns: list[dict]=None, table_properties: dict=None) -> None:
        self.table_name = table_name
        self.columns = columns

        self.table_operation = table_operation or 'CREATE TABLE IF NOT EXISTS'
        self.additional_columns = additional_columns or []
        self.table_properties = table_properties or {}

        self.ddl = None

    @classmethod
    def from_dict(cls, table_name: str, schema_dict: dict, table_operation: str=None, additional_columns: list=None, table_properties: dict=None):
        """Factory method to create DDL instance from a schema dict."""
        columns = schema_dict.get('fields', [])
        instance = cls(table_name, columns, table_operation, additional_columns, table_properties)
        instance.compile()
        return instance

    @classmethod
    def from_struct(cls, table_name: str, struct: StructType, table_operation: str=None, additional_columns: list=None, table_properties: dict=None):
        """Factory method to create DDL instance from a Spark StructType."""
        schema_dict = struct.jsonValue()
        return cls.from_dict(table_name, schema_dict, table_operation, additional_columns, table_properties)

    def compile(self) -> None:
        """Compile the DDL"""
        self.target_field_ddl = self.format_columns()
        self.properties_ddl = self.format_table_properties()

        self.ddl = f"""{self.table_operation} {self.table_name} (
        {self.target_field_ddl}
        )
        {self.properties_ddl}
        """

    def format_columns(self) -> str:
        """Create DDL field list from schema dict."""
        
        column_list = [
            f"{field.get('name')} {field.get('type').upper()}"
            for field in self.columns + self.additional_columns
        ]

        return ',\n    '.join(column_list)

    def format_table_properties(self) -> str:
        if not self.table_properties:
            return ''
        properties = ', '.join([f"{k} = '{v}'" for k, v in self.table_properties.items()])
        return f"\nTBLPROPERTIES ({properties})"

    def __str__(self):
        return self.ddl or ''
