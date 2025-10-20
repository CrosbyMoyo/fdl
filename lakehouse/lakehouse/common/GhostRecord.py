# Databricks notebook source
from pyspark.sql.types import StructType, DataType
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from delta.tables import DeltaTable

class GhostRecord:
    """
    A class to generate a ghost record for a delta table. This contains class methods that can easily be used without initialisation but still making use of the class attributes for consistency.

    Class Attributes: 
        - GHOST_SKEY: The skey value for the ghost record 
        - GHOST_DATATYPE_MAPPING: A dictionary mapping datatypes in DDL format to their ghost value 
        - GHOST_METADATA: A dictionary mapping metadata field names to their ghost value

    Parameters: 
        - table_name: The name of the table to generate the ghost record for
        - key_fields: The fields to merge if doing a merge operation. Default will be the key fprint
    
    Class Attributes:
        - table_schema: The schema of the table to generate the ghost record 
        - dict: The dictionary of the ghost values for this table
        - ghost_record: A DataFrame of a single record of the ghost values for this table 
    """

    # Values that will appear in the ghost record these should not change
    GHOST_SKEY = -1
    GHOST_DATATYPE_MAPPING = {
        'long': 0,
        'bigint': 0,
        'binary': 0,
        'boolean': False,
        'date': '1900-01-01',
        'decimal': 0.0,
        'double': 0.0,
        'float': 0.0,
        'integer': 0,
        'smallint': 0,
        'timestamp': '1900-01-01 00:00:00',
        'string': 'UNKNOWN'
    }
    GHOST_METADATA = {
        '__etl_keys_fprint': -1,
        '__etl_row_fprint': -1,
        '__etl_effective_from': '1900-01-01 00:00:00',
        '__etl_effective_to': '9999-12-31 23:59:59',
        '__etl_is_active': True,
        '__etl_is_deleted': False
    }

    def __init__(self, table_name: str, key_fields: list[str]=['__etl_keys_fprint']):
        self.table_name = table_name
        self.key_fields = key_fields

        self.table_schema = None 
        self.dict = None 
        self.dataframe = None 
        self.temp_view = 'ghost_record_temp_view'

        self.set_ghost_record()
        self.dataframe.createOrReplaceTempView(self.temp_view)


    def set_ghost_record(self) -> None: 
        """Set the ghost record"""
        self.table_schema = spark.table(self.table_name).schema
        self.ghost_values = self.generate_ghost_values_from_fields(self.table_schema.jsonValue()['fields'])
        self.dataframe = self.generate_ghost_record_from_schema(self.table_schema, self.ghost_values)
    

    @classmethod 
    def get_ghost_value_for_ddl_type(cls, datatype: str) -> object:
        """Get the ghost payload value for the given type in DDL format"""
        datatype_obj = DataType.fromDDL(datatype)
        return cls.GHOST_DATATYPE_MAPPING.get(datatype_obj.typeName().lower(), None)
    

    @classmethod
    def generate_ghost_values_from_fields(cls, fields: list[dict]) -> dict:
        """
        Generate the ghost values from the schema struct of a dataframe
        Arguments:
            - fields: A list of fields from the schema struct of a dataframe
        Returns:
            A dictionary mapping the field names to their ghost value
        """
        ghost_values = {}
        for field in fields:
            if field['name'][-5:] == '_skey':
                ghost_values[field['name']] = cls.GHOST_SKEY
            elif field['name'][:2] == '__':
                ghost_values[field['name']] = cls.GHOST_METADATA.get(field['name'].lower())
            else:
                ghost_values[field['name']] = cls.get_ghost_value_for_ddl_type(field['type'])

        return ghost_values


    @classmethod
    def generate_ghost_record_from_schema(cls, schema: StructType, ghost_values: dict=None) -> DataFrame:
        """Generate a ghost record from a provided schema. Returns a DataFrame of a single record."""
        if not ghost_values:
            ghost_values = cls.generate_ghost_values_from_fields(schema.jsonValue()['fields'])
        # Need to create a version of the schema with strings then cast to target types
        string_fields = [
            {**field, 'type': 'string'}
            for field in schema.jsonValue()['fields']
        ]
        string_schema = {'type': 'struct', 'fields': string_fields}

        string_ghost_record = spark.createDataFrame(data=[ghost_values], schema=StructType.fromJson(string_schema))
        casted_ghost_record = string_ghost_record.select(*[col(field['name']).cast(field['type']) for field in schema.jsonValue()['fields']])

        return casted_ghost_record
    

    @classmethod
    def generate_ghost_record_from_table(cls, table_name: str) -> DataFrame:
        """Generate a ghost record from a table. Returns a DataFrame of a single record."""
        return cls.generate_ghost_record_from_schema(spark.table(table_name).schema)
    