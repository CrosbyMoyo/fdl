# Databricks notebook source
# MAGIC %run ./hash_utils

# COMMAND ----------

import yaml
from pyspark.sql.dataframe import DataFrame

# COMMAND ----------

#TODO: [NW] Group up the methods of these classes based on what is generic for all metadata, same signature but silver implementation, specific to silver
# If you're reviewing a PR and this is here, tell me to do it 

class MetadataYaml:
    """Reads a yaml metadata file and exposes the properties"""

    def __init__(self, file_path: str):

        self.ddl_col_delimiter = ', '

        self._etl_columns = {
            '__etl_keys_fprint': 'BIGINT',
            '__etl_row_fprint': 'BIGINT',
            '__etl_effective_from': 'DATE',
            '__etl_effective_to': 'DATE',
            '__etl_is_active': 'BOOLEAN',
            '__etl_is_deleted': 'BOOLEAN'
        }

        with open(file_path, 'r') as f:
            self._metadata = yaml.safe_load(f)

    # TODO: These methods should be defined in either a parent class or an interface 

    @property
    def yaml(self) -> dict:
        return self._metadata

    def source_2partname(self, include_schemaversion: bool = False) -> str:

        schema = self._metadata.get('source').get('schema')
        table = self._metadata.get('source').get('table')

        rtn_name = f'{schema}.{table}'

        return rtn_name
    
    def sources_2partname(self, table: str, include_schemaversion: bool=False) -> str: 
        source = [
            source for source 
            in self._metadata.get('sources')
            if source['table'] == table
        ]
       
        if source:
            rtn_name = f"{source[0].get('schema')}.{source[0].get('table')}"
        
        return rtn_name
    

    @property
    def etl_columns(self) -> list:
        return self._etl_columns.keys()

    def destination_2partname(self, stage: str, include_schemaversion: bool = False) -> str:

        rtn_name = None

        stage_dict = [
            s for s
            in self._metadata.get('destinations')
            if s['stage'] == stage
        ]

        if stage_dict:
            props = stage_dict[0]
            schema = props.get('schema')
            table = props.get('table')

            rtn_name = f'{schema}.{table}'

        return rtn_name

    def get_columns(self, role: str, prefix: str='') -> list:
        """Get the columns for a specific role"""
        return [
            f'''{prefix}{c['rename_to']}'''
            for c in self._metadata.get('column_transformations')
            if c['column_role'] == role
        ]

    ## Column Getters

    def get_etl_columns(self, prefix: str='') -> str:
        """Get the metadata columns"""
        etl_columns = [
            f'''{prefix}{c}'''
            for c in self._etl_columns.keys()
        ]
        return etl_columns


    def get_etl_columns_ddl(self, prefix: str='') -> str:
        """Get the metadata columns as a string"""
        etl_columns = self.get_etl_columns(prefix)
        return f'{self.ddl_col_delimiter}'.join(etl_columns)


    def get_columns_ddl(self, role: str, prefix: str='') -> list:
        """Get the columns for a specific role"""
        columns = self.get_columns(role, prefix)
        return f'{self.ddl_col_delimiter}'.join(columns)


    def get_payload_columns(self, prefix: str='') -> list:
        """Get the non key columns in the metadata"""
        return self.get_columns('PAYLOAD', prefix)


    def get_payload_columns_ddl(self, prefix: str='') -> str:
        """Get the non key columns in the metadata as a string"""
        return self.get_columns_ddl('PAYLOAD', prefix)


    def get_key_columns(self, prefix: str='') -> list:
        """Get the key columns in the metadata sorted lexicographically"""
        key_columns = self.get_columns('PK', prefix)
        key_columns.sort()
        return key_columns

    def get_key_columns_ddl(self, prefix: str='') -> str:
        """Get the ey columns in the metadata as a string"""
        return self.get_columns_ddl('PK', prefix)

    ## Merge Helpers
    # TODO: I think we should take the functionality for writing data to the next layer in a separate class

    def get_update_set_ddl(self, src_prefix: str='src.', tgt_prefix: str='tgt.') -> str: 
        """Format the merge match statement for a sql merge statement"""
        match_cols = [
            f'''{tgt_prefix}{c['rename_to']} = {src_prefix}{c['rename_to']}'''
            for c in self._metadata.get('column_transformations')
        ]

        return f'{self.ddl_col_delimiter}'.join(match_cols)

    def get_insert_ddl(self, prefix: str='') -> str:
        """Get the columns in the metadata as a string for a sql statement"""
        cols = [
            f'''{prefix}{c['rename_to']}'''
            for c in self._metadata.get('column_transformations')
        ]

        etl_cols = self.get_etl_columns(prefix)

        return f'{self.ddl_col_delimiter}'.join(cols + etl_cols)

    def get_merge_condition(self) -> str:
        """
        Get the merge condition as a list of primary key comparisons 
        """
        return [f'{s} = {t}' for s, t in list(zip(self.get_key_columns('src.'), self.get_key_columns('tgt.')))]

    def get_merge_condition_ddl(self) -> str: 
        """
        Get the merge condition as a string for a sql statement based on primary key columns 
        """
        return ' AND '.join(self.get_merge_condition())
    
    def get_key_fprint_ddl(self, prefix: str='') -> str: 
        """
        Get the fprint ddl as a string for a sql statement.
        """
        return hash_ddl(self.get_key_columns(prefix=prefix))
    
    def get_row_fprint_ddl(self, prefix: str='') -> str: 
        """
        Get the fprint ddl as a string for a sql statement.
        """
        return hash_ddl(self.get_payload_columns(prefix=prefix))

    def get_merge_ddl(self, source_name: str, target_name: str) -> str:
        """
        Get the merge query as a string for a sql statement join query will be on the etl fprint.
        """
        query = f'''
            MERGE INTO {target_name} AS tgt
            USING {source_name} AS src
            ON
                {self.get_merge_condition_ddl()}
            WHEN MATCHED AND NOT tgt.__etl_row_fprint = src.__etl_row_fprint THEN
                UPDATE SET
                    {self.get_update_set_ddl('src.', 'tgt.')}
                    {self.ddl_col_delimiter} tgt.__etl_row_fprint = {self.get_row_fprint_ddl('src.')}
            WHEN NOT MATCHED THEN
                INSERT (
                    {self.get_key_columns_ddl()}
                    ,{self.get_payload_columns_ddl()}
                    ,{self.get_etl_columns_ddl()}
                )
                VALUES (
                    {self.get_key_columns_ddl('src.')}
                    ,{self.get_payload_columns_ddl('src.')}
                    ,{self.get_etl_columns_ddl('src.')}
                )
        '''

        return query
    
    def test_uniqueness(self, table_name: str, run_test: str) -> None:
        # TODO: This will logic will get carried to the expectations logic
        """
        Test the uniqueness of the primary key values.
        """

        if run_test == 'True':
            keys = self.get_key_columns_ddl()

            query = f'''
                SELECT 
                    {keys}, 
                    __etl_effective_from,
                    count(*) AS occurance 
                FROM {table_name} 
                GROUP BY 
                    {keys}, 
                    __etl_effective_from 
                HAVING count(*) > 1 
                ORDER BY occurance DESC;                  
            '''
            
            temp = spark.sql(query)
            temp.display()

            if temp.count() > 0:
                raise Exception(f'The primary keys are not unique. Please review the data and ensure the primary keys are unique. \n The current table has the following primary keys: {keys}')
            else:
                print('The primary keys are unique.')
        
    def add_etl_fields(self, dataframe: DataFrame) -> DataFrame: 
        """Add the etl fields to a dataframe"""
        dataframe.createOrReplaceTempView('transformed')
        return spark.sql(self.get_etl_fields_ddl('transformed'))

    def get_key_fprint_hash_ddl(self, prefix: str='') -> str: 
        """Get the fprint ddl as a string for a sql statement."""
        return hash_ddl(self.get_key_columns(prefix=prefix)) + ' AS __etl_keys_fprint'
    

    def get_row_fprint_hash_ddl(self, prefix: str='') -> str: 
        """Get the fprint ddl as a string for a sql statement."""
        return hash_ddl(self.get_payload_columns(prefix=prefix)) + ' AS __etl_row_fprint'
    

    def get_etl_fields_ddl(self, temp_view: str='transformed') -> str: 
        """Apply required hashing"""
        return f'''
            SELECT 
                t.*
                ,{self.get_key_fprint_hash_ddl('t.')}
                ,{self.get_row_fprint_hash_ddl('t.')}
                ,current_timestamp() AS __etl_effective_from
                ,NULL                AS __etl_effective_to
                ,TRUE                AS __etl_is_active
                ,FALSE               AS __etl_is_deleted
            FROM
                {temp_view} AS t
        '''
    
    def write(self, dataframe: DataFrame, target_name: str) -> dict:
        """Write the data to the target based on the operation in the metadata."""
        dataframe.createOrReplaceTempView('_final')
        if self._metadata['destinations'][-1].get('operation') == 'merge':
            return self.merge('_final', target_name)
        elif self._metadata['destinations'][-1].get('operation') == 'insert_overwrite':
            return self.insert_overwrite('_final', target_name)
        else:
            raise KeyError(f"Missing or Unsupported write operation in metadata: {self._metadata.get('operation')}")

    def get_write_ddl(self, source_name: str, target_name: str) -> dict:
        """Write the data to the target based on the operation in the metadata."""
        if self._metadata['destinations'][-1].get('operation') == 'merge':
            return self.get_merge_ddl(source_name, target_name)
        elif self._metadata['destinations'][-1].get('operation') == 'insert_overwrite':
            return self.get_insert_overwrite_ddl(source_name, target_name)
        else:
            raise KeyError(f"Missing or Unsupported write operation in metadata: {self._metadata.get('operation')}")


    def insert_overwrite(self, source_name: str, target_name: str) -> dict:
        """Insert overwrite a table."""
        overwrite_statement = self.get_insert_overwrite_ddl(source_name, target_name)
        overwrite_result = spark.sql(overwrite_statement)
        return overwrite_result.toPandas().head(1).to_dict()
    

    def get_insert_overwrite_ddl(self, source_name: str, target_name: str) -> str: 
        """Get the insert overwrite query as a string for a sql statement"""
        query = f'''
            INSERT OVERWRITE {target_name} (
                -- Keys
                {self.get_key_columns_ddl()},
                -- Payload
                {self.get_payload_columns_ddl()},
                -- Metadata
                {self.get_etl_columns_ddl()}
            ) SELECT
                -- Keys
                {self.get_key_columns_ddl('src.')},
                -- Payload
                {self.get_payload_columns_ddl('src.')},
                -- Metadata
                {self.get_etl_columns_ddl('src.')}
            FROM {source_name} AS src
        '''

        return query
    

    def generate_ddl_from_dataframe(self, tablename: str, dataframe: DataFrame, replace: bool=False) -> str: 
        """Generate the DDL string from a Dataframe"""

        payload_list = [
            f"\t{field.name} {field.dataType.simpleString()}\n\t\tCOMMENT '{field.name.replace('_', ' ').title()}'"
            for field in dataframe.schema.fields
        ]

        metadata_list = [
            f"\t{field} {datatype}"
            for field, datatype in self._etl_columns.items()
        ]

        column_list = payload_list + metadata_list

        column_ddl  = ",\n".join(column_list)

        if replace:
            return f"CREATE OR REPLACE TABLE {tablename} (\n{column_ddl}\n)\nCLUSTER BY AUTO;"
        else:
            return f"CREATE TABLE IF NOT EXISTS {tablename} (\n{column_ddl}\n)\nCLUSTER BY AUTO;"
        

    def generate_destination_ddl(self, catalog: str, replace: bool=False) -> str:
        """Generate the DDL for the destination based on the metadata"""
        
        final = self._metadata.get('destinations')[-1]
        table_name = f'''{catalog}.{final['schema']}.{final['table']}'''

        keys_list = [
            f"\t{c['rename_to']} {c['cast_to']}\n\t\tCOMMENT '{c['rename_to'].replace('_', ' ').title()}'"
            for c in self._metadata.get('column_transformations')
            if c['column_role'] == 'PK'
        ]
                
        payload_list = [
            f"\t{c['rename_to']} {c['cast_to']}\n\t\tCOMMENT '{c['rename_to'].replace('_', ' ').title()}'"
            for c in self._metadata.get('column_transformations')
            if c['column_role'] == 'PAYLOAD' or c['column_role'] == 'DERIVED'
        ]

        metadata_list = [
            f"\t{field} {datatype}"
            for field, datatype in self._etl_columns.items()
        ]

        column_list = ['\t-- Keys'] + keys_list + ['\t-- Payload'] + payload_list + ['\t-- Metadata'] + metadata_list

        column_ddl  = ",\n".join(column_list)

        if replace:
            return f"CREATE OR REPLACE TABLE {table_name} (\n{column_ddl}\n)\nCLUSTER BY AUTO;"
        else:
            return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{column_ddl}\n)\nCLUSTER BY AUTO;"
        
    def process_transformation_table(self, table_name: str, catalog: str) -> dict: 
        """Process a transformation query"""
        return self.process_transformation_query(f"SELECT * FROM {table_name}", catalog)
        
    def process_transformation_query(self, query: str, catalog: str) -> dict: 
        """Process a transformation query"""
        final = self._metadata.get('destinations')[-1]
        table_name = f'''{catalog}.{final['schema']}.{final['table']}'''

        process_query = f'''
            -- Transformation 
            WITH transformed AS (
                {query}
            )

            -- Add metadata
            , with_metadata AS (
                {self.get_etl_fields_ddl('transformed')}
            )

            -- Write to target
            {self.get_write_ddl('with_metadata', table_name)}
        '''

        try:
            result = spark.sql(process_query)
            result_dictionary = result.toPandas().head(1).to_dict()
        except Exception as err: 
            raise Exception(f'Failed to process transformation query: {process_query}')

        return result_dictionary
    