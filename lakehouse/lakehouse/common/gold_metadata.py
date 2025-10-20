# Databricks notebook source
# MAGIC %run ./hash_utils

# COMMAND ----------

# MAGIC %run ./GhostRecord

# COMMAND ----------

#TODO: [NW] Group up the methods of these classes based on what is generic for all metadata, same signature but gold implementation, specific to gold
# If you're reviewing a PR and this is here, tell me to do it 


from pyspark.sql.dataframe import DataFrame 
import yaml

class GoldMetadataYaml:
    """Reads a yaml metadata file and exposes the properties"""

    def __init__(self, file_path: str, slv_catalog: str, gld_catalog: str):
        
        self.ddl_col_delimiter = ', '

        # TODO: This class needs to be split out into separate implementations for this

        with open(file_path, 'r') as f:
            self._metadata = yaml.safe_load(f)

        self._slv_catalog = slv_catalog
        self._gld_catalog = gld_catalog

        self._source_aliases = [
            (t['alias'], self.source_3partname(t['name'], True))
            for t in self._metadata.get('source_tables')
        ]

        self.ghost = GhostRecord(self.dest_3partname(True))

        self._table_type = self._metadata.get('destination')['type'].lower()

        if self._table_type == 'fact':
            self._etl_columns = {
                '__etl_fprint': 'BIGINT',
                '__etl_load_timestamp': 'TIMESTAMP',
                '__etl_is_active': 'BOOLEAN',
                '__etl_is_deleted': 'BOOLEAN',
            }
        else: 
            self._etl_columns = {
                '__etl_keys_fprint': 'BIGINT',
                '__etl_row_fprint': 'BIGINT',
                '__etl_effective_from': 'DATE',
                '__etl_effective_to': 'DATE',
                '__etl_is_active': 'BOOLEAN',
                '__etl_is_deleted': 'BOOLEAN',
            }


    @property
    def yaml(self) -> dict: 
        return self._metadata

    def source_2partname(self, tablename: str, include_schemaversion: bool = False) -> str:

        return_name = None

        table_dict = [
            s for s
            in self._metadata.get('source_tables')
            if s['name'] == tablename
        ]

        if table_dict:
            props = table_dict[0]
            schema = props.get('schema')
            table = props.get('name')

            return_name = f'{schema}.{table}'

            sv = props.get('schemaversion')
            if include_schemaversion and sv:
                return_name = f'{return_name}_sv{sv}'

        return return_name


    def source_3partname(self, tablename: str, include_schemaversion: bool = False) -> str:

        sourcename = self.source_2partname(tablename, include_schemaversion)

        return f'{self._slv_catalog}.{sourcename}'
        # TODO: needs some error trapping


    def alias2src(self, alias: str) -> str:
        """A deliberately shorthand method for getting the 3-part name + schemaversion for a table alias"""

        src = [
            s[1]
            for s in self._source_aliases
            if s[0] == alias
        ]

        if src:
            return src[0]


    def dest_2partname(self, include_schemaversion: bool = False) -> str:

        return_name = None

        destination = self._metadata.get('destination')
        if destination:
            table = destination.get('name')
            schema = destination.get('schema')

            return_name = f'{schema}.{table}'

            sv = destination.get('schemaversion')
            if include_schemaversion and sv:
                return_name = f'{return_name}_sv{sv}'

        return return_name


    def dest_3partname(self, include_schemaversion: bool = False) -> str:

        destname = self.dest_2partname(include_schemaversion)

        return f'{self._gld_catalog}.{destname}'
    

    def get_columns(self, role: str, prefix: str='') -> list:
        """Get the columns for a specific role"""
        return [
            f'''{prefix}{c['name']}'''
            for c in self._metadata.get('columns')
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


    def get_skey_columns(self, prefix: str='') -> list: 
        """Get the Skey columns"""
        if self._metadata.get('destination').get('skey'): 
            return [
                f'''{prefix}{c['name']}'''
                for c in self._metadata.get('destination').get('skey').get('columns')
            ]


    def get_key_columns(self, prefix: str='') -> list:
        """Get the key columns in the metadata sorted lexicographically"""
        key_columns = self.get_columns('PK', prefix)
        key_columns.sort()
        return key_columns


    def get_key_columns_ddl(self, prefix: str='') -> str:
        """Get the ey columns in the metadata as a string"""
        return self.get_columns_ddl('PK', prefix)
    

    def get_skey_column_ddl(self, prefix: str='') -> str:
        """Get the surrogate column in the metadata as a string"""
        if self._metadata['destination'].get('skey'):
            return f'{prefix}{self._metadata["destination"].get("skey")["name"]}'
        else:
            return ''
    

    def get_update_set_ddl(self, src_prefix: str='src.', tgt_prefix: str='tgt.') -> str: 
        """Format the merge match statement for a sql merge statement"""
        match_cols = [
            f'''{tgt_prefix}{c['name']} = {src_prefix}{c['name']}'''
            for c in self._metadata.get('columns')
        ]

        return f'{self.ddl_col_delimiter}'.join(match_cols)


    def get_insert_ddl(self, prefix: str='') -> str:
        """Get the columns in the metadata as a string for a sql statement"""
        cols = [
            f'''{prefix}{c['rename_to']}'''
            for c in self._metadata.get('columns')
        ]

        etl_cols = self.get_etl_columns(prefix)

        return f'{self.ddl_col_delimiter}'.join(cols + etl_cols)


    def get_merge_condition(self) -> str:
        """Get the merge condition as a list of primary key comparisons"""
        return [f'{s} = {t}' for s, t in list(zip(self.get_key_columns('src.'), self.get_key_columns('tgt.')))]


    def get_merge_condition_ddl(self) -> str: 
        """Get the merge condition as a string for a sql statement based on primary key columns"""
        return ' AND '.join(self.get_merge_condition())
    

    def merge(self, source_name: str, target_name: str) -> dict: 
        """Execute the merge."""
        merge_statement = self.get_merge_ddl(source_name, target_name)
        merge_result = spark.sql(merge_statement)
        return merge_result.toPandas().head(1).to_dict()


    def get_merge_ddl(self, source_name: str, target_name: str) -> str:
        """Get the merge query as a string for a sql statement join query will be on the etl fprint."""
        query = f'''
            MERGE INTO {target_name} AS tgt
            USING {source_name} AS src
            ON
                {self.get_merge_condition_ddl()}
            WHEN MATCHED AND NOT tgt.__etl_row_fprint = src.__etl_row_fprint THEN
                UPDATE SET
                    {self.get_update_set_ddl('src.', 'tgt.')}
            WHEN NOT MATCHED THEN
                INSERT (
                    -- Keys 
                    {self.get_skey_column_ddl() + ', ' if self._metadata.get('destination').get('skey') else '' }
                    {self.get_key_columns_ddl()},
                    -- Payload
                    {self.get_payload_columns_ddl()},
                    -- Metadata
                    {self.get_etl_columns_ddl()}
                )
                VALUES (
                    -- Keys 
                    {self.get_skey_column_ddl('src.') + ', ' if self._metadata.get('destination').get('skey') else '' }
                    {self.get_key_columns_ddl('src.')},
                    -- Payload
                    {self.get_payload_columns_ddl('src.')},
                    -- Metadata
                    {self.get_etl_columns_ddl('src.')}
                )
        '''

        return query
    

    def write(self, dataframe: DataFrame, target_name: str) -> dict:
        """Write the data to the target based on the operation in the metadata."""
        dataframe.createOrReplaceTempView('_final')
        if self._metadata['destination'].get('operation') == 'merge':
            return self.merge('_final', target_name)
        elif self._metadata['destination'].get('operation') == 'insert_overwrite':
            return self.insert_overwrite('_final', target_name)
        else:
            raise KeyError(f"Missing or Unsupported write operation in metadata: {self._metadata.get('operation')}")


    def get_write_ddl(self, source_name: str, target_name: str) -> dict:
        """Write the data to the target based on the operation in the metadata."""
        if self._metadata['destination'].get('operation') == 'merge':
            return self.get_merge_ddl(source_name, target_name)
        elif self._metadata['destination'].get('operation') == 'insert_overwrite':
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
            INSERT OVERWRITE {target_name}
            BY NAME
            SELECT
                -- Keys
                {self.get_skey_column_ddl('src.') + ',' if self._metadata.get('destination').get('skey') else ''}
                {self.get_key_columns_ddl('src.') + ',' if self.get_key_columns_ddl() != '' else ''}
                -- Payload
                {self.get_payload_columns_ddl('src.')},
                -- Metadata
                {self.get_etl_columns_ddl('src.')}
            FROM {source_name} AS src
        '''

        return query
    

    def get_skey_hash_ddl(self, prefix: str='') -> str: 
        """Get the skey hash ddl as a string for a sql statement."""
        if self._table_type == 'fact':
            return  hash_ddl(self.get_payload_columns(prefix=prefix)) + f' AS {self._metadata["destination"].get("skey")["name"]}'
        else:
            return hash_ddl(self.get_skey_columns(prefix=prefix)) + f' AS {self._metadata["destination"].get("skey")["name"]}'
    

    def get_key_fprint_hash_ddl(self, prefix: str='') -> str: 
        """Get the fprint ddl as a string for a sql statement."""
        return hash_ddl(self.get_key_columns(prefix=prefix)) + ' AS __etl_keys_fprint'
    

    def get_row_fprint_hash_ddl(self, prefix: str='') -> str: 
        """Get the fprint ddl as a string for a sql statement."""
        if self._table_type == 'fact':
            return hash_ddl(self.get_payload_columns(prefix=prefix)) + ' AS __etl_fprint'
        else:
            return hash_ddl(self.get_payload_columns(prefix=prefix)) + ' AS __etl_row_fprint'

    @classmethod
    def get_fkey_ddl(cls, columns: list[str], surrogate_null: str='', ghost_skey: int=-1, surrogate_nulls: dict={}, sort_columns:bool=True) -> str:
        """
        Generates the DDL expression for computing the skey of a FK using xxhash64.

        NOTE: The skeys on dimensions will always have the columns ordered. However, if 
        you pass in column names that are different to what is on the fact these could be sorted differently.
        If that is the case then set sort_columns to False and ensure the columns are ordered correctly.

        Arguments: 
            columns - The list of columns to hash
            surrogate_null - The expected surrogate null value of the columns as a string 
            ghost_skey - The value to return if the surrogate null is found
            surrogate_nulls - A dictionary of column names and their expected surrogate nulls. 
                This will take priority over the surrogate_null if a value is provided for a column
            sort_columns - Whether to sort the columns before hashing them

        Returns: 
            A SQL if statement that will check for surrogate nulls in the columns and return the hash of the columns if none found otherwise -1
        """
        hash_ddl_str = hash_ddl(columns, sort_columns)
        if_statement = ' OR '.join([f"{c} = '{surrogate_nulls.get(c) or surrogate_null}'" for c in columns])

        return f'IF({if_statement}, {ghost_skey}, {hash_ddl_str})'
    

    def add_etl_fields(self, dataframe: DataFrame) -> DataFrame: 
        """Add the etl fields to a dataframe"""
        dataframe.createOrReplaceTempView('transformed')
        return spark.sql(self.get_etl_fields_ddl('transformed'))
    

    def get_etl_fields_ddl(self, temp_view: str='transformed') -> str: 
        """Apply required hashing"""
        if self._table_type == 'dim':
            return f'''
                SELECT 
                    {self.get_skey_hash_ddl('t.') + ',' if self._metadata.get('destination').get('skey') else '' } 
                    t.*
                    ,{self.get_key_fprint_hash_ddl('t.') + ','}
                    {self.get_row_fprint_hash_ddl('t.')}
                    ,current_timestamp() AS __etl_effective_from
                    ,NULL                AS __etl_effective_to
                    ,TRUE                AS __etl_is_active
                    ,FALSE               AS __etl_is_deleted
                FROM
                    {temp_view} AS t
            '''
        else:
            return f'''
                SELECT
                    {self.get_skey_hash_ddl('t.') + ',' if self._metadata.get('destination').get('skey') else '' }
                    t.*
                    ,{self.get_row_fprint_hash_ddl('t.')}
                    ,current_timestamp() AS __etl_load_timestamp
                    ,TRUE                AS __etl_is_active
                    ,FALSE               AS __etl_is_deleted
                FROM
                    {temp_view} AS t
            '''
    

    def add_ghost_record(self, dataframe: DataFrame) -> DataFrame: 
        """Add the etl fields to a dataframe"""
        dataframe.createOrReplaceTempView('final')
        return spark.sql(self.get_ghost_record_ddl('final'))


    def get_ghost_record_ddl(self, temp_view: str='final') -> str: 
        """Get the DDL to union the ghost record on. This will include metadata fields."""
        return f'''
                SELECT 
                    {self.get_skey_column_ddl('f.') + ', ' if self._metadata.get('destination').get('skey') else '' }
                    {self.get_key_columns_ddl('f.')},
                    {self.get_payload_columns_ddl('f.')},
                    {self.get_etl_columns_ddl('f.')}
                FROM {temp_view} AS f
            UNION ALL
                SELECT 
                    {self.get_skey_column_ddl('g.') + ', ' if self._metadata.get('destination').get('skey') else '' }
                    {self.get_key_columns_ddl('g.')},
                    {self.get_payload_columns_ddl('g.')},
                    {self.get_etl_columns_ddl('g.')}
                FROM {self.ghost.temp_view} AS g
        '''

    def process_dim_transformation_query(self, query: str, add_ghost_record: bool=True) -> dict: 
        """Process a dimension transformation query"""
        process_query = f'''
            -- Transformation 
            WITH transformed AS (
                {query}
            )

            -- Add metadata
            , with_metadata AS (
                {self.get_etl_fields_ddl('transformed')}
            )

            -- Add ghost record if required
            , final AS (
                {self.get_ghost_record_ddl('with_metadata') if add_ghost_record else 'SELECT * FROM with_metadata'}
            )

            -- Write to target
            {self.get_write_ddl('final', self.dest_3partname(True))}
        '''

        try:
            result = spark.sql(process_query)
            result_dictionary = result.toPandas().head(1).to_dict()
        except Exception as err: 
            raise Exception(f'Failed to process transformation query: {process_query}')

        return result_dictionary
    