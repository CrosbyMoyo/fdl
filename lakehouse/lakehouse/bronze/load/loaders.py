# Databricks notebook source
#TODO: Should be separate files but leaving this in a single notebook to reduce %run overhead 

# COMMAND ----------

#TODO: Implement as ABC 
from pyspark.sql import DataFrame
class StreamTransformer:
    """
    Base class for all stream transformations.
    You can add helper methods to this that are called by the transform() method.
    """
    def __init__(self) -> None:
        # Anything added to the init will execute before the stream starts. 
        # Put expensive computations here if you can reduce the overhead of the stream once its running.
        pass  

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the input DataFrame and return the result.
        """
        raise NotImplementedError("Naughty! Subclasses must implement my transform method :(")


# COMMAND ----------

class ChangeDataCaptureTransformer(StreamTransformer):
    """
    Change Data Capture (CDC) Transformer 

    This extends the StreamTransformer base class to read from a Change Data Feed (CDF) stream and transform the result into change data capture (CDC).

    Requires:
        1. Read stream has been started with readChangeFeed = True
    """
    def __init__(self):
        self.record_action_column = '__etl_source_operation'
        self.timestamp_column = '__etl_bronze_timestamp'

        self.tmp_view_name = 'tmp_table_changes'
        self.query = None
        self.compile()

    def compile(self) -> None:
        """
        Compile and format the SQL that will be executed on the streaming query.
        """
        self.query = f"""
            SELECT
                tc.* EXCEPT(_commit_version, _change_type, _commit_timestamp)
                ,CASE 
                    WHEN _change_type = 'update_postimage' THEN 'UPDATE'
                    WHEN _change_type = 'insert' THEN 'INSERT'
                    WHEN _change_type = 'delete' THEN 'DELETE'
                END                AS {self.record_action_column}
                ,_commit_timestamp AS {self.timestamp_column}
            FROM
                {self.tmp_view_name} AS tc 
            WHERE 
                _change_type IN ('update_postimage', 'insert', 'delete');
        """

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the streaming query by executing the compiled query.
        """
        df.createOrReplaceTempView(self.tmp_view_name)
        return df.sparkSession.sql(self.query)

# COMMAND ----------

from pyspark.sql import DataFrame

#TODO: Implement as ABC 

class MicroBatchProcessor:
    """
    Base class for all foreachBatch processors.
    You can add helper methods to this that are called by the process() method.
    """
    def __init__(self) -> None:
        # Anything added to the init will execute before the stream starts. 
        # Put expensive computations here if you can reduce the overhead of the stream once its running.
        # The signature of process() cannot be changed.
        pass 

    def process(self, df: DataFrame, id: int) -> DataFrame:
        """
        Process a static batch of the stream. 
        """
        raise NotImplementedError("Naughty! Subclasses must implement my process method :(")


# COMMAND ----------

class SoftDeleteProcessor(MicroBatchProcessor):
    """
    Soft Delete Processor

    This extends the MicroBatchProcessor base class to read a table of change data capture (CDC) and consolidate to a table with soft deletes.

    Requires: 
        - Source table in CDC format
    """
    def __init__(self, source_table: str, target_table: str, primary_key: list[str]):
        self.target_table = target_table
        self.source_table = source_table
        self.primary_key = primary_key

        self.source_timestamp_col = '__etl_bronze_timestamp'
        self.source_record_action_col = '__etl_source_operation'

        self.target_is_deleted_col = '__etl_is_deleted'
        self.target_bronze_timestamp_col  = '__etl_bronze_timestamp'
        self.target_silver_timestamp_col = '__etl_silver_timestamp'

        self.source_columns = [
            col for col in spark.table(self.source_table).columns
            if col not in (self.source_timestamp_col, self.source_record_action_col)
        ]

        self.join_condition_ddl = ' AND '.join([f'tgt.{col} <=> src.{col}' for col in self.primary_key])
        self.insert_columns_ddl = ', '.join(self.source_columns)
        self.insert_values_ddl = ', '.join([f'src.{col}' for col in self.source_columns])
        self.update_set_ddl = ', '.join([f'tgt.{col} = src.{col}' for col in self.source_columns])

        self.tmp_view_name = 'tmp_view'
        self.query = None
        self.compile()

    # JN: is this a private method?  Do you subscribe to the idea of prefixing with an underscore? _compile?
    def compile(self) -> None:
        """
        Compile and format the SQL that will be executed on the static temp view.
        """
        self.query = f'''
            MERGE INTO {self.target_table} AS tgt 
            USING {self.tmp_view_name} AS src 
            ON
                {self.join_condition_ddl}
            WHEN MATCHED AND src.{self.source_record_action_col} = 'DELETE'
                THEN UPDATE SET 
                    -- Metadata
                    tgt.{self.target_bronze_timestamp_col} = src.{self.source_timestamp_col},
                    tgt.{self.target_silver_timestamp_col} = NULL,
                    tgt.{self.target_is_deleted_col} = True
            WHEN MATCHED AND src.{self.source_record_action_col} = 'UPDATE'
                THEN UPDATE SET 
                    {self.update_set_ddl},
                    -- Metadata
                    tgt.{self.target_bronze_timestamp_col} = src.{self.source_timestamp_col},
                    tgt.{self.target_silver_timestamp_col} = NULL, 
                    tgt.{self.target_is_deleted_col} = False
            WHEN NOT MATCHED
                THEN INSERT (
                    {self.insert_columns_ddl},
                    -- Metadata
                    {self.target_bronze_timestamp_col},
                    {self.target_silver_timestamp_col},
                    {self.target_is_deleted_col}
                ) VALUES (
                    {self.insert_values_ddl},
                    -- Metadata
                    src.{self.source_timestamp_col},
                    NULL,
                    False
                );
        '''

    def process(self, df: DataFrame, id: int=None) -> None:
        """
        Process a static batch of the stream.
        """
        df.createOrReplaceTempView(self.tmp_view_name)
        return df.sparkSession.sql(self.query)

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener
# TODO: Add this to the logger 

class ListeningTom(StreamingQueryListener):
    """
    Basic stream listener to log progress.
    Couple of good resources if you're looking to extend this:
        - https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring?language=Python
        - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/StreamingQueryListener.QueryStartedEvent.html
        - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/StreamingQueryProgress.html
        - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/StreamingQueryListener.QueryTerminatedEvent.html
    """
    def onQueryStarted(self, event: 'QueryStartedEvent'):
        """ 
        Query start event. 
        """
        print(f"Query started: {event.name or event.runId or event.id}")

    def onQueryProgress(self, event: 'QueryProgressEvent'):
        """
        On query progress.
        """
        print(f"Query made progress: {event.progress.numInputRows} rows processed at {event.progress.processedRowsPerSecond} rows per second")

    def onQueryTerminated(self, event):
        """
        On query termination.
        """
        if event.exception:
            print(f"Query terminated: {event.runId or event.id} with exception: [{event.exception}]: {event.errorClassOnException}")
        else:
            print(f"Query terminated: {event.runId or event.id}")

# COMMAND ----------

class StreamOrchestrator:
    """
    This is a generic class that helps orchestrate a structured stream between two delta tables.

    Parameters: 
        source_table: Full qualified table name of the source table
        target_table: Full qualified table name of the target table
        checkpoint_location: Checkpoint location for the streaming offsets. 
           For example "abfss://{container}@{storage}.dfs.core.windows.net/metadata/offsets/{target_table}"
        spark: The spark session context 
        trigger_type [Optional]: Trigger type for the stream. Default: {'availableNow': True}
        read_kwargs [Optional]: Read stream options. No additional read options are added by default
        write_kwargs [Optional]: Write stream options. Default: {'writeMode': 'append'}

    Requires:
        1. Target and source tables to exist
        2. Target table schema to match the output of your transformer

    Usage: 
        cdf_transformer = CDFTransformer()

        orchestrator = StreamOrchestrator(
            source_table="test_table",
            target_table="test_table_cdc",
            checkpoint_location="{path to your checkpoints ...}",
            spark=spark
        )

        orchestrator.add(cdf_transformer)
        orchestrator.run()
    """

    def __init__(
        self,
        source_table: str,
        target_table: str,
        checkpoint_location: str,
        spark,
        trigger_type: dict=None,
        read_kwargs: dict=None,
        write_kwargs: dict=None,
        query_name: str=None,
    ):
        self.source_table = source_table
        self.target_table = target_table
        self.checkpoint_location = checkpoint_location
        self.spark = spark

        self.trigger_type = trigger_type or {'availableNow': True}
        self.write_kwargs = write_kwargs or {'writeMode': 'append'}
        self.read_kwargs = read_kwargs or {}
        self.query_name = query_name

        self.transformers: list[StreamTransformer] = []

        self.read_stream = (
            self.spark.readStream
            .options(**self.read_kwargs)
            .table(self.source_table)
        )

    def add(self, transformer: StreamTransformer) -> 'StreamOrchestrator':
        """
        Add a StreamTransformer.
        This enables us to orchestrator.add(transformerA).add(transformerB).run()
        """
        self.transformers.append(transformer)
        return self

    def run(self, processor: MicroBatchProcessor=None) -> None:
        """Run the stream with the transformers"""
        # Apply transformers
        self.transformed_stream = self.read_stream
        for transformer in self.transformers:
            self.transformed_stream = transformer.transform(self.transformed_stream)

        streaming_query = (
            self.transformed_stream.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_location)
            .options(**self.write_kwargs)
            .trigger(**self.trigger_type)
        )

        if self.query_name:
            streaming_query = streaming_query.queryName(self.query_name)

        if processor:
            write_stream = streaming_query.foreachBatch(processor.process).start()
        else:
            write_stream = streaming_query.toTable(self.target_table)
        
        write_stream.awaitTermination()