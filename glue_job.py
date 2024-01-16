try:
    import os, uuid, sys, boto3, time, sys, json
    from pyspark.sql.functions import lit, udf
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Modules are missing : {} ".format(e))


def resolve_args(args_list):
    print(">> Resolving Argument Variables: START")
    available_args_list = []
    for item in args_list:
        try:
            args = getResolvedOptions(
                sys.argv, [f'{item}']
            )
            available_args_list.append(item)
        except Exception as e:
            print(f"WARNING : Missing argument, {e}")
    print(f"AVAILABLE arguments : {available_args_list}")
    print(">> Resolving Argument Variables: COMPLETE")
    return available_args_list


args_list = [
    "QUEUE_URL",
    'JOB_NAME',
    'GLUE_DATABASE',
    'GLUE_TABLE_NAME',
    "HUDI_TABLE_TYPE",
    'HUDI_PRECOMB_KEY',
    'HUDI_RECORD_KEY',
    "ENABLE_CLEANER",
    'ENABLE_HIVE_SYNC',
    "ENABLE_PARTITION",
    'INDEX_TYPE',
    "PARTITON_FIELDS",
    "USE_SQL_TRANSFORMER",
    'SQL_TRANSFORMER_QUERY',
    'TARGET_S3_PATH',
    'HUDI_METHOD'
]
available_args_list = resolve_args(args_list)
args = getResolvedOptions(
    sys.argv, available_args_list
)
# Create a Spark session
spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


class Poller:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs'
                                       )
        self.batch_size = 10
        self.messages_to_delete = []

    def get_messages(self, batch_size):
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                self.messages_to_delete.append({
                    'ReceiptHandle': message['ReceiptHandle'],
                    'Body': message['Body']
                })
            return messages
        else:
            return []

    def commit(self):
        for message in self.messages_to_delete:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        self.messages_to_delete = []


def read_data_s3(path, format):
    if format == "parquet" or format == "json":
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format=format,
            connection_options={
                "paths": path,
                "recurse": True,
            },
            transformation_ctx="job_glue",
        )

        spark_df = glue_df.toDF()

        print(spark_df.show())

        return spark_df


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df,
                      enable_partition, enable_cleaner, enable_hive_sync, use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    print("##### HUDI SETTINGS #####")
    print({
        'glue_database': glue_database,
        'table_name': table_name,
        'record_id': record_id,
        'precomb_key': precomb_key,
        'table_type': table_type,
        'spark_df': spark_df,
        'enable_partition': enable_partition,
        'enable_cleaner': enable_cleaner,
        'enable_hive_sync': enable_hive_sync,
        'use_sql_transformer': use_sql_transformer,
        'sql_transformer_query': sql_transformer_query,
        'target_path': target_path,
        'index_type': index_type,
        'method': method,
    })
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": args['PARTITON_FIELDS'],
        "hoodie.datasource.hive_sync.partition_fields": args['PARTITON_FIELDS'],
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        current_columns = spark_df.columns
        new_columns = [c.replace("$", "") for c in current_columns]
        for i in range(len(current_columns)):
            spark_df = spark_df.withColumnRenamed(current_columns[i], new_columns[i])

        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)
            print("spark_df")
            print("***************")
            print(spark_df.show(100))

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)

def process_message(messages, file_format='json'):
    try:
        batch_files = []

        for message in messages:
            payload = json.loads(message['Body'])
            records = payload['Records']
            s3_files = [f"s3://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}" for record in records]
            for item in s3_files: batch_files.append(item)

        if batch_files != []:
            spark_df = read_data_s3(
                path=batch_files,
                format=file_format
            )
            print("**************")
            spark_df.show()
            print("**************")

            upsert_hudi_table(
                glue_database=args.get('GLUE_DATABASE', ''),
                table_name=args.get('GLUE_TABLE_NAME', ''),
                record_id=args.get('HUDI_RECORD_KEY', ''),
                precomb_key=args.get('HUDI_PRECOMB_KEY', ''),
                table_type=args.get('HUDI_TABLE_TYPE', 'COPY_ON_WRITE'),
                method=args.get('HUDI_METHOD', 'upsert'),
                index_type=args.get('INDEX_TYPE', 'BLOOM'),
                enable_partition=args.get('ENABLE_PARTITION', 'False'),
                enable_cleaner=args.get('ENABLE_CLEANER', 'False'),
                enable_hive_sync=args.get('ENABLE_HIVE_SYNC', 'True'),
                use_sql_transformer=args.get('USE_SQL_TRANSFORMER', 'False'),
                sql_transformer_query=args.get('SQL_TRANSFORMER_QUERY', 'default'),
                target_path=args.get('TARGET_S3_PATH', ''),
                spark_df=spark_df,
            )

    except Exception as e:
        print("Error processing message:", e)
        raise Exception("Error processing message:", e)


def main():
    queue_url = args.get('QUEUE_URL', '')

    poller = Poller(queue_url)

    while True:
        messages = poller.get_messages(poller.batch_size)
        if not messages:
            print("No messages to process. Exiting.")
            break
        else:
            process_message(messages)

        poller.commit()


if __name__ == "__main__":
    main()
