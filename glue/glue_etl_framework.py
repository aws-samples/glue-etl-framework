import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3
import yaml
import ast
from loguru import logger
import json
 
from urllib.parse import urlparse
 
# Global variables
ds_mapping = {}
s3_client = boto3.client('s3')
 
def set_global_variables(variables):
 
    if isinstance(variables, dict):
        variables_dict = variables
    elif isinstance(variables, str):
        variables_dict = ast.literal_eval(variables)
    else:
        message = f"ERROR : Global variables should be either dict or str : {variables_dict}"
        logger.ERROR(message)
        sys.exit(message)
 
    logger.info(f"INFO : Config Variables = {variables_dict}")
 
    for key in variables_dict.keys():
        globals()[key] = variables_dict[key]
 
def getConfValue(item, default = ""):
    if not item:
        item = ""
    if item == "" and default:
        item = default
 
    logger.info(f"INFO : getConfValue.item = {item}")
    #return ast.literal_eval(str(f"f'{item}'"))
     
    try:
        return item.format(**globals())
    except:
        return item
 
def str2int(s):
    try:
        i = int(s)
    except ValueError:
        i = 0
    return i
 
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
 
def create_dynamic_frame(table):
 
    # required config items
    table_name = getConfValue(table.get("name")).lower()
    table_source = getConfValue(table.get("source")).lower()
 
    if table_name == "":
        message = f"ERROR : Table name is missing"
        logger.ERROR(message)
        sys.exit(message)
         
 
    if table_source == "":       
        message = f"ERROR : Table source is missing : {table_name}"
        logger.ERROR(message)
        sys.exit(message)
 
    # If table source is S3
    if table_source == "s3":
        table_location = getConfValue(table.get("location"))
 
        if table_location == "":
            message = "ERROR : Table location is missing : {table_name}"
            logger.ERROR(message)
            sys.exit(message)
 
        # optional config items
        table_format = getConfValue(table.get("format"), "parquet")
        table_format_options = getConfValue(table.get("format_options"),{})
 
        dynFrm = glueContext.create_dynamic_frame.from_options(
            format_options= table_format_options,
            connection_type="s3",
            format=f"{table_format}",
            connection_options={
                "paths": [f"{table_location}"],
                "recurse": True,
            },
            transformation_ctx=f"dynFrm_{table_name}",
        )
 
    # If table source is GDC
    elif table_source == "gdc":
        database = getConfValue(table.get("database")).lower()
        if database == "":           
            message = f"ERROR : Table source is missing : {table_name}"
            logger.ERROR(message)
            sys.exit(message)
 
        dynFrm = glueContext.create_dynamic_frame.from_catalog(
            database = f"{database}",
            table_name = f"{table_name}",
            transformation_ctx = f"dynFrm_{table_name}",
        )
 
    # If table source is JDBC
    elif table_source == "jdbc":
        print("Placeholder for jdbc sources")
 
    else:
        message = f"ERROR : No valid data source is provided : {table_source}"
        logger.ERROR(message)
        sys.exit(message)
 
    # Add the dataframe to the mapping table
    if dynFrm:
        ds_mapping[f"{table_name}"] = dynFrm
    else:
        message = f"ERROR : dynFrm is not created!"
        logger.ERROR(message)
        sys.exit(message)
 
# Executer for only Spark-sql based jobs
def run_spark_sql_job():
    input_tables = config['input_tables']
    for input_table in input_tables:
        create_dynamic_frame(input_table)
 
    sql_query = config.get('sql')
     
    if (not sql_query) or (sql_query == ""):
        sql_query_file = getConfValue(config.get('sql_file'))
        if sql_query_file == "":
            message = "ERROR : SQL or SQL File should be provided in the config"
            logger.ERROR(message)
            sys.exit(message)
         
        # Load the SQL file
        sql_query_file_url = urlparse(sql_query_file, allow_fragments=False)
        sql_query_file_bucket = sql_query_file_url.netloc
        sql_query_file_filepath = sql_query_file_url.path.lstrip('/')
         
        response = s3_client.get_object(Bucket=sql_query_file_bucket, Key=sql_query_file_filepath)
         
        try:
            sql_query = response["Body"].read().decode("utf-8")
             
        except yaml.YAMLError as exc:
            message = f"ERROR : SQL File {sql_query_file} cannot be read"
            logger.ERROR(message)
            sys.exit(message)
     
     
    if (not sql_query) or (sql_query == ""):
        message = f"ERROR : SQL script cannot be found"
        logger.ERROR(message)
        sys.exit(message)
         
    # Convert multiline script to a single line
    sql_query = " ".join(sql_query.split())
     
    # Replace single quotes with double quotes
    sql_query = sql_query.replace("'","\"")
     
    # Evaluate the script to embed variables
    sql_query = getConfValue(sql_query)
     
    logger.info(f"INFO : SQL Statement : {sql_query}")
     
    ssq = sparkSqlQuery(
        glueContext,
        query=sql_query,
        mapping=ds_mapping,
        transformation_ctx="sql",
    )
 
 
    output_table_target = getConfValue(config.get("output_table").get("target")).lower()
    output_table_name = getConfValue((config.get("output_table").get("name"))).lower()
    output_table_partition_keys = getConfValue(config.get("output_table").get("partition_keys"))
    output_table_location = getConfValue((config.get("output_table").get("location")))
    output_table_format = getConfValue(config.get("output_table").get("format")).lower()
    output_table_refresh = getConfValue(config.get("output_table").get("refresh")).lower()
    output_table_repartition = str2int(getConfValue(config.get("output_table").get("repartition")))
    output_table_coalesce = str2int(getConfValue(config.get("output_table").get("coalesce")))
 
    if output_table_repartition > 0:
        ssq = ssq.repartition(output_table_repartition)
    elif output_table_coalesce > 0:
        ssq = ssq.coalesce(output_table_coalesce)
 
    if output_table_partition_keys:
        output_table_partition_keys = output_table_partition_keys.replace(" ", "").split(",")
    else:
        output_table_partition_keys = []
 
    if not output_table_format:
        output_table_format="glueparquet"
 
    if not output_table_refresh:
        output_table_refresh="full"
 
    if output_table_target == "s3":
 
        if output_table_refresh == "full":
            # Deletes files from the specified Amazon S3 path recursively.
            glueContext.purge_s3_path(output_table_location, options = {"retentionPeriod": 0}, transformation_ctx="purge_s3_path")
 
        # Write DynamicFrame to S3
        sink = glueContext.getSink(
            path = output_table_location,
            connection_type = "s3",
            partitionKeys = output_table_partition_keys,
            enableUpdateCatalog = True,
            transformation_ctx=f"sink_{output_table_name}",
        )
 
        sink.setFormat(output_table_format)
        sink.writeFrame(ssq)
 
    elif output_table_target == "gdc":
        output_table_database = getConfValue(config.get("output_table").get("database"))
        output_table_governed = getConfValue(config.get("output_table").get("governed"))
 
        if output_table_governed == "yes":
            transaction_id = glueContext.start_transaction(False)
 
        if output_table_refresh == "full":
            # Delete files from Amazon S3 for the specified catalog's database and table. This also deletes all existing table partitions from the catalog.
            glueContext.purge_table(
                output_table_database, output_table_name, options={"retentionPeriod": 0}, transformation_ctx="purge_table")
 
        getSinkParameters = {}
        getSinkParameters["connection_type"] = "s3"
        getSinkParameters["path"] = output_table_location
        getSinkParameters["enableUpdateCatalog"] = True
        getSinkParameters["updateBehavior"] = "UPDATE_IN_DATABASE"
        getSinkParameters["partitionKeys"] = output_table_partition_keys
 
        if output_table_governed == "yes":
            getSinkParameters["transactionId"] = transaction_id
            getSinkParameters["additional_options"] = {"callDeleteObjectsOnCancel":"true"}
 
        sink = glueContext.getSink(**getSinkParameters)
        sink.setFormat(output_table_format)
        sink.setCatalogInfo(catalogDatabase = output_table_database, catalogTableName = output_table_name)
 
        try:
            sink.writeFrame(ssq)
            if output_table_governed == "yes":
                glueContext.commit_transaction(transaction_id)
        except Exception:
            if output_table_governed == "yes":
                glueContext.cancel_transaction(transaction_id)
             
            messsage = "ERROR : writeFrame operation failed!"
            logger.ERROR(message)
            sys.exit(message)   
 
    else:
        message = f"ERROR : Not a valid output_table.target : {output_table_target}"
        logger.ERROR(message)
        sys.exit(message)
 
 
# Executer for Spark (Python) based jobs
def run_python_job():
    python_file = getConfValue(config.get("python_file"))
    if python_file == "":
        message = "ERROR : Python script file should be provided in the config"
        logger.ERROR(message)
        sys.exit(message)
 
    python_file_url = urlparse(python_file, allow_fragments=False)
    python_file_bucket = python_file_url.netloc
    python_file_filepath = python_file_url.path.lstrip('/')
 
    try:
        response = s3_client.get_object(Bucket=python_file_bucket, Key=python_file_filepath)
    except:
        message = f"ERROR : Python file cannot be found : {python_file}"
        logger.ERROR(message)
        sys.exit(message)
 
    try:
        python_script = response["Body"].read()
    except Exception as exc:
        message = f"ERROR : Error in Retriving Python Script = {exc}"
        logger.ERROR(message)
        sys.exit(message)
 
    try:
        exec(python_script)
    except Exception as exc:
        message = f"ERROR : Error in Executing Python Script = {exc}"
        logger.ERROR(message)
        sys.exit(message)
 
def log_serialize(record):
    """
    Custom Serialize subset of message record dictionary
    """
    #Prepare a subset of the elements required to log message record
    subset = {
        "timestamp": str(record["time"]),
        "epoch": record["time"].timestamp(),
        "level": record["level"].name,
        "file": record["file"].path,
        "function": record["function"],
        "line": record["line"],
        "module": record["module"],
        "name": record["name"],
        "message": record["message"],
        "job": record["extra"]
    }
    return json.dumps(subset)
 
def log_sink(message):
    """
    Custom loguru sink for subset of message record serialization
    """
    serialized = log_serialize(message.record)
    print(serialized)
 
def get_logger():
    """
    Gets loguru logger object with base setting
    """
    try:
        #log = loguru.logger
        log = logger
        log.remove()
        logger_config = dict()
 
        logger_config["serialize"] = True
        logger_config["sink"] = sys.stderr
        log.add(**logger_config)
 
        log.remove()
        log.add(log_sink)
     
    except Exception as exc:
        message = f"ERROR : Error in Executing Python Script = {exc}"
        logger.ERROR(message)
        sys.exit(message)
 
    return log
 
############## JOB STARTS HERE ##########################
# Retrive job args
args = getResolvedOptions(sys.argv, ["JOB_NAME", "pipeline", "config_file", "variables"])
pipeline_name = args.get("pipeline")
config_file = args.get("config_file")
job_variables = args.get("variables")
 
log = get_logger()
job_fields = {"pipeline": pipeline_name,"config_file":config_file}
log.configure(extra=job_fields)
logger.info(f"INFO : Pipeline {pipeline_name} started using config file = {config_file}")
 
 
# Load and parse the job config file
config_url = urlparse(config_file, allow_fragments=False)
config_bucket = config_url.netloc
config_filepath = config_url.path.lstrip('/')
 
try:
    response = s3_client.get_object(Bucket=config_bucket, Key=config_filepath)
except:
    message = f"Config file cannot be found : {config_file}"
    logger.ERROR(message)
    sys.exit(message)
try:
    config = yaml.safe_load(response["Body"])
except yaml.YAMLError as exc:
    message = f"Error = {exc}"
    logger.ERROR(message)
    sys.exit(message)
 
# Set custom Spark Configs
spark_configs = config.get("job").get("spark_config")
if spark_configs:
    conf=SparkConf()
    for spark_config in spark_configs:       
        for key, value in spark_config.items():
            conf.set(key, value)
            logger.info(f"INFO : Spark Config : {key} = {value}")
 
    sc = SparkContext.getOrCreate(conf)
else:
    sc = SparkContext()
 
glueContext = GlueContext(sc)
#logger = glueContext.get_logger()
 
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
 
 
config_variables = config.get("variables").get(f"{pipeline_name}")
if config_variables:
    set_global_variables(config_variables)
 
job_type = getConfValue(config.get("job").get("type"))
 
#Below 2 lines are for Loguru configuration
job_fields = {"type": job_type, "pipeline": pipeline_name, "config":config_file}
if job_type == "spark-sql":
    output_table = getConfValue((config.get("output_table").get("name"))).lower()
    job_fields["output"] = output_table
 
log.configure(extra=job_fields)
 
if job_type == "spark-sql":
    run_spark_sql_job()
 
elif job_type == "python-script":
    run_python_job()
 
else:
    message = "ERROR : Job Type has not been defined in the config file"
    logger.ERROR(message)
    sys.exit(message)
 
job.commit()