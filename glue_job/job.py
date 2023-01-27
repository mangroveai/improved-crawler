import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket", "db", "raw_table", "repartitioned_table", "s3_path"],
)

bucket = args["bucket"]
db = args["db"]
raw_table = args["raw_table"]
repartitioned_table = args["repartitioned_table"]
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=db, table_name=raw_table, transformation_ctx="datasource"
)
# Your job here
job.commit()
