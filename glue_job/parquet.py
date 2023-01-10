import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json, month, to_date, year
from pyspark.sql.types import MapType, StringType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket", "db", "raw_table", "repartitioned_table", "s3_path"],
)

bucket = args["bucket"]
s3_path = args["s3_path"]
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
df = datasource.toDF()

if len(df.columns) != 0:

    gluedf = DynamicFrame.fromDF(glueContext, "gluedf")

    sink = glueContext.getSink(
        connection_type="s3",
        path=f"s3://{bucket}{s3_path}",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=db, catalogTableName=repartitioned_table)
    sink.writeFrame(gluedf)
job.commit()
