import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1690754077614 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-dev-datalake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1690754077614",
)

# Script generated for node customer trusted
customertrusted_node1690978814629 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1690978814629",
)

# Script generated for node Join
Join_node1690978118155 = Join.apply(
    frame1=accelerometerlanding_node1690754077614,
    frame2=customertrusted_node1690978814629,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690978118155",
)

# Script generated for node Select Fields
SelectFields_node1690981009625 = SelectFields.apply(
    frame=Join_node1690978118155,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="SelectFields_node1690981009625",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1690663719253 = glueContext.getSink(
    path="s3://stedi-human-balance-dev-datalake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometertrusted_node1690663719253",
)
accelerometertrusted_node1690663719253.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="accelerometer_trusted"
)
accelerometertrusted_node1690663719253.setFormat("json")
accelerometertrusted_node1690663719253.writeFrame(SelectFields_node1690981009625)
job.commit()
