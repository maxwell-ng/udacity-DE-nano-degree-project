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

# Script generated for node customer curated
customercurated_node1690982350374 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1690982350374",
)

# Script generated for node Step Trainer landing
StepTrainerlanding_node1691152666551 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerlanding_node1691152666551",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691153863705 = ApplyMapping.apply(
    frame=StepTrainerlanding_node1691152666551,
    mappings=[
        ("sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691153863705",
)

# Script generated for node Join
Join_node1691151047827 = Join.apply(
    frame1=customercurated_node1690982350374,
    frame2=RenamedkeysforJoin_node1691153863705,
    keys1=["timestamp"],
    keys2=["right_sensorreadingtime"],
    transformation_ctx="Join_node1691151047827",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-human-balance-dev-datalake/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(Join_node1691151047827)
job.commit()
