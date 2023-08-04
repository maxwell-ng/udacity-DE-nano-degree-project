import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer trusted
customertrusted_node1690981857534 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1690981857534",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1690981879542 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1690981879542",
)

# Script generated for node Join
Join_node1691148936385 = Join.apply(
    frame1=customertrusted_node1690981857534,
    frame2=accelerometertrusted_node1690981879542,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1691148936385",
)

# Script generated for node SQL Query
SqlQuery1033 = """
select customername,email,phone,birthday,serialnumber,timestamp,sharewithresearchasofdate,x,y,z
from myDataSource
where sharewithresearchasofdate is not null;

"""
SQLQuery_node1691148044243 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1033,
    mapping={"myDataSource": Join_node1691148936385},
    transformation_ctx="SQLQuery_node1691148044243",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-human-balance-dev-datalake/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(SQLQuery_node1691148044243)
job.commit()
