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

# Script generated for node Amazon S3
AmazonS3_node1690898209071 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-dev-datalake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1690898209071",
)

# Script generated for node SQL Query
SqlQuery660 = """
select customerName, email, phone, birthDay, serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate 
from myDataSource
where shareWithResearchAsOfDate is not null;
"""
SQLQuery_node1690975179749 = sparkSqlQuery(
    glueContext,
    query=SqlQuery660,
    mapping={"myDataSource": AmazonS3_node1690898209071},
    transformation_ctx="SQLQuery_node1690975179749",
)

# Script generated for node customer trusted
customertrusted_node3 = glueContext.getSink(
    path="s3://stedi-human-balance-dev-datalake/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customertrusted_node3",
)
customertrusted_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_trusted"
)
customertrusted_node3.setFormat("json")
customertrusted_node3.writeFrame(SQLQuery_node1690975179749)
job.commit()
