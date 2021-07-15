import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "target-dataset", table_name = "product", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "target-dataset", table_name = "product", transformation_ctx = "DataSource0")
## @type: RenameField
## @args: [old_name = "col0", new_name = "ACCESS_DATE", transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = RenameField.apply(frame = DataSource0, old_name = "col0", new_name = "ACCESS_DATE", transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("ACCESS_DATE", "long", "index", "int"), ("col1", "string", "ACCESS_DATE", "date"), ("col2", "string", "TCIN ", "long"), ("col3", "string", "PRODUCT_CATEGORY", "string")], transformation_ctx = "Transform9"]
## @return: Transform9
## @inputs: [frame = Transform1]
Transform9 = ApplyMapping.apply(frame = Transform1, mappings = [("ACCESS_DATE", "long", "index", "int"), ("col1", "string", "ACCESS_DATE", "date"), ("col2", "string", "TCIN ", "long"), ("col3", "string", "PRODUCT_CATEGORY", "string")], transformation_ctx = "Transform9")
## @type: DataSource
## @args: [database = "target-dataset", table_name = "online_top", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "target-dataset", table_name = "online_top", transformation_ctx = "DataSource2")
## @type: ApplyMapping
## @args: [mappings = [("col0", "string", "(right) col0", "string"), ("col1", "long", "(right) col1", "long"), ("col2", "long", "(right) col2", "long"), ("col3", "long", "(right) col3", "long"), ("col4", "double", "(right) col4", "double"), ("col5", "long", "(right) col5", "long")], transformation_ctx = "Transform14"]
## @return: Transform14
## @inputs: [frame = DataSource2]
Transform14 = ApplyMapping.apply(frame = DataSource2, mappings = [("col0", "string", "(right) col0", "string"), ("col1", "long", "(right) col1", "long"), ("col2", "long", "(right) col2", "long"), ("col3", "long", "(right) col3", "long"), ("col4", "double", "(right) col4", "double"), ("col5", "long", "(right) col5", "long")], transformation_ctx = "Transform14")
## @type: RenameField
## @args: [old_name = "col0", new_name = "date", transformation_ctx = "Transform13"]
## @return: Transform13
## @inputs: [frame = DataSource2]
Transform13 = RenameField.apply(frame = DataSource2, old_name = "col0", new_name = "date", transformation_ctx = "Transform13")
## @type: RenameField
## @args: [old_name = "col1", new_name = "TCIN", transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [frame = Transform13]
Transform7 = RenameField.apply(frame = Transform13, old_name = "col1", new_name = "TCIN", transformation_ctx = "Transform7")
## @type: RenameField
## @args: [old_name = "col2", new_name = "unitssold", transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [frame = Transform7]
Transform8 = RenameField.apply(frame = Transform7, old_name = "col2", new_name = "unitssold", transformation_ctx = "Transform8")
## @type: RenameField
## @args: [old_name = "col3", new_name = "unitsrestocked", transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame = Transform8]
Transform3 = RenameField.apply(frame = Transform8, old_name = "col3", new_name = "unitsrestocked", transformation_ctx = "Transform3")
## @type: RenameField
## @args: [old_name = "col4", new_name = "availiblequantity", transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [frame = Transform3]
Transform6 = RenameField.apply(frame = Transform3, old_name = "col4", new_name = "availiblequantity", transformation_ctx = "Transform6")
## @type: RenameField
## @args: [old_name = "col5", new_name = "currentprice", transformation_ctx = "Transform11"]
## @return: Transform11
## @inputs: [frame = Transform6]
Transform11 = RenameField.apply(frame = Transform6, old_name = "col5", new_name = "currentprice", transformation_ctx = "Transform11")
## @type: ApplyMapping
## @args: [mappings = [("date", "string", "date", "string"), ("TCIN", "long", "TCIN", "long"), ("unitssold", "long", "unitssold", "long"), ("unitsrestocked", "long", "unitsrestocked", "long"), ("availiblequantity", "double", "availiblequantity", "double"), ("currentprice", "long", "currentprice", "long")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = Transform11]
Transform4 = ApplyMapping.apply(frame = Transform11, mappings = [("date", "string", "date", "string"), ("TCIN", "long", "TCIN", "long"), ("unitssold", "long", "unitssold", "long"), ("unitsrestocked", "long", "unitsrestocked", "long"), ("availiblequantity", "double", "availiblequantity", "double"), ("currentprice", "long", "currentprice", "long")], transformation_ctx = "Transform4")
## @type: Join
## @args: [keys2 = ["TCIN"], keys1 = ["TCIN "], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame1 = Transform9, frame2 = Transform4]
Transform0 = Join.apply(frame1 = Transform9, frame2 = Transform4, keys2 = ["TCIN"], keys1 = ["TCIN "], transformation_ctx = "Transform0")
## @type: Filter
## @args: [f = lambda row : (bool(re.match("Shampoos and Conditioners", row["PRODUCT_CATEGORY"]))), transformation_ctx = "Transform12"]
## @return: Transform12
## @inputs: [frame = Transform0]
Transform12 = Filter.apply(frame = Transform0, f = lambda row : (bool(re.match("Shampoos and Conditioners", row["PRODUCT_CATEGORY"]))), transformation_ctx = "Transform12")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://class-project-target/glue-target/", "partitionKeys": []}, transformation_ctx = "DataSink1"]
## @return: DataSink1
## @inputs: [frame = Transform12]
DataSink1 = glueContext.write_dynamic_frame.from_options(frame = Transform12, connection_type = "s3", format = "csv", connection_options = {"path": "s3://class-project-target/glue-target/", "partitionKeys": []}, transformation_ctx = "DataSink1")
## @type: DataSource
## @args: [database = "target-dataset", table_name = "store_sales", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "target-dataset", table_name = "store_sales", transformation_ctx = "DataSource1")
## @type: RenameField
## @args: [old_name = "'productid'", new_name = "TCIN", transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = DataSource1]
Transform5 = RenameField.apply(frame = DataSource1, old_name = "'productid'", new_name = "TCIN", transformation_ctx = "Transform5")
## @type: ApplyMapping
## @args: [mappings = [("'date'", "string", "date", "string"), ("TCIN", "string", "TCIN", "long"), ("'locationid'", "string", "locationid", "string"), ("'unitssold'", "long", "unitssold", "long"), ("'availablequantity'", "long", "availablequantity", "long"), ("'currentprice'", "double", "currentprice", "double"), ("'unitsrestocked'", "long", "unitsrestocked", "long")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform5]
Transform2 = ApplyMapping.apply(frame = Transform5, mappings = [("'date'", "string", "date", "string"), ("TCIN", "string", "TCIN", "long"), ("'locationid'", "string", "locationid", "string"), ("'unitssold'", "long", "unitssold", "long"), ("'availablequantity'", "long", "availablequantity", "long"), ("'currentprice'", "double", "currentprice", "double"), ("'unitsrestocked'", "long", "unitsrestocked", "long")], transformation_ctx = "Transform2")
## @type: Join
## @args: [keys2 = ["TCIN "], keys1 = ["TCIN"], transformation_ctx = "Transform15"]
## @return: Transform15
## @inputs: [frame1 = Transform2, frame2 = Transform9]
Transform15 = Join.apply(frame1 = Transform2, frame2 = Transform9, keys2 = ["TCIN "], keys1 = ["TCIN"], transformation_ctx = "Transform15")
## @type: Filter
## @args: [f = lambda row : (bool(re.match("Shampoos and Conditioners", row["PRODUCT_CATEGORY"]))), transformation_ctx = "Transform10"]
## @return: Transform10
## @inputs: [frame = Transform15]
Transform10 = Filter.apply(frame = Transform15, f = lambda row : (bool(re.match("Shampoos and Conditioners", row["PRODUCT_CATEGORY"]))), transformation_ctx = "Transform10")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://class-project-target/glue-target/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform10]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform10, connection_type = "s3", format = "csv", connection_options = {"path": "s3://class-project-target/glue-target/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()