from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col
import logging
from pyhive import hive
import os
import psycopg2
import boto3

'''conf = SparkConf().setAppName("spark_redshift_app") \
                  .setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
'''

logger = logging.getLogger(__name__)
logging.basicConfig(filename='spark-events/Sparktest.log', format='%(asctime)s:%(message)s', level=logging.INFO)

pyspark_log = logging.getLogger('spark')
pyspark_log.setLevel(logging.ERROR)

#get spark session
spark = SparkSession.builder \
    .appName("Redshift Connection with PySpark") \
    .config("spark.jars", "/opt/spark/jars/redshift-jdbc42-2.1.0.26.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.binaryAsString","true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .getOrCreate()

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
host=os.environ["DBT_HOST"]
port=os.environ["DBT_PORT"]
database=os.environ["DBT_DB"]

# add spark context config
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)

#define redshift connection params
redshift_url = f"jdbc:redshift://{host}:{port}/{database}"

redshift_properties = {
    "user": os.environ["DBT_USER"],
    "password": os.environ["DBT_PASSWORD"],
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "tablename": "dbt_test.stvterm",
    #"iam_role": os.environ["AW_IAM_ROLE"],
    "tmpdir": "s3a://"+ os.environ["AWS_S3_bucket"],
    "s3_bucket": os.environ["AWS_S3_bucket"],
}

tablename = redshift_properties["tablename"]

logger.info(f"Starting read table: {tablename}")

#read a table
df = spark.read \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_properties["tablename"]) \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .option("tempdir", redshift_properties["tmpdir"]) \
    .load()

logger.info(f"Ended read table: {tablename}")

tgttable = redshift_properties["tablename"] +'_tgt'
print("Emptying s3 bucket")
s3_bucket = redshift_properties["s3_bucket"]
s3_key = 'spark_parquet'
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
bucket = s3.Bucket(s3_bucket)
bucket.object_versions.filter(Prefix=f"{s3_key}/").delete()
print("s3 bucket removed")

logger.info(f"Starting write parquet: {tgttable}")
read_path = "s3://"+f"{s3_bucket}"+f"/{s3_key}"+f"/{tgttable}"
write_path = "s3a://"+f"{s3_bucket}"+f"/{s3_key}"+f"/{tgttable}"

'''
def spark_dtypes(typels):
    return typels[0], typels[1]

df_dtypes = df.dtypes
df_dtypes_ls = list(map(spark_dtypes, df_dtypes))
print(df_dtypes_ls)

for colname, coltype in df_dtypes_ls:
    df = df.withColumn(colname, col(colname).cast(coltype))
'''

df.write \
    .format("parquet") \
    .mode("append") \
    .save(write_path) \
#.option("aws_iam_role", redshift_properties["iam_role"]) \
logger.info(f"Ending write parquet: {tgttable}")


truncate_command = f"truncate table {tgttable} ;"
conn = psycopg2.connect(dbname=os.environ["DBT_DB"], user=redshift_properties["user"], password=redshift_properties["password"], host=os.environ["DBT_HOST"], port=os.environ["DBT_PORT"])
cur = conn.cursor()
print(truncate_command)

cur.execute(truncate_command)
conn.commit()

logger.info(f"Starting write table: {tgttable}")
#write the df to a table
copy_command = f"COPY {tgttable} from '" + f"{read_path}" + f"' CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}' FORMAT AS PARQUET"

conn = psycopg2.connect(dbname=os.environ["DBT_DB"], user=redshift_properties["user"], password=redshift_properties["password"], host=os.environ["DBT_HOST"], port=os.environ["DBT_PORT"])
cur = conn.cursor()
cur.execute(copy_command)
conn.commit()
logger.info(f"Ended write table: {tgttable}")

thrift_host = os.environ["THRIFT_URL"]
thrift_port = os.environ["THRIFT_PORT"]
thrift_user = os.environ["THRIFT_USER"]

conn = hive.Connection(host=thrift_host, port=thrift_port,username =thrift_user)
cursor = conn.cursor()
cursor.execute("Select count(*) from kv2")
row = cursor.fetchone()
print("Count = " + str(row[0]))

#df.write.csv("/opt/spark/spark-events/file.csv")

#write as parquet
'''
df.write.parquet("/opt/spark/spark-events/file.parquet")

parquetFile = spark.read.parquet("/opt/spark/spark-events/file.parquet")
parquetFile.createOrReplaceTempView("parquetFile")

tmpview = spark.sql("SELECT * FROM parquetFile WHERE stvterm_code='202320'")
tmpview.show()
'''