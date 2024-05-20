from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging
import os

'''conf = SparkConf().setAppName("spark_redshift_app") \
                  .setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
'''

logger = logging.getLogger(__name__)
logging.basicConfig(filename='Sparktest.log', format='%(asctime)s:%(message)s', level=logging.INFO)

pyspark_log = logging.getLogger('pyspark')
pyspark_log.setLevel(logging.ERROR)

#get spark session
spark = SparkSession.builder \
    .appName("Redshift Connection with PySpark") \
    .config("spark.jars", "/opt/spark/drivers/redshift-jdbc42-2.1.0.26.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .getOrCreate()

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)

#define redshift connection params
redshift_url = "jdbc:redshift://tamu-usv-rs-01.ckpwibmvkp9u.us-east-1.redshift.amazonaws.com:5439/tamu".format(
    host=os.environ["DBT_HOST"],
    port=os.environ["DBT_PORT"],
    database=os.environ["DBT_DB"]
)

redshift_properties = {
    "user": os.environ["DBT_USER"],
    "password": os.environ["DBT_PASSWORD"],
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "tablename": "dbt_test.stvterm",
    #"iam_role": os.environ["AW_IAM_ROLE"],
    "tmpdir": os.environ["AW_S3_bucket"]
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

'''
logger.info(f"Starting write table: {tgttable}")

#write the df to a table
df.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", tgttable) \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("tempdir", redshift_properties["tmpdir"]) \
        .option("batchsize", "1000") \
        .save()

logger.info(f"Ended write table: {tgttable}")
'''

logger.info(f"Starting write parquet: file.parquet")
write_path = redshift_properties["tmpdir"]+'/file.csv'

df.write \
    .format("csv") \
    .mode("append") \
    .save(write_path) \

#.option("aws_iam_role", redshift_properties["iam_role"]) \

logger.info(f"Ending write parquet: file.csv")

#df.write.csv("/opt/spark/spark-events/file.csv")

#write as parquet
'''
df.write.parquet("/opt/spark/spark-events/file.parquet")

parquetFile = spark.read.parquet("/opt/spark/spark-events/file.parquet")
parquetFile.createOrReplaceTempView("parquetFile")

tmpview = spark.sql("SELECT * FROM parquetFile WHERE stvterm_code='202320'")
tmpview.show()
'''