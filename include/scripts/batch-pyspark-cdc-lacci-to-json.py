from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql import SparkSession
import argparse

parser_main_desc = '''
  This is a pyspark script to perform batch cdc lacci to json
'''
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter, 
    description=parser_main_desc
)
parser.add_argument("--prc-dt", required=True, type=str, dest="dt_id", 
                    help="""Expected dt_id argument""")
kwargs = vars(parser.parse_args())
dt_id = kwargs.get("dt_id", "")

spark = SparkSession.builder.master("yarn") \
    .appName("batch-produce-message-ldm-cdc-lacci-prc-dt={}".format(dt_id)) \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.executor.cores", "1") \
    .config("spark.io.compression.codec", "snappy") \
    .config("spark.driver.memory", "4G") \
    .config("spark.executor.memory", "4G") \
    .config('spark.yarn.dist.files', "/data/07/bigdatadev/devops/ldm-ndb-lacci-cdc/keytab/hdp-batch_user2.keytab") \
    .config('spark.yarn.keytab', "/data/07/bigdatadev/devops/ldm-ndb-lacci-cdc/keytab/hdp-batch_user2.keytab") \
    .config('spark.yarn.principal', 'hdp-batch_user2') \
    .config('spark.driver.extraJavaOptions', '-Djava.security.krb5.conf=/etc/krb5.conf') \
    .config('spark.executor.extraJavaOptions', '-Djava.security.krb5.conf=/etc/krb5.conf') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

svrurl = spark.conf.get("spark.yarn.historyServer.address")
appid = spark.conf.get("spark.app.id")
print("Spark application ID: " + appid)
print("Go to {}/history/{} for monitor this app on spark".format(svrurl, appid))
print("Processing dt_id: " + dt_id)

# Read directly from HDFS - bypass metastore cache entirely
df_all = spark.read.parquet(
    "hdfs://nameservice1-datalake/user/hive/warehouse/stg.db/ldm_cdc_lacci_com/"
)
print("Columns: " + str(df_all.columns))

# Get max dt_id from HDFS directly
max_dt = df_all.agg({"dt_id": "max"}).collect()[0][0]
print("Max dt_id from HDFS: " + str(max_dt))

# Handle missing is_sent column in older parquet files
if "is_sent" not in df_all.columns:
    print("is_sent column not found, adding as NULL")
    df_all = df_all.withColumn("is_sent", lit(None).cast("int"))

# Register as temp view so we can use it in INSERT OVERWRITE later
df_all.createOrReplaceTempView("lacci_direct")

# Filter unsent rows using DataFrame API
df = df_all.filter(
    (col("dt_id") == max_dt) &
    (coalesce(col("is_sent"), lit(0)) == 0)
).select("lac", "ci", "long", "lat", "rattype", "status",
         "region", "subdistrict", "district", "is_deleted", "dt_id")

df.cache()
row_count = df.count()
print("Row count: {}".format(row_count))
df.show(5)

if row_count > 0:
    # Write JSON to HDFS for Kafka producer
    df.write.mode("overwrite").json("/user/hdp-batch_user2/cdc_ldm_lacci")
    print("Written {} rows to /user/hdp-batch_user2/cdc_ldm_lacci".format(row_count))

    # Register partition in metastore first so INSERT OVERWRITE can target it
    spark.sql("ALTER TABLE stg.ldm_cdc_lacci_com ADD IF NOT EXISTS PARTITION (dt_id='{}')".format(max_dt))

    # Mark rows as sent using Spark SQL INSERT OVERWRITE
    spark.sql("""
        INSERT OVERWRITE TABLE stg.ldm_cdc_lacci_com PARTITION (dt_id='{max_dt}')
        SELECT
            lac,
            ci,
            `long`,
            lat,
            rattype,
            `status`,
            region,
            subdistrict,
            district,
            is_deleted,
            1 as is_sent
        FROM lacci_direct
        WHERE dt_id = '{max_dt}'
    """.format(max_dt=max_dt))
    print("Marked rows as sent for dt_id={}".format(max_dt))
else:
    print("No rows to write, skipping JSON output.")

df.unpersist()
spark.stop()