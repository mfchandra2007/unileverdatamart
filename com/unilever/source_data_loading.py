#Read data from MYSQL  - transactionsync,create a dataframe out of df
#Add a column 'ins_dt' -current_date()
#Write a dataframe in s3 partitioned by ins-dt

from pyspark.sql import SparkSession, functions
import os.path
import yaml
import utils.aws_utils as ut


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate() \

    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf["source_data_list"]

    for src in src_list:
        src_conf = app_conf[src]
        if src == "SB":
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                           "lowerBound": "1",
                           "upperBound": "100",
                           "dbtable": src_conf["mysql_conf"]["dbtable"],
                           "numPartitions": "2",
                           "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                           "user": app_secret["mysql_conf"]["username"],
                           "password": app_secret["mysql_conf"]["password"]
                          }

        txn_df = spark \
            .read.format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option(**jdbc_params) \
            .load() \
            .withColumn("ins_dt", functions.current_date())

        txn_df.show(5)
        txn_df.write \
            .mode("overwrite") \
            .partitionBy("INS_DT") \
            .option("header","true") \
            .option("delimiter", "~") \
            .csv("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/staging/SB")

        print("\n Write SB data in S3 << ")

        # spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/unilever/source_data_loading.py


