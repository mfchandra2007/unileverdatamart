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
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf["source_data_list"]
    print("app_secret:" + str(app_secret["mysql_conf"]["hostname"]))
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
            print("\n jdbc_params :" + jdbc_params["url"])
            txn_df = spark \
                .read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .options(**jdbc_params) \
                .load() \
                .withColumn("ins_dt", functions.current_date())

            txn_df.show(5)
            txn_df.write \
                .mode("overwrite") \
                .partitionBy("INS_DT") \
                .option("header","true") \
                .option("delimiter", "~") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/SB")

            print("\n Write SB data in S3 << ")

        elif src == "OL":
            print("\nReading OL data from sftp >>")
            txn_df2 = spark.read.format("com.springml.spark.sftp") \
                .option("host", app_secret["sftp_conf"]["hostname"]) \
                .option("port", app_secret["sftp_conf"]["port"]) \
                .option("username", app_secret["sftp_conf"]["username"]) \
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .option("fileType", "csv") \
                .option("delimiter", "|") \
                .load(src_conf["sftp_conf"]["directory"] + src_conf["sftp_conf"]["filename"]) \
                .withColumn("ins_dt", functions.current_date())
            txn_df2.show(5)

            txn_df2.write \
                .mode('overwrite') \
                .partitionBy("INS_DT") \
                .option("header", "true") \
                .option("delimiter", "~") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/OL")

            print("\nWriting OL data to S3 <<")

        elif src == "CP":
            print("\nReading OL data from Aws S3 >>")
            txn_df3 = spark.read.csv("s3a://" + src_conf["s3_source_conf"]["s3_bucket"] + "/KC_Extract_1_20171009.csv")\
                      .option("header","true") \
                      .option("delimiter","|") \
                      .withColumn("ins_dt", functions.current_date())
            #                src_conf["s3_source_conf"]["sourcefile"]) \
            txn_df3.show(5)

            txn_df3.write \
                    .mode("overwrite") \
                    .partitionBy("INS_DT") \
                    .option("header","true") \
                    .option("delimiter", "~") \
                    .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/CP")

            print("\n Writing CP data to S3 <<")

        # spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/unilever/source_data_loading.py
        # spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/unilever/source_data_loading.py


