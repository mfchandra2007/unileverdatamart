#Read data from MYSQL  - transactionsync,create a dataframe out of df
#Add a column 'ins_dt' -current_date()
#Write a dataframe in s3 partitioned by ins-dt

from pyspark.sql import SparkSession
from pyspark.sql import functions
import os.path
import yaml
import utils.aws_utils as ut


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
            .builder \
            .appName("Read Files") \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
            .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
            .config("spark.mongodb.output.uri", app_secret["mongodb_config"]["uri"])\
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    src_list = app_conf["source_data_list"]
    print("app_secret:" + str(app_secret["mysql_conf"]["hostname"]))
    for src in src_list:
        src_conf = app_conf[src]
        if src == "CP":
            print("\nReading CP data from Aws S3 >>")
            txn_df3 = spark.read.option("header","true") \
                      .option("delimiter","~") \
                      .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] +
                       "/staging/CP/INS_DT=2021-04-04/part-00000-c67d7351-8280-4bf3-a5c4-aa90f1f0feab.c000.csv") \

            txn_df3.show(5)



        # spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/unilever/target_data_loading.py

        # spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/unilever/source_data_loading.py

