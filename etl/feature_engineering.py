#PACKAGE IMPORTS
import re, argparse
from urllib. parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

#FILENAMES
CSV_FILENAME = "whoisdata.csv"
PARQUET_FILENAME = "urls.parquet"

#START SPARK SESSION
spark = SparkSession.builder.master("local").appName("feature_engineering").getOrCreate()

#PATHS
parser = argparse.ArgumentParser()
parser.add_argument("input_csv", help="Directory containing whois data formatted as csv")
parser.add_argument("input_parquet", help="Directory containg urls data fomratted as parquet")
parser.add_argument("output_file", help="Output file name")
args = parser.parse_args()
input_csv = args.input_csv
input_parquet = args.input_parquet
output_file = args.output_file
csv_file_path = input_csv + "/" + CSV_FILENAME
parquet_file_path = input_parquet + "/" + PARQUET_FILENAME


#READ DATA
df = spark.read.options(inferschema='true').option("header", "True").format("csv").load(csv_file_path)
df2 = spark.read.options(inferschema='true').format("parquet").load(parquet_file_path)
df2 = df2.withColumnRenamed("url","url2")

#DROP DUPLICATE DATA
df = df.dropDuplicates(['url'])
df2 = df2.dropDuplicates(['url2'])

#TEMP VIEW
df.createOrReplaceTempView("t1")
df2.createOrReplaceTempView("t2")

#JOIN TABLES
df3 = spark.sql("SELECT * FROM t1 JOIN t2 ON t1.url = t2.url2")

#DROP DUPLICATE COLUMN
df3 = df3.dropDuplicates(['url', 'url2'])
df3 = df3.drop("url2")

#REFORMAT LABEL; GOOD=0, BAD=1
def relabel(v):
    if (v=='good'): return 0
    if (v=='bad'): return 1
reLabel = F.udf(lambda x: relabel(x), IntegerType())

##############   FEATURE ENGINEERING ##############

#NUMBER OF "/"s IN THE URL
def slash_count(col):
    fixed = len('\\')
    try:
        count = col.count("/")
        return count
    except:
        return 0
slashCount = F.udf(lambda x: slash_count(x), IntegerType())

#DOMAIN LENGTH
def domain_length(col):
    try:
        domain = urlparse(col).netloc
        return len(domain)
    except:
        return 0
domainLength = F.udf(lambda x: domain_length(x), IntegerType())

#PATH LENGTH
def path_length(col):
    try:
        path = urlparse(col).path
        return len(path)
    except:
        return 0
pathLength = F.udf(lambda x: path_length(x), IntegerType())


#NUMBER OF "."s IN THE URL
def dot_count(col):
    try:
        count = col.count(".")
        return count
    except:
        return 0
dotCount = F.udf(lambda x: dot_count(x), IntegerType())


#HTTPS PRESENT
def is_https(col):
    if (re.search("https", col)):
        return 1
    else:
        return 0
isHttps = F.udf(lambda x: is_https(x), IntegerType())
######################################################################

df4 = df3.select("*",
                 F.length(F.col("url")).alias("url_length"),
                 domainLength(F.col("url")).alias("domain_length"),
                 pathLength(F.col("url")).alias("path_length"),
                 dotCount(F.col("url")).alias("dot_count"),
                 slashCount(F.col("url")).alias("slash_count"),
                 isHttps(F.col("url")).alias("is_https"),
                 reLabel(F.col("label")).alias("is_malicious"))

#DROP label, NOW SET AS is_malicious
df4 = df4.drop("label")

#SAVE SILVER LEVEL DATA
df4.write.format("parquet").save(output_file)

spark.stop()
