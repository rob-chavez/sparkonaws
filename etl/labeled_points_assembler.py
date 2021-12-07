from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

import argparse, pathlib

#PATHS
parser = argparse.ArgumentParser()
parser.add_argument("input_dir", help="Directory containg urls data fomratted as parquet")
parser.add_argument("output_dir", help="Directory to save LIBSVM")
args = parser.parse_args()
input_dir = args.input_dir
output_dir = args.output_dir

#START SPARK SESSION
spark = SparkSession.builder.master("local").appName("vector_assemebler").getOrCreate()

#LOAD DATA
data = spark.read.options(inferschema='true').format("parquet").load(input_dir)

#RDD TO LABELED POINT
data_rdd = data.rdd
labeledPoint = data_rdd.map(lambda r: LabeledPoint(r[-1],[ r["is_registered"], 
                                                           r["months_til_expiration"], 
                                                           r["url_length"], 
                                                           r["domain_length"], 
                                                           r["path_length"], 
                                                           r["dot_count"], 
                                                           r["slash_count"], 
                                                           r["is_https"]]))

#SAVE
MLUtils.saveAsLibSVMFile(labeledPoint, output_dir)

spark.stop()