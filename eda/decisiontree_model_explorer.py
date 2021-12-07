from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession

import argparse, pathlib

#PATHS
parser = argparse.ArgumentParser()
parser.add_argument("input_dir", help="Directory containing LIBSVM")
parser.add_argument("output_dir", help="Directory to save model")
args = parser.parse_args()
input_dir = args.input_dir
output_dir = args.output_dir

#START SPARK SESSION
spark = SparkSession.builder.master("local").appName("model_explorer").getOrCreate()

# LOAD DATA
data = MLUtils.loadLibSVMFile(spark.sparkContext, input_dir)

# TRAIN/TEST DATA
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# TRAIN DECISION TREE MODEL
model = DecisionTree.trainClassifier(trainingData, numClasses=2, 
                                     impurity='gini', maxDepth=5, 
                                     categoricalFeaturesInfo={}, 
                                     maxBins=32)

# EVALUATE ON TEST DATA
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(
    lambda lp: lp[0] != lp[1]).count() / float(testData.count())

print('Test Error = ' + str(testErr))
print('Learned classification tree model:')
print(model.toDebugString())

# SAVE MODEL
model.save(spark.sparkContext, output_dir)

spark.stop()