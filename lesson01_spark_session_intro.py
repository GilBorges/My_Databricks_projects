# Databricks notebook source
# Lesson 1: Creating and Configuring a SparkSession
# Author: Your Name
# Description: This notebook introduces the SparkSession object in Databricks.

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
# The SparkSession is the entry point to Spark SQL and DataFrame API
spark = SparkSession.builder \
    .appName("Lesson01-SparkSessionIntro") \
    .getOrCreate()

# COMMAND ----------

# Checking Spark version
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# Creating a simple DataFrame from a Python list
data = [("Alice", 29), ("Bob", 35), ("Cathy", 23)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Showing the DataFrame
df.show()

# COMMAND ----------

# Stopping the SparkSession
# Good practice in scripts, not always needed in Databricks notebooks
spark.stop()
