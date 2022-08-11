from pyspark.sql import *

def starter():
    spark = SparkSession.builder.master('local[1]').getOrCreate()
    return spark

def file_read(spark, path):
    json_file = spark.read.json(path)
    return json_file

def read_json(spark, format_, infershecma_, multiline_, data_):
    read_json_file = spark.read.format(format_).options(inferSchema=infershecma_, multiline= multiline_).load(data_)
    return  read_json_file


def printschema(file):
    return file.show() , file.printSchema()

#gives the count of features
def count_len(file_name):
    return print(f"number of rows {file_name.count()} and columns {len(file_name.columns)}")

def tempview(file_name, file):
    return file.createOrReplaceTempView("zip")

def write_json_file(file, format_, mode_, path_):
    return file.write.format(format_).mode(mode_).save(path=path_)
    