import json
from json_file import *

file_path = r'C:\Users\sid\Downloads\code pil\DATA_JSON'

#start the spark session
spark = starter()

#give files for reading files
json_file = file_read(spark, file_path + './reading_file.json')

#printschema
printschema(json_file)

#writing json file 
write_json_file(file=json_file, format_="json", mode_="overwrite", path_="./output/json_file")



#create sql file
temporary = tempview("zip", json_file)

#sql 
zip = spark.sql("SELECT City,Country,State from zip")

#zip
printschema(zip)

#count_len(zip)

user_file = read_json(spark, format_="json",
 infershecma_="True",  
 multiline_= "True", 
 data_= 'user_file.json')



printschema(user_file)

count_len(user_file)

print('writing multiline json file')


#writing json file 
write_json_file(file=user_file, format_="json", mode_="overwrite", path_="./output/user_file")


