import os
import pandas as pd
import sys

MIN_NUM_ARGS = 3

def printUsage():
    print "Usage: xlsx2csv.py <input-folderpath> <output-folderpath>" 

def xlsx2csv(input_dir,output_dir,file_path):
	input_file_path = os.path.join(input_dir,file_path)
	print "Converting file to CSV format:" , input_file_path
	excel_file = pd.ExcelFile(input_file_path,encoding='utf-8')
	sheet_names = excel_file.sheet_names
	print "This file has",len(sheet_names),"sheets."
	for i in range(len(sheet_names)):
		sheet_name = sheet_names[i].decode("utf-8")
		print "Converting sheet#",i,":",sheet_name
		sheet_df = excel_file.parse(sheet_name)
		if ('Unnamed: 0' in sheet_df.columns): #Removing index column
			sheet_df = sheet_df.drop('Unnamed: 0',1)
		output_file_path = os.path.join(output_dir,file_path).replace('.xlsx','-' + sheet_name + '.csv').replace(' ','')
		sheet_df.to_csv(output_file_path,encoding='utf-8',index=False)

if len(sys.argv) < MIN_NUM_ARGS: 
    print "Wrong Usage!"
    printUsage()
    exit(1)


input_directory = sys.argv[1]
output_directory = sys.argv[2]
for file in os.listdir(input_directory):
	if file.endswith('.xlsx'):
		xlsx2csv(input_directory,output_directory,file)
