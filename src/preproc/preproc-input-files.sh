#!/bin/bash

display_usage()
{
	echo -e "Usage: $0 <INPUT_FOLDER> <OUTPUT_FOLDER>\n"
}

echo "number of arguments: " $#

if [ $# -lt 2 ];
then
	display_usage 
	exit 1
fi

input_folder=${1}						# Input folder containing xlsx input files
output_folder=${2}						# Output folder where output files will be stored

echo "-------------------------"
echo "Preprocessing files at: " $input_folder
echo "-------------------------"

raw_csvs_folder="$output_folder"/raw-csvs
selected_csvs_folder="$output_folder"/selected-csvs

mkdir -p "$raw_csvs_folder"
mkdir -p "$selected_csvs_folder"

time python xlsx2csv.py "$input_folder" "$raw_csvs_folder"

#Remove unl-pedido extra top rows
tail -n +2 "$raw_csvs_folder"/unl-Pedido-Inscritos_longo_01_07e04_17.csv > "$raw_csvs_folder"/unl-Pedido-Inscritos_longo_01_07e04_17_2.csv
mv "$raw_csvs_folder"/unl-Pedido-Inscritos_longo_01_07e04_17_2.csv "$raw_csvs_folder"/unl-Pedido-Inscritos_longo_01_07e04_17.csv

#Change ID column name to UTE-ID in Cancellation file
new_header="AnoMes,CTipo Movimento,DTipo_Movimento,Ute-ID,Anulacao Motivo,DMotivo Anula<C3><A7><C3><A3>o,Anulacao Data"
sed -i "1s/.*/$new_header/" "$raw_csvs_folder"/unl-sie-31-Mov31.csv

#Copy selected csv files to selected-csvs folder
cp "$raw_csvs_folder"/unl-Pedido-Inscritos_longo_01_07e04_17.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-31-Mov31.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-35-novoenvio_v2-Mov35.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-36_v2-Mov36.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-37-novoenvio_v2-Mov37.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-38_v2-Mov38.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-43_v2-Mov43.csv "$selected_csvs_folder"
cp "$raw_csvs_folder"/unl-sie-11-2-version-Mov11.csv "$selected_csvs_folder"
