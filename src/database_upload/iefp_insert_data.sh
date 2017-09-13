#!/bin/bash

display_usage()
{
echo -e "Usage: $0 <POSTGRES_USERNAME> <CSV_FOLDER_PATH>\n"
}

echo "number of arguments: " $#

if [ $# -lt 2 ];
then
	display_usage
	exit 1
fi

username=${1}
folder_path=${2}


IEFP_DATA_SCHEMA="iefp"


#Run data schema creation script
psql -h db.dssg.io -U "$username" -d iefp -f iefp_data_upload.sql

#Run model output schema creaton script
psql -h db.dssg.io -U "$username" -d iefp -f model_output_upload.sql  

#Copy tables data from csv files
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-11-2-version-Mov11.csv| psql -c '\copy '"$IEFP_DATA_SCHEMA"'.application from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-31-Mov31.csv | psql -c '\copy '"$IEFP_DATA_SCHEMA"'.cancellation from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-37-novoenvio_v2-Mov37.csv | psql -c '\copy '"$IEFP_DATA_SCHEMA"'.interview from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-35-novoenvio_v2-Mov35.csv | psql -c '\copy '"$IEFP_DATA_SCHEMA"'.intervention from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-38_v2-Mov38.csv | psql -c '\copy '"$IEFP_DATA_SCHEMA"'.convocation from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp
iconv -f ISO-8859-1 -t UTF-8 "$folder_path"/unl-sie-43_v2-Mov43.csv | psql -c '\copy '"$IEFP_DATA_SCHEMA"'.category_change from stdin with csv header;' -h db.dssg.io -U "$username" -d iefp

#Run Table Index Creation script
psql -h db.dssg.io -U "$username" -d iefp -f insert_iefp_tables_index.sql

#Create Movements table
python integrate_data.py "$IEFP_DATA_SCHEMA"

