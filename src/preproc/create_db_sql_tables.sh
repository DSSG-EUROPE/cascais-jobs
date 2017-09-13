# not a working file - will make this script at end of project
#!/bin/bash

iconv -f ISO-8859-1 -t UTF-8 unl-sie-31.csv | tr -d "?" | tr -d "?" | tr "." " " |tr [:upper:] [:lower:] | tr ' ' '_' | tr '-' '_' |csvsql -i postgresql

# copy and past into DBeaver, and manually make changes (e.g., find DATE and replace with VARCHAR(32), change any weird characters)

# load data

iconv -f ISO-8859-1 -t UTF-8 unl-sie-31.csv | psql -c '\copy IEFP.cancellations from stdin with csv header;' -h db.dssg.io -U kyang -d postgres