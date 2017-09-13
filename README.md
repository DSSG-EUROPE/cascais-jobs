# cascais-jobs
Repo for the Cascais unemployment reduction team

## Pipeline Steps

### Preprocessing
  * Get data in raw xlsx files (with multiple sheets)
  * Convert xlsx files to csv (each sheet in a different csv file)
  * Fix errors in csv files
  * Select the csv files (sheets) to be used
```bash
cd <project-folder>/src/preproc/
./preproc-input-files.sh <INPUT-FOLDER> <PREPROC-OUTPUT-FOLDER>
```

### Database Setup
  * Create Postgres DB
  * Copy dbcreds example file
```bash
cp <project-folder>/utils/dbcreds.example <project-folder>/utils/dbcreds.py 
```
  * Update dbcreds example file with credentials for the created DB
  
### Data Upload
  * Create schema and setup DB
  * Copy tables data from csv files
  * Add table index to each table
  * Build user interactions journey from the database tables
```bash
cd <project-folder>/src/database_upload/
./iefp_insert_data.sh <POSTGRES_USERNAME> <PREPROC-OUTPUT-FOLDER>/selected-csvs
```

### Run Model Pipeline
  * Read applications and movements tables from DB
  * Split data into train/test sets based on application status on action date
  * Label train/test applications
  * Extend applications data to use month granularity
  * Generate features for train/test set  
  * Train model
  * Test model
  * Evaluate model performance at a given cutoff value
  * Save model config, feature importances and performance evaluation to DB
```bash
cd <project-folder>/src/model/
python run_pipeline.py <pipeline-config-filepath>
```
Notice: An example pipeline configuration file is in src/model folder. The simple_pipeline-config.example.json
shows the dynamic model, while the simple_applicationmodel-config.example.json shows the application time model 
## Optional

If there are new tables or new variables in the tables, a new DB schema SQL script must be created. Instructions below:

  * Build schema/tables creation statements from each csv file (follow example below)
```bash
iconv -f ISO-8859-1 -t UTF-8 unl-sie-31.csv | tr -d "?" | tr -d "?" | tr "." " " |tr [:upper:] [:lower:] | tr ' ' '_' | tr '-' '_' |csvsql -i postgresql
```
  * Copy and append to script, and manually make changes (e.g., find DATE and replace with VARCHAR(32), change any unicode characters)
