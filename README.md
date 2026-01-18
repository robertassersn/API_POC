
### PREREQUISITES
- necessary python packages provided in requirements.txt file
- in order to use PARAMETERS, MAIN_CONFIG_FILE variable value needs to be filled in .env file
- airflow-docker/docker-compose.yml must provide within "x-airflow-common:" block following "volume"  "- ../:/opt/airflow/project"
  - following volume allows airflow to activate venv, install required packages, execute scripts   

### PROJECT NOTES
- To have functional reusability, project has "project_files" directory currently containing 2 python files that contain functions dedicated to specific area of work
  - functions.py contains generic functions that may be used both in ETL/pipeline layer as well as DWH Fact/Dim table modelling
  - parsing_functions.py contains functions related to data parsing topic
- Data is being loaded into Postgresql database

- parameters are listed in /parameters/job_parameters.conf , currently they have been adjusted to meet airflow logic
- virtual environment used by airflow is listed in /task_venv/ directory\
- database schemas in .sql files are parametrized for easier development environment.
- each job has "temp" and "main" schema. Loadings are initially done into temp schema tables and then if loadings are successful and sql verifications pass, data can be loaded into main schema tables, which can be used by analytics engineers.
- in order to monitor job runs from database side "job_runs" table was created, its granularity is based on job run, does not have individual steps tracking at the moment.
  - postgresql SEQUENCES are used to identify individual job runs
    - following sequences are inserted into tables during insert from "temp" schema into "main" schema   
  - runs can have following statuses [STARTED,FINISHED,FAILED]
- api returns some of key values as sql database keywords, decided to keep matching columns names to the json file, it creates requirement to add doublequotes when query'ing columns. 

### Model details

DIRECTORY:
created model, listed in STG/STG_GOOGLE_TRENDS/
- executed from STG/STG_GOOGLE_TRENDS/STG_GOOGLE_TRENDS.py
PREREQUISTES:
  - deployment script needs to be executed in order to successfully run the job
    - it can be found at: STG/STG_GOOGLE_TRENDS/deployment_scripts/STG_GOOGLE_TRENDS.sql
ALGORITHM:
- script uses serpapi library to retrieve google_trends results and saves the results in JSON file at provided Directory
- before parsing is began, script compares current json schema versus schema.json file located at STG/STG_GOOGLE_TRENDS/ directory
- if validation passes, parsing begins
  - due to structure of json, I've made a choice to normalize the data, meaning JSON keys that contain ARRAY objects are treated as separate "Tables" that have their own granularity
    - However key to parent table is provided in child tables
  - for traceability, source_file_name column is added to parquet files that lists json filename
- parsing functions yield 2 parquet files according to their granularity, search_event/search_event_value
- Files are initially sent temp schema's tables
- Downloaded and Parsed files are being added into separate .zip files and sent into Archived files directory
  - contents from directories where  
- sql verifications are executed, in case of failure, at the moment job is being terminated.
- if verifications pass, insert is being made from temp schema tables into main schema tables



### Logging 
created model creates .log file in  STG/STG_GOOGLE_TRENDS/logs/ during runtime in format STG_GOOGLE_TRENDS_YYYYMMDD_HHMMSS
example:
<img width="1374" height="439" alt="image" src="https://github.com/user-attachments/assets/e241541c-b6a2-4e4d-91cf-3c4455f7a155" />


### Areas for improvement

PERSONAL
- not everything is still parametrized
- current parquet parser, needs much improvement. It definately will not handle all possible schema types. I've personally spent more time parsing data into CSV due to business requirements. Which is fine if you have control over output file content and column positioning
- so far have been doing majority of testing inside of database rather than outside.
- Not using python's decorators, they can be very helpful in error/warning handling to reduce script length

  --------------------------------------------------------------------------------------------------------------
JOB
- current provided API solution can search 5 terms at a time, task requested for 4 terms, however job needs to be modified to handle larger lists of terms, possibly from 1 large list, generate string of 5 terms and iterate.
-  Can add functionality so send email or notification in case of ERROR or WARNING.
  - if such functionality is added, need to adjust function used for verifications to allow some verifications to be treated as Warning rather than all treated as ERRORS
- limit number of hardcodings in job, many of hardcodings can be passed as parameters
-  STG/STG_GOOGLE_TRENDS/STG_GOOGLE_TRENDS.py following file calls individual functions that are responsible for different tasks, such functions and their order can be controlled from database table.
   t would also be easier to enable/disable steps that way. In addition Script length would be reduced. 
-  Each function should be simple and do as little as possible, and should contain docstring description, variables used, input, output clearly explained
-  Airflow metadata can be passed via flags to the script and database
-  sql scripts e.g start_log.sql,end_log.sql, end_log_error.sql must be called from Python functions. That accept necessary parameters and job specific details.
-  Need to add and integrate function for old .log file deletion
-  In addition, current approach executes functions/operations linearly, multiple steps could be executed concurently
