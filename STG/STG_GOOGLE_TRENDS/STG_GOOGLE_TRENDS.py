import sys 
import os 
base_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),'../../'
        )
    )
sys.path.append(base_path)
job_config = {
    "JOB_NAME" : "STG_GOOGLE_TRENDS"
    ,"DATA_SOURCE":"GOOGLE"
    ,"FILE_DIRECTORY":os.path.dirname(os.path.abspath(__file__))
    ,"FILE_NAME":os.path.basename(__file__)
    ,"JOB_LAYER":'STG'
}

path_to_sql_scripts = os.path.join(
    job_config['FILE_DIRECTORY']
    ,'sql'
)