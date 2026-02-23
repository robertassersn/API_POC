
### Description and motivation
provide a template for data ingestion, template can fluctuate on demand, therefore provide coverage examples for various scenarios that company can adapt.
Template must be easy to understand and causes ease in further development.

- jobs are parametrized, therefore you'll need to create and fill config file using example from /parameters/job_parameters _example.conf

**pipeline algorithm:**
https://dlthub.com/docs/reference/explainers/how-dlt-works

**ingestion pipelines can be found at:**
/ingestion/pipelines/

**carvago_to_filesystem_example.py** provides example how to load files into filesystem
**carvago_to_postgresql_example.py** provides examples how to load files into database (in current example Postgresql)

**Limitations**:
- DLT on its own does not retain original output from API call, it always normalizes it. However it is possible to retain both original and normalized output without difficulty on demand.



### Assumptions
- Colleagues must understand python language, have ability to install python packages, read and understand python packages on demand on high level.
- Understandent of .env and .conf file usage
- I'm restricted to using .env and .conf files together (cannot use only single type)

### Prerequisites
- install python packages from requirements.txt
- create and fill parameters file in parameters directory according to example
- create and fill .env file according to provided example

### Features
- possibility to insert additional metadata columns on demand so sync with external processes
<img width="681" height="243" alt="image" src="https://github.com/user-attachments/assets/c8e158d8-e92b-44c9-aa0a-2d8da04a11f7" />

- Logging included in template proposal
<img width="1335" height="611" alt="image" src="https://github.com/user-attachments/assets/76a88f9e-00b3-4478-a27e-2e915a89fdb4" />

- tracking of historical pipeline runs
<img width="668" height="160" alt="image" src="https://github.com/user-attachments/assets/fca0ad53-5f40-4ba5-9ccf-e9483b52ca60" />


### DRAFT NOTES



Qs
- pipeline stoppage possibility in case of schema change
- examples how very dynamic pipelines get structured

examples to provide
- retention of original format
- parsed example in filesystem
- example of results in db
- metadata passage
- provide sample of logs
- provide means to easily reset schema, e.g column renaming from api side
- orchestration in airflow using proper commands
- means of output format control

Topics 
- sftp to DB
- DB to SFTP
- API
- authentication
- reverse ETL
- terminal commands vs script run, info provided by terminal commands
- Means of quality assurance besides schema validation
- explain how to interpret metadata 

