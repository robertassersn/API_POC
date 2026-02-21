
### Description and motivation
provide a template for data ingestion, template can fluctuate on demand, therefore provide coverage examples for various scenarios that company can adapt.
Template must be easy to understand and causes ease in further development.

### Assumptions
- Colleagues must understand python language, have ability to install python packages, read and understand python packages on demand on high level.
- Understandent of .env and .conf file usage
- I'm restricted to using .env and .conf files together (cannot use only single type)

### Prerequisites
- install python packages from requirements.txt (need to update file)
- create and fill parameters file in parameters directory according to example
- create and fill .env file according to provided example


### DRAFT NOTES

pipeline algorithm:
https://dlthub.com/docs/reference/explainers/how-dlt-works

- DLT on its own does not retain original output from API call, it always normalizes it. However it is possible to retain both original and normalized output without difficulty on demand.

possibility to insert additional metadata columns on demand so sync with external processes
<img width="681" height="243" alt="image" src="https://github.com/user-attachments/assets/c8e158d8-e92b-44c9-aa0a-2d8da04a11f7" />

TODO:
- update requirements.txt

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

