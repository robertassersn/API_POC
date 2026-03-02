
### Description and motivation
provide a template for data ingestion, template can fluctuate on demand, therefore provide coverage examples for various scenarios that company can adapt.
Template must be easy to understand and causes ease in further development.

**TL;DR**
When data is received from datasource DLT framework takes care of normalization of records into their dedicated grain, tracks schema changes, allows control over the job and data in case of schema changes. Supports wide range of destinations where data can be stored. Meaning Data Engineer can focus on writing and maintaining data extraction from source scripts.

- fill path to config in .env file
- jobs are parametrized, therefore you'll need to create and fill config file using example from /parameters/job_parameters _example.conf

### Examples
**ingestion pipelines can be found at**: /ingestion/pipelines/

**carvago_to_filesystem_example.py** provides example how to load files into filesystem

**carvago_to_postgresql_example.py** provides examples how to load files into database (in current example Postgresql)

**filesystem_to_postgresql.py** provides examples how to load from filesystem into database

**/ingestion/sources/worldbank.py** example of datasource that yields .xml and library handles it

### Additional information

**pipeline algorithm:**
https://dlthub.com/docs/reference/explainers/how-dlt-works

**DLT Terms**
<details>
<summary>Source</summary>
A source is a logical grouping of resources, i.e., endpoints of a single API. The most common approach is to define it in a separate Python module.
</details>

<details>
<summary>Pipeline</summary>
A pipeline moves data from your Python code to a destination. The pipeline accepts dlt sources or resources, as well as generators, async generators, lists, and any iterables. Once the pipeline runs, all resources are evaluated and the data is loaded at the destination.
</details>

<details>
<summary>Schema contract</summary>
a config, describing pipeline behavior in case of schema changes. Changes may be on table,column, column data types. Allowing to adjust due to particular changes or stop the job altogether.
  
In depth description can be found here:
https://dlthub.com/docs/general-usage/schema-contracts
</details>

**Further Details**
<details>
<summary>Limitations</summary>
  
- DLT on its own does not retain original output from API call, it always normalizes it. However it is possible to retain both original and normalized output without difficulty on demand.
- currently destinations are limited to postgresql and local filesystem
</details>

<details>
<summary>RAW response example (CARVAGO)</summary>
  
<img width="1273" height="224" alt="image" src="https://github.com/user-attachments/assets/08efb8ad-af07-4ee3-9ef8-e0caa66a5b51" />

</details>


<details>
<summary>Output example</summary>
  
- received data during api request is being normalized to its grain. Meaning one response may yield multiple files/tables
  
<img width="709" height="432" alt="image" src="https://github.com/user-attachments/assets/9930fabd-1a5d-43f5-9052-ca1a1e1c2cfa" />

Generated output files example

<img width="678" height="401" alt="image" src="https://github.com/user-attachments/assets/c1961b5c-d956-4e2b-bf11-3c7a43a1ff0d" />
</details>

<details>
<summary>Assumptions</summary>
  
- Familiarity with python requires, have ability to install python packages, read and understand python packages on demand on high level.
- Understanding of .env and .conf file usage
- For consistency I'm requested to pass ALL variables via .conf file
-  ingestion/schemas/export directory wouldn't be listed in prod, but provides an example how schema definition for ingested files would be stored
  
</details>

<details>
<summary>Prerequisites</summary>
  
- install python packages from requirements.txt
- create and fill parameters file in parameters directory according to example
- create and fill .env file according to provided example
</details>

<details>
<summary>Features</summary>
  
- Possibility to reset pipeline state completely (delete metadata from configurations and start fresh. Does not drop/truncate tables)
<img width="902" height="204" alt="image" src="https://github.com/user-attachments/assets/7bc46351-2195-41dc-9bab-a00084d83186" />

  
- Quality features: https://dlthub.com/docs/general-usage/data-quality-lifecycle
- linkage between normalized tables/filzes
<img width="547" height="331" alt="image" src="https://github.com/user-attachments/assets/aae1c383-4bc8-4d1c-85a9-37561482925a" />

- Possibility to define schema contracts: https://dlthub.com/docs/general-usage/schema-contracts
- possibility to insert additional metadata columns on demand so sync with external processes
<img width="681" height="243" alt="image" src="https://github.com/user-attachments/assets/c8e158d8-e92b-44c9-aa0a-2d8da04a11f7" />

- Logging included in template proposal
<img width="1335" height="611" alt="image" src="https://github.com/user-attachments/assets/76a88f9e-00b3-4478-a27e-2e915a89fdb4" />

- tracking of historical pipeline runs
<img width="668" height="160" alt="image" src="https://github.com/user-attachments/assets/fca0ad53-5f40-4ba5-9ccf-e9483b52ca60" />

- Possibility to avoid duplication from source by defining API output Primary key + write disposition that controls loading behavior of job
<img width="859" height="240" alt="image" src="https://github.com/user-attachments/assets/3fd385ef-67c9-44d5-86ce-02d8619d12b9" />

- Library supports creating table's on its own. Meaning with enough permissions it can create schema + tables required to insert data
<img width="810" height="490" alt="image" src="https://github.com/user-attachments/assets/613eb827-4441-4850-8691-2735b752f310" />
</details>


