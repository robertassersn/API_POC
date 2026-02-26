
### Description and motivation
provide a template for data ingestion, template can fluctuate on demand, therefore provide coverage examples for various scenarios that company can adapt.
Template must be easy to understand and causes ease in further development.

- fill path to config in .env file
- jobs are parametrized, therefore you'll need to create and fill config file using example from /parameters/job_parameters _example.conf
  
**ingestion pipelines can be found at**: /ingestion/pipelines/

**carvago_to_filesystem_example.py** provides example how to load files into filesystem

**carvago_to_postgresql_example.py** provides examples how to load files into database (in current example Postgresql)

**filesystem_to_postgresql.py** provides examples how to load from filesystem into database

**/ingestion/sources/worldbank.py** example of datasource that yields .xml and library handles it


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
<summary>Limitations</summary>
  
- DLT on its own does not retain original output from API call, it always normalizes it. However it is possible to retain both original and normalized output without difficulty on demand.
</details>

<details>
<summary>RAW response example (CARVAGO)</summary>
  
<img width="1273" height="224" alt="image" src="https://github.com/user-attachments/assets/08efb8ad-af07-4ee3-9ef8-e0caa66a5b51" />

</details>


<details>
<summary>Output example</summary>
  
<img width="709" height="432" alt="image" src="https://github.com/user-attachments/assets/9930fabd-1a5d-43f5-9052-ca1a1e1c2cfa" />

Generated output files example
<img width="678" height="401" alt="image" src="https://github.com/user-attachments/assets/c1961b5c-d956-4e2b-bf11-3c7a43a1ff0d" />
</details>

<details>
<summary>Assumptions</summary>
  
- Colleagues must understand python language, have ability to install python packages, read and understand python packages on demand on high level.
- Understandent of .env and .conf file usage
  
</details>

<details>
<summary>Prerequisites</summary>
  
- install python packages from requirements.txt
- create and fill parameters file in parameters directory according to example
- create and fill .env file according to provided example
</details>

<details>
<summary>Features</summary>
  
- possibility to insert additional metadata columns on demand so sync with external processes
<img width="681" height="243" alt="image" src="https://github.com/user-attachments/assets/c8e158d8-e92b-44c9-aa0a-2d8da04a11f7" />

- Logging included in template proposal
<img width="1335" height="611" alt="image" src="https://github.com/user-attachments/assets/76a88f9e-00b3-4478-a27e-2e915a89fdb4" />

- tracking of historical pipeline runs
<img width="668" height="160" alt="image" src="https://github.com/user-attachments/assets/fca0ad53-5f40-4ba5-9ccf-e9483b52ca60" />

- Possbility to avoid duplication from source by defining API output Primary key + write disposition that controls loading behavior of job
<img width="859" height="240" alt="image" src="https://github.com/user-attachments/assets/3fd385ef-67c9-44d5-86ce-02d8619d12b9" />

- Library supports creating table's on its own. Meaning with enough permissions it can create schema + tables required to insert data
<img width="810" height="490" alt="image" src="https://github.com/user-attachments/assets/613eb827-4441-4850-8691-2735b752f310" />
</details>


