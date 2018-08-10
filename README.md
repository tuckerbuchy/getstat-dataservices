## Introduction
This a project completed for the Data Services team at STAT Search Analytics. The goal was to take a subset of STAT SERP data and build an ETL pipeline to process it. After processing, the SERP data would be used for some analytical functionality. 

## Architecture
![STAT ETL Architecture](https://raw.githubusercontent.com/tuckerbuchy/getstat-dataservices/master/getstat.png)

The ETL pipeline works as follows. Given a data folder containing SERP CSVs, the `script/stream.py` daemon watches for new files being added. When a new file is added, the CSV file is loaded into Spark (Extract), changes are made to adjust the data for loading (Transform), and finally the data is written as a stream to a Parquet datastore. 

When analytics is to be performed on the SERP data, the queries can should be made against the Parquet datastore. These queries can be made in the Spark SQL language. 

## Setup
This project relies on PySpark being installed on the host. We recommend installing PySpark through the Anaconda package management system for ease. **Note that this application is written using Python 2.7.**

 1. [Download Anaconda](https://www.continuum.io/downloads%20%28python%202.7%29). Ensure you down the Python 2.7 version.
 2. Follow the Anaconda installation procedure.
 3. Create a Anaconda environment with PySpark (named here `getStatEnv`).
     ```conda create -n getStatEnv -c conda-forge pyspark```
 4. Activate your new environment.
	 ```source activate getStatEnv```
  
TODO: ENVIRNMENT VARIABLES 

## Trade offs  
Apache Spark vs. Pandas  
Mongodb  
Csv vs. parquet  
  
  
## things I would do with more time  
Add HDFS to store the Parquet data