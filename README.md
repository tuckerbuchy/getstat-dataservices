
# Get STAT Data Services Assesment

## Introduction 

This a project completed for the Data Services team at STAT Search Analytics. The goal was to take a subset of STAT SERP data and build an ETL pipeline to process it. After processing, the SERP data would be used for some analytical functionality.     
    
## Architecture ![STAT ETL Architecture](https://raw.githubusercontent.com/tuckerbuchy/getstat-dataservices/master/assets/getstat.png)    
 The ETL pipeline works as follows. Given a data folder containing SERP CSVs, the `script/stream.py` daemon watches for new files being added. When a new file is added, the CSV file is loaded into Spark (Extract), changes are made to adjust the data for loading (Transform), and finally the data is written as a stream to a Parquet datastore.     
    
When analytics is to be performed on the SERP data, the queries can should be made against the Parquet datastore. These queries can be made in the Spark SQL language.     
    
## Environment Setup This project relies on PySpark being installed on the host. We recommend installing PySpark through the Anaconda package management system for ease. **Note that this application is written using Python 2.7.**    
  ### Installing the Dependencies  
 - [Download Anaconda](https://www.continuum.io/downloads%20%28python%202.7%29). Ensure you down the Python 2.7 version.    
 - Follow the Anaconda installation procedure.    
 - Create a Anaconda environment with PySpark (named here `getStatEnv`).    
     ```conda create -n getStatEnv -c conda-forge pyspark```    
 - Activate your new environment.    
    ```source activate getStatEnv```  
  
  ### Setting up Environment Variables  
This project uses two environment variables that must be set prior to running the application.  
  
 - `SERP_SOURCE_DIR` : A directory that will act as the 'data source' for the SERP .csv files. When starting the application for the firs time, make this a directory containing only the SERP csv provided with the assignment.   
 - `SERP_TARGET_DIR` : The directory where that will act as the on disk location of our data lake. This is where the ETL pipeline will write the Parquet files, and where the analytics engine will read data from. This can just any directory that the application has read/write access to.  
  
Here is a snapshot of what my setup looks like:  
![Local Directory Setup](https://raw.githubusercontent.com/tuckerbuchy/getstat-dataservices/master/assets/dir_structure.png)  
`SERP_SOURCE_DIR` is set to `$HOME/stat/source/`  
`SERP_TARGET_DIR` is set to `$HOME/stat/target/`  
  
## Starting the Application  
  
#### ETL Streamer  
To use the application, we first need to start streaming our SERP data into the data lake. This is done through a streamer script that will run in the background. To run this script, navigate to the code directory and run the following.  
  
`PYTHONPATH=$(pwd) python scripts/stream.py`  
  
This will start a looping stream that watches for new files coming into the `SERP_SOURCE_DIR`. Give the streamer a bit of time to ETL the data in the directory.  
  
#### Analytics  
To perform analytics on the data lake, we can simply run the scripts provided in `scripts/`. Open a new terminal, and run the scripts for each question. There is one script for each question required by the assignment  
  
##### Question 1  
Which URL has the most ranks in the top 10 across all keywords over the period?  
`PYTHONPATH=$(pwd) python scripts/first_question.py`

Sample Response:
```
+--------------------+-------------+
| URL|TimesInTopTen|
+--------------------+-------------+
|serps.com/tools/r...|  22228|
|moz.com/tools/ran...|  21810|
|www.rankscanner.com/|  14965|
| ranktrackr.com/|  14474|
|www.shoutmeloud.c...|  11702|
| www.google.com/|  11289|
| www.google.co.uk/|  10000|
| proranktracker.com/| 9142|
|www.link-assistan...| 8831|
|  serps.com/| 7959|
+--------------------+-------------+  
```
##### Question 2  
Provide the set of keywords (keyword information) where the rank 1 URL changes the most over the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is different from the previous day's URL.  
`PYTHONPATH=$(pwd) python scripts/second_question.py`  

Sample Response:
```
+--------------------+---------------+
| Keyword|NumberOfChanges|
+--------------------+---------------+
|seo market & oppo...| 23|
|  stat local| 21|
|  rob search| 20|
| answers boxes| 20|
| serps can| 20|
|  mobile ranking| 18|
| rank tracker 2013| 18|
|how much does sta...| 18|
|precise google ra...| 18|
|spanish serp trac...| 18|
+--------------------+---------------+
```
##### Question 3  
We would like to understand how similar the results returned for the same keyword, market, and location are across devices. For the set of keywords, markets, and locations that have data for both desktop and smartphone devices, please devise a measure of difference to indicate how similar the URLs and ranks are, and please use this to show how the mobile and desktop results in our provided data set converge or diverge over the period.  
`PYTHONPATH=$(pwd) python scripts/third_question.py`  

Sample Response: 
```
+--------------------+------+--------+-------------------+
| keyword|market|location| ConsistencyScore|
+--------------------+------+--------+-------------------+
| google answer boxes| US-en|  |0.13724400871459694|
|organic share of ...| CA-en|  |0.13063599100885892|
|  how to track serps| CA-en|  |0.12796666666666667|
|daily serp analytics| US-en|  | 0.1264|
| serp analysis| GB-en|  |0.12281784708685294|
|  serp checker| GB-en|  |0.12233333333333334|
|  how to track serps| US-en|  |0.12177777777777778|
|organic share of ...| GB-en|  |0.12137513751375137|
|what is rank trac...| US-en|  |0.12055005500550055|
| organic ctr and seo| GB-en|  |0.12021111111111112|
+--------------------+------+--------+-------------------+
```  

## The Process I Went Through  
In the process to build this application I went through several stages. Firstly, I thought about the problem and the issues that could be encountered. The most obvious one is the size of the data. With the sample given being around 1 GB uncompressed CSV with 10 million row, and the real data more like 54 billion rows, that would be more like 5 TB of data each month to process. This means using in-memory analytics tools like Python's Pandas is out of the questions as it will not be able to read the entire dataset in. Spark was the obvious choice for this as it provides the ability to analyze datasets too large for memory. Next, was to design the ETL architecture. I initially through I would write code to watch for new SERP csv files in a directory and pump those rows into a MongoDB for efficient storage. I soon learned about Spark streaming which enabled me to run a streamer looking for files in a directory instead of writing this code to myself. I switched away from MongoDB to Parquet upon learning that the connector did not support write streaming. Parquet surprised me with it's performance gains compared to reading from CSV. I found a [blog post](https://dzone.com/articles/how-to-be-a-hero-with-powerful-parquet-google-and) describing this which convinced me. 

The last step involved writing code to extract the analytics required by the assignment. I ran the streaming service to populate the data, then launched an interactive `iPython` terminal to play around with the SERP data. This made it easier to discover a query that would work to extract the desired information. 
  
## Things To Do With More Time  
If I had more time on this application, I would:  
  
 - **Use AWS S3 as the data source for SERP CSV files**. I had some trouble installing the Spark .jars that are required for this, and thought it may be cumbersome to make the user set up AWS credentials to access the files.  
 - **Use HDFS as the location of the Parquet datastore**. In a real scenario, we need a network mounted file system so that all the slaves running in the Spark cluster can access the data. I would use HDFS for this.   
 - **Deploy the ETL and Analytics to a full Spark cluster**. Currently, I am running everything in "local" mode, provided through Spark. To make this ready for production, I would swap this mode with the URL of a real spark cluster.   
 - **Consider porting to Scala**. I found a lot of the best examples and documentation were written in Scala. I went with Python as it is more familiar to me, but think the online resources make Scala a better choice.   