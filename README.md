# Call Data Analysis

### Big Data Framework Chosen

The framework selected for the given task is Spark + HDFS. We can use PySpark API available in Spark and other Python libraries for our task. 

![PySpark + HDFS](https://github.com/zubairalijaleel/adnan_assignment/blob/master/spark_and_hdfs.png)

The reasons behind the framework decision are:
* Spark is an in-memory distributed computing framework which can work on large data sets residing in clusters of commodity servers. For our use case we need a framework which can work on large data sets.
* Spark abstracts APIs in many language including, Java, Scala, Python, R etc. It has native APIs like MLlib which can be used for anomaly detection use cases like ours.
* Spark comes with standalone cluster management options, however HDFS + YARN provides better distributed resource management (scalability, reliability, mutli-tenacy, security). And we can use files stored in HDFS for other analyzing framework like HIVE in parallel to Spark.
* Our call data in Avro format will be stored in HDFS. YARN will manage cluster and job scheduling. Spark will work on the data sets to carry out the business logic.
* PySpark is an Python API for Spark. It has libraries which are better suited for anomaly detection.

### iPython Jupyter Notebook
An iPython Jupyter Notebook and its exported conda environment (.yml) are attached with the solution files. It is a Python code which works on one data file and not on Spark. It takes an avro file, extracts the schema, find max & average RxLevel, flatten the data, saves it in csv file, convert the csv into parquet file which is partitioned by the MCC information.

### Anomaly detection
* We can use MLlib API for anomaly detection.
* The impact of having anomalies in big data sets is: It will produce inaccurate analysis results. For example, if we want to discover some pattern from the given data set, the aberrant value (anomaly) will influence the machine learning model that we train and it will make the model less accurate. 
* We can reduce the impact of anomaly by replacing the extreme values by the average value (or most occurring categorical value).

### How to run the code
* Download and install anaconda 
* Download 'adnan_conda_environment.yml' file and import this conda environment that I used for this assignment using following command:

`conda env create -n adnan -f adnan_conda_environment.yml`
* Download the call_data_analysis.ipynb file to a folder
* In terminal, cd to the folder and activate the conda environment using following command:

`conda activate adnan`
* Download the input avro file to the same folder from this link: https://p3filecloud.p3-group.com/Download/ad436b2dc52e6b03f02a513e6f9c42
* Run jupyter notebook in the same folder using the command: 

`jupyter notebook`
* Open and run all the cells in 'call_data_analysis.ipynb' file
* It will create csv and parquet files, displays avro schema, max & average of RxLevel.
