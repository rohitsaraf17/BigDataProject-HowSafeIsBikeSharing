# Realtime and Big Data Analytics Project
CSCI-GA.3033-006 - Realtime and Big Data Analytics


## We are using the following data to analyze How Safe is Bike Sharing in New York City.

1. Motor Vehicle Crash[https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions-Crashes/h9gi-nx95]
2. NYC Weather Data https://www.ncdc.noaa.gov/
3. NYC Citi Bike Data https://s3.amazonaws.com/tripdata/index.html

## Team Members:

1. Rohit Saraf
2. Mohit Nihalani
3. Hardik Bharath Rokad



##  Folders and process to run ...

# 01. Data Ingest
	a) WeatherData-Ingest: It contains the command for ingesting weather datasets into the HDFS. 
	b) CitiBike-Ingest: It contains the command for ingesting Citi bike datasets into the HDFS. 
	c) Accidents-Ingest: It contains the command for ingesting accidents datasets into the HDFS.

# 02. Profiling Code
	a) WeatherProfiling: It contains the map reduce for profiling weather data.
	b) CitiBikeProfiling: It contains the map reduce for profiling Citi bike data.
	c) AccidentsProfiling: It contains the map reduce for profiling accidents data.

# 03. ETL
	a) WeatherETL: It contains the map reduce for cleaning weather data.
	b) CitiBikeETL: It contains the map reduce for cleaning Citi bike data.
	c) AccidentsETL: It contains the map reduce for cleaning accidents data.
	D) lat-long : It contains local python code for extracting zip codes using latitude and longitude for accidents and Citi bike dataset.
		i) /citibike/extractzipcode.py -> extracting zip code of Citi Bike 
		ii) /accidents/read.py --> ConvertingLatLong for accidents data.

# 04 Code Iterations: This directory consists of java files that didn't make it to the final code.

# 05 Source Code for the analytics - this directory contains the hive queries which were used for the analytics.

# 05. Outputs It  contains the screenshots of the analytics

######################################## Process for Weather Dataset #########################################

1. Dataset (the dataset file is present in Data ingest directory) https://www.ncdc.noaa.gov/
2. Data Schema
* 	WBAN
* 	Station no
*	Year (int)	
*	Dew(double)
*	Month (int)	
*	Visibility (double) 
*	Average temperature(double)	
*	Windspeed (double).
*	Weather_State (String)	
*	Max temperature   (double)

3. We now ingest the data into dumbo
* Step 1: I tried using two ways, one was the curl command, the other one is the transferring it from local machine to Hadoop node. 
** Step 1: I used the curl command to ingest data initially,   
** Step 2: Downloaded the csv version and transferred it to Hadoop using WinSCP. This way the entire data is now comma separated and easy to operate on. To transfer using WinSCP 
1. Connect to VPN 
2. Open winscp 
3. Login using your nyu creds 
4. Drag and drop the file from your local to PUTTY 

4. MapReduce Code
We now perform ETL and Profiling
The code for both of these is present in the ETL directory and Profiling directory


We now execute the code using the below command
Compile the mapper class
Mapper class -->javac -classpath `yarn classpath` -d . NYCweatherdataMapperEX1.java
Compile the driver
javac -classpath `yarn classpath`:. -d . NYCweatherdata.java

In the Data cleaning process, we use Hadoop MapReduce to do the data cleaning and formatting. (refer the ETL and the Profiling directory for the code and output screenshots)
The Output is stored in the below hdfs directory and we save the output in a csv file.
hdfs dfs -cat /user/hbr244/Project/output58/part-m-00000 > Weather_Output1.csv
 
5. Ingest data into Hive
The Cleaned and formatted data is now ingested into HIVE using the below commands. 
hdfs dfs -mkdir ProjectInput 
Get the data into Hive 
hdfs dfs -put Weather_Output1.csv ProjectInput 
To create a table in your database 
Beeline; 
!connect jdbc:hive2://babar.es.its.nyu.edu:10000/ 
Use hbr244; 
create external table weather2 (Year int, Month int, Avg_Temp double,Dew double,Visibility double, WindSpeed double, MaxTemp double, MinTemp double,Weather_State string) row format delimited fields terminated by ',' location '/user/hbr244/ProjectInput/'; 

#############################################   Process for Citi Bike Dataset ################################
1.  Ingesting Data:
    a. Url of the data are stored in file urls.txt.
    b. To download the data run the script file using ./download.sh. Data will be stored in /data directory.
2. Push the data into HDFS. hfs dfs -put /user/mn2643/FinalProject/data /home/mn2643/FinalProject/data
Once the files are transferred to dumbo, create a directory on HDFS and store the input file
--> Location of Data into HDFS is : /user/mn2643/FinalProject/data
--> Location of Data into Dumbo is : /home/mn2643/FinalProject/data
There will be multiple input file in /home/mn2643/FinalProject/data, as data is divided into quarterly

3. Then Profiling Code is Run on the data in HDFS
--> Mapper and Reducer File location : /home/mn2643/FinalProject/CitiMapper.java
--> Mapper: /home/mn2643/FinalProject/CitiMapper.java, 
	Jar File-> javac -classpath `yarn classpath` -d . CitiMapper.java 
--> Reducer: /home/mn2643/FinalProject/CitiReducer.java, 
	Jar File --> javac -classpath `yarn classpath` -d . CitiReducer.java 
--> Job: /home/mn2643/FinalProject/CitiJob.java/, Jar File --> 
	jar -cvf citi.jar *.class
--> Running Job File: hadoop jar  citi.jar CitiJob /user/mn2643/FinalProject/data /user/mn2643/FinalProject/output
--> Output of MapReduce: /user/mn2643/FinalProject/output

4. Cleaning the Code Based on the analysis of Profiling
--> Mapper and Reducer File location : /home/mn2643/FinalProject/Clean
--> Mapper: /home/mn2643/FinalProject/CitiCleanMapper.java
	jar-->javac -classpath `yarn classpath` -d . CitiCleanMapper.java
--> Reducer: /home/mn2643/FinalProject/CitiCleanReducer.java
	jar-->javac -classpath `yarn classpath` -d . CitiCleanReducer.java
--> Job: /home/mn2643/FinalProject/CitiCleanJob.java
	jar--> jar -cvf citiclean.jar *.class
--> Running Job File: hadoop jar citiclean.jar CitiCleanJob /user/mn2643/FinalProject/data /user/mn2643/FinalProject/clean/
--> Output of MapReduce: /user/mn2643/FinalProject/clean/

5. Move Clean Output to Hive Directory in HDFS:
--> mkdir /user/mn2643/FinalProject/HiveData/Data
--> hdfs dfs -mv /user/mn2643/FinalProject/clean/part-r-00027 /user/mn2643/FinalProject/HiveData/Data

6. Creating Citi Bike Table in Hive:
-->create external table citibike (date1 string,date2 string, distance double, startid int, endid int, duration double,
lat1 double, lat2 double, longitude1 double, longitude2 double, age double, startname string, endname string)
row format delimited fields terminated by ','
location '/user/mn2643/FinalProject/HiveData/Data/';

7. Extracting latitude and longitude of all unique stations and insert into HDFS directory for further analysis:
--> Extract zip code --> python extractzipcode.py
-->INSERT OVERWRITE DIRECTORY '/user/mn2643/hiveu'
select lat1, longitude1, startid from citibike WHERE startid in (select distinct * from (select startid as startid from citibike UNION ALL select endid as startid from citibike) 
w )group by startid,lat1, longitude1) 
--> Location of Hive Query Ouputu in HDFS : /user/mn2643/hiveu/
--> Hive Query Stored Output location on local: /home/mn2643/FinalProject/HiveOutput/start_lat_long_unique

8. Create new Folder in Dumbo for PostCodes:
--> After extracting zipcode locally new file for postcode was created and added into dumbo:
--> location /user/mn2643/FinalProject/HiveData/postCodes

9. Move postcode data from Dumbo to HDFS:
--> hdfs dfs -mv /home/mn2643/FinalProject/HiveOutput/postcode /user/mn2643/FinalProject/HiveData/postcode

10. In hive create new external tables for postcode:
--> postcode
--> finaldata (this is similar to Citibike external table) but with removed unnecessary coloumns.

11. At the end, there are three external tables is Hive:
Hive Execution:
Launch the hive shell on dumbo:
-->beeline
-->!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
-->Database used: mn2643
use mn2643,

External Tables Used for analytics:
--> accidents -> zipcode, accidents
--> Finaldata -> date, distance, station id, duration, location, age, station name

######################################################## NYC Vision Zero Dataset #######################################################

1. Dataset:

Link to InputDataSet-"https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions-Crashes/h9gi-nx95"

-->Download dataset
curl -O "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD" \\to download data on local

-->Rename dataset
mv row.csv?accessType=DOWNLOAD vehicleCollisionData.csv      

Input file : "vehicleCollisionData.csv"
The dataset contains information about accidents.The fields in the input dataset are as follows:
--> date
--> time
--> borough
--> latitude
--> longitude	
--> street address	
--> number of person injured	
--> number of person killed	
--> vehicle type1
--> vehicle type2

2. Push the data onto Dumbo and store it in HDFS. 
Once the files are transferred to dumbo, create a directory on HDFS and store the input file(vehicleCollisionData.csv) in the directory(Project).

-->Input Directory and File on HDFS:
hdfs dfs -mkdir /user/rs6785/Project
hdfs dfs -put ./vehicleCollisionData.csv Project

3. Profile Data.
Run the map reduce functions under the directory /user/rs6785/Project/Profile to get information about the dataset.
-->Profiling Execution

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/rs6785/Project/Profile/UsefulDataProfileMapper.py,hdfs://dumbo/user/rs6785/Project/Profile/UsefulDataProfileReducer.py -mapper "python UsefulDataProfileMapper.py" -reducer "python UsefulDataProfileReducer.py" -input /user/rs6785/Project/vehicleCollisionData.csv -output /user/rs6785/Project/dataInfo

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/rs6785/Project/Profile/columnDataTypeProfile.py,hdfs://dumbo/user/rs6785/Project/Profile/columnDataTypeProfileReducer.py -mapper "python columnDataTypeProfile.py" -reducer "python columnDataTypeProfileReducer.py" -input /user/rs6785/Project/vehicleCollisionData.csv -output /user/rs6785/Project/outputUsefulDataProfile 

4. Clean Data.
Run the map reduce functions under the directory /user/rs6785/Project/FilesForClean to clean the dataset.
-->Cleaning Execution
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/rs6785/Project/FilesForClean/FinalCleanDataMapper.py,hdfs://dumbo/user/rs6785/Project/FilesForClean/FinalCleanDataReducer.py -mapper "python FinalCleanDataMapper.py" -reducer "python FinalCleanDataReducer.py" -input /user/rs6785/Project/outputPostCode.csv -output /user/rs6785/Project/outputHivePostCodeData

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/rs6785/Project/FilesForClean/FCleanMapper.py,hdfs://dumbo/user/rs6785/Project/FilesForClean/FinalCleanDataReducer.py -mapper "python FCleanMapper.py" -reducer "python FinalCleanDataReducer.py" -input /user/rs6785/Project/outputPostCode.csv -output /user/rs6785/Project/FData

5. Run the Analytics on InputData using Hive.
-->Create and InputDirectory and push the input data into it.
hdfs dfs -mkdir Project/HiveFData/
hdfs dfs -mkdir Project/HiveInput/
hdfs dfs -mv Project/FData/part-00000 Project/HiveFData/
hdfs dfs -mv Project/outputHivePostCodeData/part-00000 Project/HiveInput/

6. Hive Execution:
Launch the hive shell on dumbo:
-->beeline
-->!connect jdbc:hive2://babar.es.its.nyu.edu:10000/
-->Database used: rs6785
use rs6785;

7. Query To Load Accident Data on HIVE
create external table accident (year int, month int, zipcode int, vehicle string)
row format delimited fields terminated by ' '
location '/user/rs6785/Project/HiveInput/';

create external table accidentinfo (year int, month int, zipcode int, injured int, killed int, vehicle string)
row format delimited fields terminated by ' '
location '/user/rs6785/Project/HiveFData/';
