# CrimeDataAnalysis

Contributors: Arati Maurya,Shreya Bhattacharya


# Goal 
This project aims to perform analysis of crime data across the United States using Big Data technologies. There are four types of analysis performed on different datasets collected from various sources.
The analysis is performed using Apache Spark.
The Analysed data is then represented in various graphical formant like Bar chart,Pie chart etc.


# File Storage
Hadoop HDFS

# Framework used
Apache Spark

# Visualization tool
Tablesaw


<img src="https://github.com/dev-shreya/CrimeDataAnalysis/blob/main/w2.PNG" width="500"/>

# Language used
Java
       
# Types of Analysis that are performed
1  Annual average crime rate in San Diego

2  Top 5 crimes in Los Angeles

3  Top 5 cities in Maryland with highest crime rate

4  Maximum crime occurences at different time of the day(Morning, Evening, Midnight)

# Dataset:
We have collected dataset from various sources like:
Kaggle,https://datasetsearch.research.google.com/search?src=0&query=Crime&docid=L2cvMTFxcDJyZjV2bg%3D%3D,data.world etc


# Steps to Run the Application:
1. IDE required e.g. IntelliJ IDEA community version
2. Select Maven as a build tool
3. Clone git repository
4. Open the cloned project in IntelliJ IDE
5. Place dataset files from input directory to hdfs. Following steps needs to be performed in order to store dataset on HDFS: 
(Execute the commands below on the command prompt) 
6. Start all the daemons 
7. make new directory on hdfs 
8. copy input dataset files from locat to hdfs directory
9. Open edit run configuration and select java class for analysis execution 

#### Note: If you don’t have a local hadoop environment setup to access the data files from hdfs, you can directly access the files from the project input folder. In the “RDD from CSV” sections of each analysis, uncomment the line where we are reading from the input folder and comment on the line where we are reading from hdfs





