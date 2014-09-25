#!/bin/bash

clear; time mvn clean compile assembly:single
java -jar target/WriteSequenceFile-1.0.1-jar-with-dependencies.jar data_02 

#-- put data into hdfs files system
#hdfs dfs -rm data_01 
#hdfs dfs -put data_01 

