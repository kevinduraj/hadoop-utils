#!/bin/bash

#clear; time mvn clean compile assembly:single
java -jar target/ReadSequenceFile-1.0.1-jar-with-dependencies.jar ~/hadoop-utils/WriteSequenceFile/data_02

