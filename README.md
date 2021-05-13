# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Description
This project consists in building an ETL pipeline for a data lake hosted on S3. The steps followed by the pipeline are:
- load data from S3
- process the data into analytics tables using Spark
- load them back into S3

# Code
The code for this project is all contained inside the `etl.py` script. 

To run the job, an active EMR cluster must be running on AWS. Move the file into the local directory inside the Master node of the cluster and run the following command:

- `sparky-submit etl.py`

The script reads in the data from the source S3 bucket hosted by Udacity and in writes the final processed data in a personalized S3 bucket specific to the developer.
