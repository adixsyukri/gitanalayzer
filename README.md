Git Analyzer
============

    - To extract git information
    - To store the information in a readable format
    - to monitor multiple repository

## Setup
    git clone https://github.com/adixsyukri/gitanalyzer.git 
    cd gitanalyzer
    virtualenv venv
    source venv/bin/activate
    pip install pyspark

## Run
    spark-submit analyzer.py --baseurl <<git ssh url>> --repo <<repo1>> <<repo2>> <<repo3>>

    example:
    spark-submit analyzer.py --baseurl git@github.com:adixsyukri --repo gitanalyzer dc-wordcloud hdp-ansible-role

## Dashboard
    npm install
    node server.js
    access http://localhost:8080
