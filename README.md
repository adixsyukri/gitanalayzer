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
    pip install pyspark pandas

## Run
    spark-submit analyzer.py --baseurl <<git root url>> --repo <<repo1>> <<repo2>> <<repo3>>

    example:
    spark-submit analyzer.py --baseurl https://github.com/adixsyukri --repo gitanalyzer dc-wordcloud hdp-ansible-role

