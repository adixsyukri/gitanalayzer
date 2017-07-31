Git Analyzer
============

    - To extract git information
    - To store the information in a readable format

## Run
    git clone https://github.com/adixsyukri/gitanalyzer.git 
    cd gitanalyzer
    virtualenv venv
    source venv/bin/activate
    pip install pyspark
    spark-submit analyzer.py --baseurl https://github.com/adixsyukri --repo gitanalyzer

