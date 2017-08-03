#!/usr/bin/env python
"""
Git statistic analyzer
"""

import argparse
import os
import subprocess
import json
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def round_time(date_time=None, date_delta=timedelta(minutes=5)):
    """Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
            Stijn Nevens 2014 - Changed to use only datetime objects as variables
    """
    round_to = date_delta.total_seconds()

    if date_time is None:
        date_time = datetime.now()
    seconds = (date_time - date_time.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = (seconds+round_to/2) // round_to * round_to
    return date_time + timedelta(0, rounding-seconds, -date_time.microsecond)

def format_date(date_time):
    """
    convert human readable date to datetime
    """
    rounded = round_time(datetime.strptime(date_time[:-6], '%a %b %d %H:%M:%S %Y'))
    return rounded.strftime('%Y-%m-%d %H:%M:%S')

def commit_stats(values):
    """
    return git diff information with diff stats info
    """
    result = []
    for row in values:
        stats = subprocess.check_output('git diff --numstat %s' % row['commit'], shell=True)
        for info in stats.split('\n'):
            splitted = info.split()
            if splitted:
                result.append({
                    'repo': row['repo'],
                    'date': row['date'],
                    'commit': row['commit'],
                    'author': row['author'],
                    'addition': splitted[0],
                    'deletion': splitted[1],
                    'file': splitted[2]
                })
    return result

def extract_by_key(key, val):
    """
    extract information by predefine key
    """
    splitted = val.split(key)
    return splitted[1].strip()

def extract_info(val, repo):
    """
    Extract the necessary information such as:
    - commit id
    - author
    - date
    """
    output = []
    regex = r"[a-f\d]{40}"
    splitted = val.split('\n')
    filtered = [x for x in splitted if 'merge'.upper() not in x.upper()]
    for i, row in enumerate(filtered):
        if re.search(regex, row):
            output.append({
                'repo': repo,
                'commit': extract_by_key('commit', filtered[i]),
                'author': extract_by_key('Author:', filtered[i+1]),
                'date': format_date(extract_by_key('Date:', filtered[i+2]))
            })

    return output

def main():
    """
    Main Function execution
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseurl', dest='baseurl', required=True)
    parser.add_argument('--repo', dest='repo', nargs='+', required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.master('local').appName("git analyzer").getOrCreate()
    working_dir = os.getcwd()
    stats_lists = []
    for repo in args.repo:
        os.chdir('..')
        if not os.path.exists(repo):
            os.system('git clone %s/%s' % (args.baseurl, repo))
        os.chdir(repo)
        os.system('git pull -r origin master')
        output = subprocess.check_output('git log', shell=True)
        extracted = extract_info(output, repo)
        stats = commit_stats(extracted)
        stats_lists.append(stats)
        os.chdir(working_dir)
    result = [item for row in stats_lists for item in row]
    data_frame = spark.createDataFrame(result)
    data_frame.registerTempTable("data_frame")
    count = spark.sql("""
        select
            repo,
            sum(addition) as addition,
            sum(deletion) as deletion,
            sum(addition - deletion) as lines,
            date as datetime,
            HOUR(date) as hour,
            MINUTE(date) as minute
        from data_frame
        group by
            repo,
            datetime,
            HOUR(date),
            MINUTE(date)
        order by
            repo,
            datetime,
            HOUR(date),
            MINUTE(date) 
        DESC
        """)
    fname = 'repo-stats.json'
#    if os.path.exists(fname):
#        os.system('rm -rf %s' % fname)
    with open(fname, 'w') as writefile:
        writefile.write(json.dumps(count.toPandas().to_dict(orient='records'), indent=4))
        print "Output: %s" % fname

if __name__ == '__main__':
    main()
