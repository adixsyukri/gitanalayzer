#!/usr/bin/env python
"""
Git statistic analyzer
"""

import argparse
import os
import subprocess
#import json
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

def extract_info(val):
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
    parser.add_argument('--repo', dest='repo', required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.master('local').appName("git analyzer").getOrCreate()
    working_dir = os.getcwd()
    os.chdir('..')
    if not os.path.exists(args.repo):
        os.system('git clone %s/%s' % (args.baseurl, args.repo))
    os.chdir(args.repo)
    os.system('git pull -r origin master')
    output = subprocess.check_output('git log', shell=True)
    extracted = extract_info(output)
    result = commit_stats(extracted)
    os.chdir(working_dir)
    data_frame = spark.createDataFrame(result)
    data_frame.registerTempTable("data_frame")
    count = spark.sql("""
        select
            sum(addition) as addition,
            sum(deletion) as deletion,
            sum(addition - deletion) as lines,
            date as datetime,
            HOUR(date) as hour,
            MINUTE(date) as minute
        from data_frame
        group by
            datetime,
            HOUR(date),
            MINUTE(date)
        order by
            datetime,
            HOUR(date),
            MINUTE(date)
        """)
    print count.collect()
    fname = '%s-repo-stats.json' % args.repo
    count.coalesce(1).write.json(fname)

if __name__ == '__main__':
    main()
