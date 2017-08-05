#!/usr/bin/env python
"""
Git statistic analyzer
"""

import argparse
import os
import subprocess
from subprocess import CalledProcessError
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
            else:
                result.append({
                    'repo': row['repo'],
                    'date': row['date'],
                    'commit': row['commit'],
                    'author': row['author'],
                    'addition': '0',
                    'deletion': '0',
                    'file': "No"
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
            print args.baseurl
            print repo
            os.system('git clone %s/%s' % (args.baseurl, repo))
        os.chdir(repo)
        print repo
        os.system('git pull -r origin master')
        try:
            print "try git log %s" % repo
            output = subprocess.check_output('git log', shell=True)
        except CalledProcessError:
            print "No log yet"
            continue
        print "extract info %s" % repo
        extracted = extract_info(output, repo)
        print "get commit stats %s" % repo
        stats = commit_stats(extracted)
        stats_lists.append(stats)
        os.chdir(working_dir)
    if stats_lists:
        result = [item for row in stats_lists for item in row]
        data_frame = spark.createDataFrame(result)
        data_frame.registerTempTable("data_frame")
        count = spark.sql("""
            select
                repo,
                commit,
                sum(addition + deletion) as lines_change,
                date as datetime,
                1 as count
            from data_frame
            group by
                repo,
                commit,
                datetime
            order by
                repo,
                commit,
                datetime 
            DESC
            """)
        fname = working_dir + '/repo-stats.json'
        temp = working_dir + '/temp.json'
        print temp
        with open(temp, 'w') as writefile:
            writefile.write(json.dumps(count.toPandas().to_dict(orient='records'), indent=4))
            print "Output: %s" % fname
        os.system('mv %s %s' % (temp, fname))
    else:
        print "no data available"
if __name__ == '__main__':
    main()
