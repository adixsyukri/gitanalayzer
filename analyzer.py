#!/usr/bin/env python

"""
Git statistic analyzer
"""

import argparse
import os
import subprocess
import json
from datetime import datetime

def now():
    """
    return current date time preformatted
    """
    return datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

def format_date(date_time):
    """
    convert human readable date to datetime
    """
    return datetime.strptime(date_time[:-6], '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')

def commit_stats(extracted):
    """
    return git diff information with diff stats info
    """
    total, values = extracted
    final = []
    result = []
    for row in values:
        stats = subprocess.check_output('git diff --numstat %s' % row['commit'], shell=True)
        row['stats'] = []
        for info in stats.split('\n'):
            splitted = info.split()
            if splitted:
                row['stats'].append({
                    'addition': splitted[0],
                    'deletion': splitted[1],
                    'file': splitted[2]
                })
        result.append(row)
    final.append({'commits': total, 'stats': result})
    return final

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
    splitted = val.split('\n')
    for i, row in enumerate(splitted):
        if 'commit' in row:
            output.append({
                'commit': extract_by_key('commit', splitted[i]),
                'author': extract_by_key('Author:', splitted[i+1]),
                'date': format_date(extract_by_key('Date:', splitted[i+2]))
            })

    return (len(output), output)

def main():
    """
    Main Function execution
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseurl', dest='baseurl', required=True)
    parser.add_argument('--repo', dest='repo', required=True)
    args = parser.parse_args()
    if not os.path.exists(args.repo):
        os.system('git clone %s/%s' % (args.baseurl, args.repo))
    os.chdir(args.repo)
    os.system('git pull -r origin master')
    output = subprocess.check_output('git log', shell=True)
    extracted = extract_info(output)
    result = commit_stats(extracted)
    os.chdir("..")
    fname = '%s-repo-stats-%s.json' % (args.repo, now())
    with open(fname, 'w') as f:
        f.write(json.dumps(result, indent=4))
        print "Output: %s" % fname

if __name__ == '__main__':
    main()
