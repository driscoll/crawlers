# -*- coding: utf-8 -*-
""" 
Given the URL of a pipermail archive
Crawl the archive, extract all URLs, and output a CSV

TODO
    Add argparse to later support more functions
    Format this like a submodule 

Kevin Driscoll, 2013

"""

import BeautifulSoup as bs
import csv
import datetime
import requests
import subprocess
import sys
import time


FIRSTLINE_RE = bs.re.compile('^From .* at .* \d\d:\d\d:\d\d \d{4}$')
# INSANE URI matching regex from:
# http://daringfireball.net/2010/07/improved_regex_for_matching_urls
URL_RE = bs.re.compile(r'(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?��....]))')


def parse_first_line(line):
    email, timestamp = line.strip()[5:].split('  ', 1)
    if timestamp[8] == ' ': 
        timestamp = timestamp[:8] + '0' + timestamp[9:]
    dt = datetime.datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    dirty = email.replace(' at ', '@')
    return dirty, dt

def iter_emails(f):
    line = ''
    while not FIRSTLINE_RE.match(line):
        line = f.readline()
    email, dt = parse_first_line(line)
    text = ''
    subject = ''
    for line in f:
        if FIRSTLINE_RE.match(line):
            yield (email, dt, subject, text)
            text = ''
            email, dt = parse_first_line(line)
        elif line.startswith('Subject: '):
            subject = line[9:].strip()
        else:
            text += line
    yield (email, dt, subject, text)


if __name__=="__main__":

    if len(sys.argv) < 2:
        print "Please specify a pipermail archive URL."
        sys.exit(1)
    else:
        piperurl = sys.argv[1]

    print "Requesting", piperurl
    r = requests.get(piperurl)
    if r.ok:
        soup = bs.BeautifulSoup(r.text)
    else:
        print "Error code", r.status_code
        sys.exit(1)

    listname = soup.find('h1').text
    print "Found list named:", listname

    gzips = []
    for row in soup.findAll('tr')[1:]:
        gzips.append(row.findAll('td')[2].find('a')['href'])
    print "Found", len(gzips), "months of archives."

    rows = [('url', 'subject', 'from', 'timestamp')]
    for fn in gzips:
        print "Downloading", fn
        r = requests.get(piperurl + fn)
        with open(fn, 'w') as fp:
            for chunk in r.iter_content(1024):
                fp.write(chunk)
        subprocess.check_call(['gunzip', '-fqk', fn])
        fn = fn.rsplit('.', 1)[0]
        with open(fn, 'r') as fp:
            count = 0
            for email, dt, subject, text in iter_emails(fp):
                for u in URL_RE.finditer(text):
                    url = u.group()
                    rows.append((url, subject, email, dt.isoformat()))
                    count += 1
            print "Found", count, "urls."
        print "Sleeping for 10s..."
        time.sleep(10)

    print "Found", len(rows[1:]), "in total."

    output_fn = '{0}_links.csv'.format(listname.replace(' ', '_'))
    print "Writing to", output_fn
    with open(output_fn, 'w') as f:
        csvw = csv.writer(f, dialect="excel")
        csvw.writerows(rows)
    print "Bye-bye!"





            









