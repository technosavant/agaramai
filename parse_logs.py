#!/usr/bin/python
# -*- coding: utf-8 -*-

# parse_logs.py
# version 0.1 2016-10-26 (YYYY-MM-DD)
# author: chris@greenlock.com
# copyright 2016 Chris Buchanann
# all rights reserved


"""
This script is designed to parse some apache logs and spit out some results

Logs that were supplied with the exercise appear to conform to the apache commmon long standard

Each request seems to be of the format "/<x>/<client_id>/<y>/<image_name>" 
Where:
 x = first letter of client_id / top level directory
 client_id = unique ID for a client
 y = any number of subdirectories
 image_name = filename of the image being requested (if there is an image being requested)

This script makes use of generator objects to create iterable labels for each element in the logfile
and then uses the logfile itself as the data store.
"""

import sys, getopt
import os, fnmatch
import re
from datetime import datetime, timedelta
from collections import Counter




# ==========================
# ===== Utility consts =====
# ==========================

# Add some coloring for printing status lines
YELLOW = '\033[93m'
GREEN = '\033[92m'
END = '\033[0m'
RED = '\033[91m'

# According to http://httpd.apache.org/docs/1.3/logs.html
# The common format for apache log files is: %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\
apache_common_format = re.compile( 
    r"(?P<host>[\d\.]+)\s" 
    r"(?P<identd>\S*)\s" 
    r"(?P<user>\S*)\s"
    r"\[(?P<time>.*?)\]\s"
    r'"(?P<request>.*?)"\s'
    r"(?P<status>\d+)\s"
    r"(?P<bytes>\S*)\s"
    r'"(?P<referer>.*?)"\s' 
    r'"(?P<user_agent>.*?)"\s*' 
)

# constant for 5 minutes in seconds 
_5minutes_ = 5 * 60 

# =============================
# ===== Utility Functions =====
# =============================

def log_find(filepattern, base_dir):
    """
    Recursively find all filenames that match filepattern from base_dir
    """
    filenames = []
    for path, dirlist, filelist in os.walk(base_dir):
        for name in fnmatch.filter(filelist, filepattern):
            filenames.append(os.path.join(path, name))
    return filenames
    
def gen_open(filenames):
    """
    Generator function to yield a sequence of open file handles from input filenames
    """
    for name in filenames:
        yield open(name)

def gen_cat(sources):
    """
    Generator function to concatenate together a sequence of other generators
    """
    for s in sources:
        for item in s:
            yield item

def gen_grep(pattern, lines):
    """
    Generator function to match a pattern against a sequence of lines
    """
    grep = re.compile(pattern)
    for line in lines:
        if grep.search(line): yield line

def has_image(request):
    """
    Returns boolean result of whether or not there is an image file in the request
    """
    exts = ['jpg', 'png', 'gif']
    return any((ext in '%s' % request) for ext in exts)

def fetch(hit):
    """
    Returns boolean result of whether or not this was a successful image fetch

    A successful fetch is a GET that results in a 2xx result code for 
    request that has an image
    """
    hit["status"] = int(hit["status"])
    if hit["status"] < 200 or hit["status"] >= 300:
        return False

    if "GET" not in hit["request"]:
        return False 

    if not has_image(hit["request"]):
        return False

    return True


def gen_datastore(filepattern, base_dir):
    """
    Generator function to iterate over the ENTIRE datastore - one line at a time as fast as possible

    Returns one log line of the datastore - not yet fixed up - call fix_line_dict for that
    """
    filenames = log_find(filepattern, base_dir)
    files = gen_open(filenames)
    lines = gen_cat(files)
    return lines


def fix_line_dict(logline):
    """
    Takes a raw log line and returns a cleaned up dictionary of python correct datatypes
    """
    match = apache_common_format.match(logline)
    linedict = match.groupdict()
    # If size is a dash, make it 0
    if linedict["bytes"] == "-":
        linedict["bytes"] = 0
    else:
        linedict["bytes"] = int(linedict["bytes"]) 

    # Convert the timestamp into a datetime object  
    # TODO: this doesn't handle timezones or DST
    time, zone = linedict["time"].split()
    linedict["time"] = datetime.strptime(time, "%d/%b/%Y:%H:%M:%S")

    # if the referer, user, or user-agent is a dash, make it None
    for t in ("user", "user_agent", "referer"):
        if linedict[t] == "-":
            linedict[t] = None 

    # We only really care about two parts of the request line, the image filename and the client_id
    # The image filename is always the last position and the client_id is always index 2
    # TODO: This is brittle - if the directory structure changes, this will break
    image_path = linedict["request"].split()[1]  # move past the HTTP access command
    split_path = image_path.split("/")           # break out the pieces we want
    linedict["image_filename"] = split_path[-1]  
    linedict["client_id"] = split_path[2]

    return linedict


# =========================
# === Main Program Loop ===
# =========================

def main(argv):
    
    logpath = "."
    logpattern = "access*"
    try:
        opts, args = getopt.getopt(argv,"hl:p:",["help", "logpath=", "file_pattern="])
    except getopt.GetoptError:
        print RED + 'Usage: parse_logs.py -l <logpath="."> -p <file_pattern="access*">' + END
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print GREEN + 'Usage: parse_logs.py -l <logpath="."> -p <file_pattern="access*">' + END
            sys.exit()
        elif opt in ("-l", "--logpath"):
            logpath = arg
        elif opt in ("-p", "--file_pattern"):
            filepattern = arg

    records = []

    # datastore generator
    pages = gen_datastore(logpattern, logpath)
    for page in pages: 
        record = fix_line_dict(page)
        if fetch(record):
            records.append(record)

    # find our customer IDs
    customers = set(r["client_id"] for r in records)

    # for each customer, tell me how many hits and the total number of bytes
    # also for each customer - tell us how many unique IPs have fetched images per day
    for customer in customers:
        # the total number of fetches for this customer
        hitlist = Counter(x["request"] for x in records if (x["client_id"] == customer))
        hits = len(list(hitlist.elements()))
        # the total number of bytes for this customer
        total_bytes = sum(x['bytes'] for x in records if (x["client_id"] == customer))
        print YELLOW + "For customer: " + GREEN + customer + YELLOW + " there were " + RED + \
            "%d hits" %hits + YELLOW + " for a total of " + RED + "%d bytes." %total_bytes + END

        # the nubmer of unique visits for this customer on a given date
        dates = Counter(y["time"] for y in records if y["client_id"] == customer)
        dates = list(dates.elements())
        calendar = []
        for date in dates:
            calendar.append(date.date())
        calendar = set(calendar)
        for date in calendar:
            unique_ips = set(r["host"] for r in records if ((r["client_id"] == customer) and (r["time"].date() == date)))
            visits = len(unique_ips)
            print "For customer: " + YELLOW + customer + GREEN + " there were %d unique visitors on %s." % (visits, str(date)) + END

    # We also need to find the busiest 5 minute interval
    high_hits = 0
    high_bytes = 0
    timeset = set(r["time"] for r in records)
    mintime = min(timeset)
    maxtime = max(timeset)

    bytestime = hitstime = time = mintime 
    while time <= maxtime:
        bytes_test = sum(x["bytes"] for x in records if (x["time"] <= time))
        if bytes_test > high_bytes:
            high_bytes = bytes_test
            bytes_time = time

        hits_test = len(Counter(z["request"] for z in records if (z["time"] <= time)))
        if hits_test > high_hits:
            high_hits = hits_test
            hits_time = time

        time = time + timedelta(0, _5minutes_)
        
    print YELLOW + "The 5 minute interval with the most hits started at: " + GREEN + "%s " %hits_time + YELLOW + \
        "\nand there were %d successful image fetches." %high_hits + END 
    print YELLOW + "The 5 minute interval with the most bytes transfered started at: " + GREEN + "%s " %bytes_time + YELLOW + \
        "\nand there were %d bytes transfered." %high_bytes + END


if __name__ == "__main__":
   main(sys.argv[1:])







