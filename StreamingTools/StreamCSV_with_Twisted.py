#!/usr/bin/env python
"""
Author: Jared Ornstead
Version: 1.0
Date: 2018-08-24
Description: Takes a CSV file with 2 columns of data. Column 1 is the URL to process and column 2 is the
  User Agent to send in the header of the request. Success/Error requests will be logged in output files.
Version Notes: Could be optimized with Twisted. Not sure how yet.
Usage: python StreamCSV_with_Twisted.py infile.csv
"""
#
from twisted.internet import defer
from twisted.internet.threads import deferToThread
from twisted.internet import reactor
import csv
import sys
import requests

ifile = ''
outfile_errors = ''
outfile_success = ''


def makeAPICall(x):
    print 'make api call'
    url = x[0]
    user_agent_string = x[1]
    print user_agent_string
    headers = {'User-Agent': user_agent_string}
    r = requests.get(url, headers=headers)
    try:
        w = (str(r.status_code) + "|" + url + "|" + user_agent_string)
        if (r.status_code == 200):
            writeSuccessToFile(w)
        else:
            writeErrorsToFile(w)
        return
    except Exception, err:
        print err
    return


def writeErrorsToFile(w):
    global outfile_errors
    with open(outfile_errors, 'a') as f:
        f.write(w + '\n')
    return


def writeSuccessToFile(w):
    print 'writeSuccessToFile'
    global outfile_success
    with open(outfile_success, 'a') as f:
        f.write(w + '\n')
    return


def print_row_list(cb):
    print "Finished: ", cb


def _deferred(row_list):
    print '_deferred'
    def errBack(eb):
        print "Error: ", eb
        writeErrorsToFile(eb)
        return

    d = deferToThread(makeAPICall, row_list)
    d.addCallback(print_row_list)
    d.addErrback(errBack)
    return d


def stop_reactor(self, args=None):
    print 'stop_reactor'
    if reactor.running:
        print 'bye'
        reactor.stop()


def main():
    if (len(sys.argv) == 2 and sys.argv[1][-4:] == ".csv"):  # Check for input file
        global ifile
        ifile = sys.argv[1]
        global outfile_errors
        outfile_errors = sys.argv[1][:-4] + "_errors.txt"
        global outfile_success
        outfile_success = sys.argv[1][:-4] + "_success.txt"

        with open(ifile, 'rb') as f:
            reader = csv.reader(f, skipinitialspace=True, delimiter='|', quoting=csv.QUOTE_ALL)
            try:
                sem = defer.DeferredSemaphore(500)
                deferreds = []
                counter = 0
                for row in reader:
                    print "Counter: ",counter
                    row_list = []
                    row_list = list(row)
                    print "Row List: ",row_list
                    deferreds.append(sem.run(_deferred, row_list))
                    counter += 1
                #print "hello"
                d = defer.gatherResults(deferreds)
                d.addCallback(stop_reactor)
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (ifile, reader.line_num, e))

    else:
        print "Usage: python StreamCSV_with_Twisted.py infile.csv"
        sys.exit()


if __name__ == '__main__':
    main()
    reactor.run()
