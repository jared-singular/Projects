#!/usr/bin/env python

"""
Author: Jared Ornstead
Version: 1.0
Date: 2018-04-24
Description: Script takes a CSV file with column headers, converts the data to
             a JSON dict, and replays the data as a JSON POST to the desired
             endpoint. Success and Failure files are created for reference.

Usage: python CSVtoJSON_POST_Replay_Script.py infile.csv
"""

import csv
import sys
import pprint
import requests
import json
import time
from tqdm import tqdm


# Function Creates Errors File
def writeErrorsToFile(w):
    out_file = "Log_CSVtoJSON_POST_RePlay_Errors.txt"
    with open(out_file, 'a') as f:
        f.write(w + '\n')
    return


# Function Creates Success File
# This function could be disabled to speed up script. See flag in MAIN
def writeSuccessToFile(w):
    out_file = "Log_CSVtoJSON_POST_RePlay_Success.txt"
    with open(out_file, 'a') as f:
        f.write(w + '\n')
    return


# Function to convert a csv file to a list of dictionaries.  Takes in one variable called "variables_file"
def csv_dict_list(variables_file):
    # Open variable-based csv, iterate over the rows and map values to a list of dictionaries containing key/value pairs
    reader = csv.DictReader(open(variables_file, 'rb'))
    dict_list = []
    for line in reader:
        dict_list.append(line)
    return dict_list


def replay(data):
    url = "http://www.example.com"
    return requests.request('POST', url=url, json=data)
    # headers = {'ApiKey': '5CwkYN3tGNthbFyHrdQFT0IYQTqPOyUG'}
    # return requests.request('POST', url=url, json=data, headers=headers)


def main():
    # Enable/Disable Success File
    successFile = True
    troubleshooting = False

    try:
        if (len(sys.argv) == 2 and sys.argv[1][-4:] == '.csv'):  # Check for input file
            ifile = sys.argv[1]
            counter = 0

            # Calls the csv_dict_list function, passing the named csv
            dataList = csv_dict_list(ifile)

            # Prints the results nice and pretty like
            # Use for troubleshooting
            if troubleshooting == True:
                pprint.pprint(dataList)

            for item in tqdm(dataList):
                res = replay(item)
                counter += 1
                if (successFile == True and res.status_code == 200):
                    line = res.status_code, res.request.body
                    writeSuccessToFile(str(line))
                elif res.status_code != 200:
                    line = res.status_code, res.request.body
                    writeErrorsToFile(str(line))

            print counter, " Successful Postbacks Sent!"
            sys.exit()
        else:
            print "Usage: python CSVtoJSON_POST_Replay_Script.py infile.csv"
            sys.exit()
    except Exception, e:
        writeErrorsToFile(str(e))
        sys.exit(e)


# --- Main ---
if __name__ == '__main__':
    main()