#!/usr/bin/env python

import requests

print "Enter iTunes store AppID: "
appID = raw_input()
# appID = '1141145524'
# Set the request parameters
url = 'https://itunes.apple.com/lookup?id=' + str(appID)
# Fetch url
print "Fetching data from: " + url
# Do the HTTP get request
response = requests.get(url, verify=True)  # Verify is check SSL certificate
# Error handling
# Check for HTTP codes other than 200
if response.status_code != 200:
    print('Status:', response.status_code, 'Problem with the request. Exiting.')
    exit()
# Decode the JSON response into a dictionary and use the data
data = response.json()
results = data['results']
for item in results:
    appReleaseDate = str(item[u'releaseDate'])
    appLatestRelease = str(item[u'currentVersionReleaseDate'])
    appName = str(item[u'trackName'])
    appGenres = str(item[u'genres'])
    appBundleID = str(item[u'bundleId'])
    appStoreURL = str(item[u'trackViewUrl'])
    print "\niTunes App Details: "
    print "App Release Date: " + appReleaseDate
    print "App Latest Release: " + appLatestRelease
    print "App Name: " + appName
    print "App Genre(s): " + appGenres
    print "App BundleID: " + appBundleID
    print "App StoreURL: " + appStoreURL
