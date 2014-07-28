#!/usr/bin/env python
# encoding: utf-8
"""
base.py: 

Created by Peter Kalchgruber on 2014-02-10.
Copyright 2014,  All rights reserved.
"""

import threading
import time 
import json
import urllib2

from resource import Resource
from datasource import Datasource


class Base(threading.Thread):
    """ A Base handles search of equals, retrieval of equals, and managing of datasources
    """

    def __init__(self, config, article, lastdate=None, callback=None):
        super(Base, self).__init__()
        self.callback = callback
        self.config = config['resources']
        self.article = article
        self.resources = []
        self.datasources = {}
        self.total = 0
        self.done = 0
        self.completed = 0
        self.lastdate = lastdate
        self.startdate = int(time.time())
        self._stop = threading.Event() #to stop thread

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def live(self):
        """
        return already fetched resources of request
        """
        return self.datasources

    def run(self):
        """
        Get doublicates of article of sameas webservice
        create datasources with resources
        updates resources (download content, save content to disk if it is new or was updated)
        """

        self.done = 0
        directoryBaseURL = self.config['directoryURL']
        dbPediaURL = self.config['dbPediaURL']
        directoryURL = "%s%s%s" % (directoryBaseURL, dbPediaURL, self.article)
        page = json.load(urllib2.urlopen(directoryURL))
        duplicates = page[0]["duplicates"]
        self.total = len(duplicates)        
                
        # create resources and append resources to datasources
        for url in duplicates:
            #DEBUG only list freebase and geonames
            if True or "freebase" in url or "geonames" in url:
                resource = Resource(url)
                if resource.domain not in self.datasources:
                    datasource = Datasource(resource.domain, self.lastdate)
                    self.datasources[resource.domain] = datasource
                datasource.resources.append(resource)
            

        # update datasources, dublicate detection, creation of json
        for domain, datasource in self.datasources.iteritems():
            if not self._stop.is_set(): #do not proceed if stop is set
                datasource.update()
                self.done += 1

        self.completed = 1
        self.callback(self.datasources)
