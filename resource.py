#!/usr/bin/env python
# encoding: utf-8
"""
resource.py: 

Created by Peter Kalchgruber on 2014-02-28.
Copyright 2014, All rights reserved.
"""

from urlparse import urlparse
from hashlib import md5
import util
import logging
import os
import urllib2

class Resource():
    def __init__(self, url):
        self.url = url
        o = urlparse(url)
        self.domain = o.netloc
        self.directory = util.formatUrl(self.url) #directory where resource is located at harddisk
        self.md5 = None
        self.length = 0
        self.diff = False #is resource unequal to newest online version?
        self.timestamp = None
        self.sd = [] #structured data
        self.diffbody = None
        self.isDuplicate = False
        self.online = True

    def save(self):
        """
        save resource to harddisk at path directory with name timestamp.
        e.g. sws.geonames.org_2770369/20140610141658.dump
        """

        try:
            if not os.path.exists(self.directory):
                os.makedirs(self.directory)
        except OSError:
            pass
        
        with open("%s/%s.dump" % (self.directory, self.timestamp),  "wb") as file:
            file.write(self.content)
            logging.debug("Downloaded %s to %s/%s" % (self.url, self.directory, self.timestamp))

    def getPath(self):
        """
        """
        return "%s/%s.dump" % (self.directory, self.timestamp)

    def getBody(self):
        """
        get body (content) of resource

        """
        logging.debug("allocating body of resource %s " % self.url)
        try:
            reg = urllib2.Request(self.url,  None,  {"ACCEPT":"application/rdf+xml"})
            uh = urllib2.urlopen(reg)
            self.content = uh.read()
            self.md5 = md5(self.content).digest()
            self.length = len(self.content)
            self.sd = self.parse()
            return self.content
        except urllib2.URLError, err:
            logging.debug(err)
            self.online = False
        except urllib2.HTTPError, err:
            logging.debug(err)
            self.online = False
        except UnicodeError as ex:
            logging.debug(ex)
            self.online = False
        except:
            self.online = False
            logging.error("something went wrong at getBody")
        return None

    def parse(self):
        """
        parse and return structured diff
        """
        return util.parse(self)
      

   


    def __repr__(self):
        #return "%s" % (self.url)
        return "%s, %s, %s" % (self.url, self.domain, self.length)
