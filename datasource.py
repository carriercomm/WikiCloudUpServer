#!/usr/bin/env python
# encoding: utf-8
"""
datasource.py: 

Created by Peter Kalchgruber on 2014-02-28.
Copyright 2014, All rights reserved.
"""

from util import formatUrl
import logging
from hashlib import md5
import os
import time
from compare import Compare
import json
import xml.etree.ElementTree as ET

class Datasource:
    """
    """

    def __init__(self, domain, lastdate=None):
        self.resources  = []
        self.domain = domain
        self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self.complete = 0
        self.diff = False
        self.lastdate = lastdate

    def update(self):
        """
        Proof if online representation is newer or diffs from last downloaded file
        If yes, download new version (or make symlink to already downloaded newer file), otherwise do nothing
        """

        
        logging.debug("Updating all resources of datasource: %s" % self.domain)
        finished = {}    #list of md5 hashes of current datasource to avoid downloading alias URIs
        compare = Compare()
        for resource in self.resources:
            logging.debug("Updating Resource %s " % resource)
            resource.getBody()
            resource.timestamp = self.timestamp
            resource.directory = formatUrl(resource.url)     

            if resource.online:
                #read content of file of same url of HD if existing
                recent_file = self.getRecentLocalFile(resource.directory, self.lastdate)
                if recent_file:
                    logging.debug("recent_file found: %s" % recent_file)
                    rf_fh = open(recent_file)
                    rf_content = rf_fh.read()
                    rf_fh.close()


                if resource.md5 in finished:     #resource already downloaded in this job
                    logging.debug("Already downloaded in this job, check if symlink needed %s" % resource.url)
                    if recent_file and resource.directory == os.path.dirname(recent_file): #only create symlink if directory differs
                        logging.debug("File already exists in same directory, no symlink created")
                    else:
                        logging.debug("File is in different directory, symlink created1 %s" % resource.url)
                        self.symlink(os.path.join(finished[resource.md5]), os.path.join(resource.directory, "%s.dump" % self.timestamp))
                    resource.isDuplicate = True
                elif recent_file == os.path.join(resource.directory,"%s.dump" % self.timestamp):     #file exists on harddisk with same name (timedelta too small)
                    logging.debug("Downloaded just a few moments ago skipping")
                elif recent_file and md5(rf_content).digest() != resource.md5:  #recent file found on HD, but diffs
                        logging.debug("Recent version found, but diff detected, downloading file %s" % resource.url)
                        resource.diff = True
                        resource.save()
                        finished[resource.md5] = recent_file  
                elif recent_file and md5(rf_content).digest() == resource.md5:  #recent file located and proved that same hash
                    finished[resource.md5] = recent_file  
                    logging.debug("File found on HD with same hash: %s at %s %s %s" % (resource.url, resource.directory, recent_file, self.timestamp))
                    if resource.directory == os.path.dirname(recent_file): #only create symlink if directory differs
                        logging.debug("File already exists in same directory, no symlink created")
                    else:
                        logging.debug("File is in different directory, symlink created %s" % resource.url)
                        self.symlink(recent_file, resource.directory)

                else:   #no recent file found and file is not in finished
                    logging.debug("No equals found, downloading file %s" % resource.url)
                    finished[resource.md5] = resource.getPath()
                    resource.save()

                if resource.diff:
                    compresult = compare.compare(recent_file, resource.url)
                    resource.diffbody = compresult
                    logging.debug("Comparing resources: %s and %s" % (compresult.file1, compresult.file2))
                
            

        self.complete = 1
        return self.resources

    def getRecentLocalFile(self, directory, last):
        """
        Return String filepath of newest local file (dump) in directory e.g. data/orf.at 
        If dump with datestamp can be found this dump will be returned
        """
        try:
            logging.debug("Trying to alocate file near %s" % directory)
            files = reversed(sorted([ f for f in os.listdir(directory)]))
            lastfile = None
            for file in files:
                if "dump" in file:
                    if last !=  None: 
                        compstr = file.replace(".dump", "")
                        print file
                        if float(compstr)<float(last):
                            return "%s/%s" % (directory, file) 
                        elif float(compstr) == float(last):
                            return "%s/%s" % (directory, file) 
                        elif float(compstr)>float(last):
                            pass
                            # return "%s/%s" % (directory, lastfile) 
                    else:
                        return "%s/%s" % (directory, file) #if date is None,  return last saved version of file
                    lastfile = file
            if lastfile is not None:
                return "%s/%s" % (directory, lastfile)
            return None
                    
        except OSError as e:
            logging.debug("%s seems like first time fetched" % e)
            return None
        except ValueError as e:
            logging.debug(e)
            return None

    
    def symlink(self, source, link):
        try:
            if not os.path.exists(os.path.dirname(link)):
                os.makedirs(os.path.dirname(link))
        except OSError:
            pass
        os.symlink("../../%s" % source, link)

    def __repr__(self):
        output = ""
        for resource in self.resources:
            output += "%s, " % resource
        return output

    def to_JSON(self):
        rs=[]
        for resource in self.resources:
            r={}
            r['domain'] = resource.domain
            r['diffbody'] = resource.diffbody
            r['sd'] = resource.sd
            rs.append(r)
        return json.dumps(rs)

    def to_XMLtree(self):
        xdatasource = ET.Element("datasource")
        xdatasource.set("domain", self.domain)
        xdatasource.set("timestamp", self.timestamp)
        xdatasource.set("complete", "yes" if self.complete == 1 else "no")
        for resource in self.resources:
            if resource.online:
                xresource = ET.Element("resource")
                xresource.set("url", resource.url)
                xresource.set("duplicate", "yes" if resource.isDuplicate else "no" )
                if resource.sd:
                    xsd = ET.Element("data")
                    for data, value in resource.sd.iteritems():
                        xdata = ET.Element(data)
                        xdata.text = "%s" % value
                        xsd.append(xdata)
                    xresource.append(xsd)
                xresource.set("update", "yes" if resource.diff else "no")
                if resource.diffbody:
                    xresource.set("diffbody", resource.diffbody.diff_text.decode("utf-8"))
                    self.diff = True
                xdatasource.append(xresource)
            xdatasource.set("diff", "yes" if self.diff else "no")
        return xdatasource   