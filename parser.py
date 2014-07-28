#!/usr/bin/env python
# encoding: utf-8
"""
parser.py: 

Created by Peter Kalchgruber on 2014-02-10.
Copyright 2014,  All rights reserved.
"""

from util import dirurl
from filemanager import FileManager
import time
import logging
import os
from hashlib import md5
from compare import Compare
from filemanager import FileManager

logging.basicConfig(level = logging.DEBUG)

class Parser():
    def __init__(self, config):
        #self.fm = FileManager()
        self.config = config
        self.parsedlist = {}
        self.compare = Compare()
        self.fm = FileManager()
    
    def parse(self, resources):
        """
        parses each resource, calculates diff to existing resource on HD (dependent on lastdate)
        """
        for resource in resources:
            recent_file = self.getRecentLocalFile(resource.directory)
            if resource.diff:
                compresult = self.compare.compare(recent_file, resource.url)
                logging.debug("Comparing resources: %s and %s" % (recent_file, resource))
                result = {}
                result['url'] = resource.url
                result['diff'] = compresult.diff
                results[resource.url] = result
                print compresult


    def getRecentLocalFile(self, directory, date = None):
        """
        Return newest local file (dump) in directory e.g. data/orf.at 
        If dump with datestamp can be found this dump will be returned
        """
        try:
            logging.debug("Trying to alocate file near %s" % directory)
            files = reversed(sorted([ f for f in os.listdir(directory)]))
            lastfile = None
            for file in files:
                if "dump" in file:
                    if date !=  None: 
                        compstr = file.replace(".dump", "")
                        if float(compstr)<float(date):
                            if lastfile:
                                return "%s/%s" % (directory, lastfile) 
                            else:
                                return "%s/%s" % (directory, file) 
                        elif float(compstr) == float(date):
                            return "%s/%s" % (directory, file) 
                        elif float(compstr)>float(date):
                            pass
                            # return "%s/%s" % (directory, lastfile) 
                    else:
                        return "%s/%s" % (directory, file) #if date is None,  return last saved version of file
                    lastfile = file
        except OSError as e:
            logging.debug("%s seems like first time fetched" % e)
            return None
        except ValueError as e:
            logging.debug(e)
            return None



    def checkEquals(self, resourcelist, date=None):
        """
        compare duplicates with duplicates in storage
        """

        timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        output = {}
        output['data'] = {}
        output['data']['date'] = timestamp
        if date:
            output['data']['lastdate'] = date
        results = {}

        
        #debug
        # datasources = []
        # for domain, resourcelist in resourcelist.iteritems():
        #     datasources.append(self.fm.getAllByDomain(domain,  resourcelist))

        import pickle
        import sys
        # fh=open("rdump","w+")
        # pickle.dump(datasources,fh)
        # fh.close() 
        fh=open("rdump","r+")
        datasources=pickle.load(fh)
        fh.close()

   
    

        for datasource in datasources:
            md5list = {}    #list of md5 hashes of current datasource to avoid downloading alias URIs
            for resource in datasource:
                if  True or "sws.geonames.org" in resource.domain:
                    directory = dirurl(resource.url)
                    resource.directory = directory     
                    diff = False
                    #read content of file of same url of HD if existing
                    recent_file = self.fm.getRecentLocalFile(directory, date)
                    if recent_file:
                        logging.debug("recent_file found: %s" % recent_file)
                        rf_fh = open(recent_file)
                        rf_content = rf_fh.read()

                    
                    if resource.md5 in md5list:     #resource already downloaded in this job
                        logging.debug("Already downloaded in this job, creating symlink %s" % resource.url)
                        self.fm.symlink(os.path.join(md5list[resource.md5], "%s.dump" % timestamp), os.path.join(resource.directory, "%s.dump" % timestamp))
                    elif recent_file == os.path.join(resource.directory,"%s.dump" % timestamp):     #file exists on harddisk with same name (timedelta too small)
                        logging.debug("Downloaded in the minute before, skipping")
                    elif recent_file and md5(rf_content).digest() != resource.md5:  #recent file found on HD, but diffs
                            logging.debug("Recent version found, but diff detected, downloading file %s" % resource.url)
                            md5list[resource.md5] = directory
                            diff = True
                            self.fm.downloadFile(resource, timestamp)
                    elif recent_file and md5(rf_content).digest() == resource.md5:  #recent file located and proved that same size
                        logging.debug("File found on HD with same hash: %s at %s %s %s" % (resource.url, resource.directory, recent_file, timestamp))
                        if resource.directory == os.path.dirname(recent_file): #only create symlink if directory differs
                            logging.debug("File already exists in same directory, no symlink created")
                        else:
                            logging.debug("File is in different directory, symlink created %s" % resource.url)
                            self.fm.symlink(recent_file, resource.directory, timestamp)
                    else:   #no recent file found and file is not in md5list
                        logging.debug("No equals found, downloading file %s" % resource.url)
                        md5list[resource.md5] = directory
                        self.fm.downloadFile(resource, timestamp)

                    #start diff process
                    if diff:
                        compresult = self.compare.compare(recent_file, resource.url)
                        logging.debug("Comparing resources: %s and %s" % (recent_file, resource))
                        result = {}
                        result['url'] = resource.url
                        result['diff'] = compresult.diff
                        results[resource.url] = result
                        print compresult

        output['equals'] = results
        return output  


            # for resources in domain:
            #     #create filename
            #     directory = resource.url.replace("http://", "")
            #     directory = directory.replace("/", "_")            
            #     directory = "data/%s" % directory
            #     self.fm.downloadAndGetContent(resource, directory, timestamp)        

            # if resource.domain not in self.parsedlist:
            #     self.parsedlist[resource.domain] = []
            # result = {}
            # result['url'] = resource.url
            # #create filename
            # directory = resource.url.replace("http://", "")
            # directory = directory.replace("/", "_")            
            # directory = "data/%s" % directory

            # #get local file of resource with newest timestamp
            # file = self.fm.getRecentLocalFile(directory, date)
            
            # if date ==  None:
            #     logging.info("No date or recent file found")
            #     content = self.fm.downloadAndGetContent(resource, directory, timestamp)
            #     if content:
            #         resource.content = content
            #         result['sd'] = self.parse(resource, directory, timestamp)
            #         self.parsedlist[resource.domain].append(hashlib.md5(resource.content).digest()) #add resourcehash to parsed list
            #     else:
            #         logging.debug("no content,  already exists?")
            # elif date !=  None and file:
            #     logging.info("START COMPARING")
            #     content = self.fm.downloadAndGetContent(resource, directory, timestamp)
            #     if content:
            #         resource.content = content
            #         compresult = self.compare(file, resource.url)
            #         result['diff'] = compresult.diff
            #         if compresult.diff is not "equal":
            #             logging.info("Diff detected")
            #             if compresult.diff[1] == "text":
            #                 logging.debug("DIFF filebased")
            #             else:
            #                 logging.debug("DIFF graph")
            #             #     logging.debug("DIFF (filebased) %s" % compresult.diff[0])
            #             # else:
            #             #     logging.debug("DIFF (graph)%s" %  compresult.diff[0].serialize(format = "n3")) already jsoned in function
            #             #     logging.debug("DIFF (graph)%s" %  compresult.diff[1].serialize(format = "n3"))
            #             #check to only download if current file diffs with last downloaded file
                      
            #             #TODO ???
            #             # comp = self.compare(self.getRecentLocalFile(directory), resource.url)
            #             # if comp.diff !=  "equal":
            #             #     content = self.downloadFile(directory, timestamp, content = comp.fc2)
            #             #     result['sd'] = self.parse(compresult.fc2, resource.url, directory, timestamp, download = True)
            #             # else:
            #             #     result['sd'] = self.parse(compresult.fc2, resource.url, directory, timestamp)

            #         else:
            #             logging.info("No Diff @ %s" % resource.url)


            #         result['sd'] = self.parse(resource, directory, timestamp)
            #         self.parsedlist[resource.domain].append(hashlib.md5(resource.content).digest()) #add resourcehash to parsed list
            #     else:
            #         logging.debug("no content,  already exists?")
                
            # results[resource.url] = result


                
                
            
            
            



                # if "sd" in result and result['sd'] !=  None and 'population' in result['sd']:
                #     results[resource.url] = result
                #     visited.append(resource.domain)

        # output['equals'] = results
        # return output      