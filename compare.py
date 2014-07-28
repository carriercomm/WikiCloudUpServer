#!/usr/bin/env python
# encoding: utf-8
"""
compare.py: 

Created by Peter Kalchgruber on 2014-02-10.
Copyright 2014,  All rights reserved.
"""

from xml.sax import SAXParseException
import logging
import rdflib
from rdflib.compare import to_isomorphic,  graph_diff
import urllib, urllib2, json
import simplejson
import difflib
import re
import operator
import xml.etree.ElementTree as ET
from util import dump_nt_sorted
logging.basicConfig(level = logging.DEBUG)

          
class HTMLException(Exception):
    def __init__(self,  value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class DiffResult():
    """
    Result of compare operation between two resources
    """
    def __init__(self, file1 = None, file2 = None, diff_text = ""):
        self.file1 = file1
        self.file2 = file2
        self.diff_text = diff_text
        self.diff = False
        
    def __str__(self):
            return "file1:%s file2:%s,  diff:%s: %s" % (self.file1, self.file2, self.diff, self.diff_text)
            
    def __repr__(self):
            return "file1:%s file2:%s,  diff:%s: %s" % (self.file1, self.file2, self.diff, self.diff_text)


class Compare():

    def compare(self, file1, file2):
        """
        Compares to files,  file2 can be a file or url. If files are rdf graphs,  rdflib.graph_diff will be downloadFile
        otherwise difflib.unified_diff will be computed and returned
        """
        try:
            result = DiffResult(file1 = file1, file2 = file2)
            logging.info("compare file1: %s,  file2: %s" % (file1,  file2))
            graph1 = rdflib.Graph()
            graph2 = rdflib.Graph()
            graph1.parse(file1)
            logging.debug("graph1 %s parsed" % file1)
            if "http" in file2:
                reg = urllib2.Request(file2,  None,  {"ACCEPT":"application/rdf+xml"})
                uh = urllib2.urlopen(reg)
                content = uh.read()
                result.fc2 = content
                if "<html" in content:
                    raise HTMLException
                graph2.parse(data = content, format = 'xml')
            else:
                graph2.parse(file2)
            logging.debug("graph2 %s parsed" % file2)
            logging.info("Graphes parsed,  Starting isomorphication")
            iso_graph1 = to_isomorphic(graph1)
            iso_graph2 = to_isomorphic(graph2)
            if iso_graph1 == iso_graph2:
                result.diff = False
                return result

            logging.debug("Starting Graph Diff")
            in_both,  in1,  in2 = graph_diff(iso_graph1, iso_graph2)
            result.diff = True
            result.diff_text = "%s ||SEPARATOR|| %s" % (dump_nt_sorted(in1), dump_nt_sorted(in2)) #[in1, in2] #TODO convert graphdiff to text
            return result
        except TypeError as ex:
            logging.error("TypeError %s" % ex)
            reg = urllib2.Request(file2,  None,  {"ACCEPT":"application/rdf+xml"})
            uh = urllib2.urlopen(reg)
            result.fc2 = uh.read()
        except SAXParseException as ex:
            logging.error("SAXParseException %s" % ex)
            reg = urllib2.Request(file2,  None,  {"ACCEPT":"application/rdf+xml"})
            uh = urllib2.urlopen(reg)
            result.fc2 = uh.read()
        except HTMLException as ex:
            logging.debug("HTML-Exception")
            pass


        # graph comparisation failed or html-tag was detected
        # use difflib.unified to compute diff
        diff = difflib.unified_diff(open(file1).readlines(),  result.fc2.splitlines(1), n = 0)
        diffs = ""
   
        for line in diff:
            diffs+= line
            
            
        if len(diffs)>0:
            result.diff = True
            result.diff_text = diffs
            return result
        else:
            result.diff = False
            return result

  






    def parse(self, resource, directory, timestamp):
        """
        parse and return structured diff
        """

        if not self.parsed(resource) ==  True:   
            struc = ""
            if "geonames" in resource.url:
                struc = self.parse_geonames(resource.content)
                pass
            elif "freebase" in resource.url:
                struc = self.parse_freebase(resource.url)
            else:
                logging.debug("No Parsing Module for domain %s installed" % resource.domain)
            # filename = "%s/%s.rdf" % (directory, timestamp)
            # if download:
            #     fh = open(filename, "wb")
            #     fh.write(json.dumps(struc))
            #     fh.close()
            return struc
        else:
            logging.debug("Not parsed,  since already in parsed list: %s" % resource.url)
            return None

    def parse_geonames(self, content):
        """
        parse geonames content and return population
        """
        try:
            #read xml tree
            g = rdflib.Graph()
            g.parse(data = content)
            gn = rdflib.Namespace("http://www.geonames.org/ontology#")
            for s, p, o in g.triples((None, gn['population'], None)):
                logging.debug("Result of geonames parsing: %s" % o)
                struc = {}
                struc['population'] = o
                return struc
            logging.debug("No parsing result at geonames")
        except SAXParseException as e:
            logging.error("saxparseexception %s" % e)
        return None

    def parse_freebase(self, url):
        """
        parse freebase api (with url-modification) and return population
        """

        
        match = re.search(".*(\/.\.[a-zA-Z0-9\._]*)", url)
        topic_id = None
        if match:
            topic_id = match.group(1).replace(".", "/")
        else:
            logging.debug("no topicid found in url")
            return None

        api_key = self.config['freebaseAPIkey']
        service_url = self.config['freebaseServiceurl']

        params = {
            'key': api_key, 
            'filter': '/location/statistical_region/population', 
            'limit': 500
        }
        url = service_url + topic_id + '?' + urllib.urlencode(params)
        logging.info("Parsing Freebase API with URL: %s" % url)
        topic = json.loads(urllib.urlopen(url).read())
        values = {}
        try:
            for value in topic['property']['/location/statistical_region/population']['values']:
                number = value['property']['/measurement_unit/dated_integer/number']['values']
                number = number.pop()
                output = ""
                if '/measurement_unit/dated_integer/year' in value['property']:
                    date = value['property']['/measurement_unit/dated_integer/year']['values']
                    date = date.pop()
                    values[date['value']] = number['value']
                    content  =  sorted(values.iteritems(),  key = operator.itemgetter(0), reverse = True)
                    output = "%s;%s" % (content[0][0], content[0][1])
                    logging.debug("Result of Freebase parsing: %s" % output)
                    output = int(content[0][1])
                else:
                    output = int(number['value'])
                    logging.debug("Result of Freebase parsing: %s" % output)
                struc = {}
                struc['population'] = output
                return struc

        except KeyError, e:
            logging.debug("Key error at parsing freebase %s" % e)

        return None



    def to_XMLtree(self, g):
        """
        Pass in a rdflib.Graph and get back a XMLtree
        """
        json = {}
        # go through all the triples in the graph
        for s,  p,  o in g:
            # initialize property dictionary if we've got a new subject
            if not json.has_key(s):
                json[s] = {}
            # initialize object list if we've got a new subject-property combo
            if not json[s].has_key(p):
                json[s][p] = []
            # determine the value dictionary for the object 
            v = {'value': unicode(o)}
            if isinstance(o,  rdflib.URIRef):
                v['type'] = 'uri'
            elif isinstance(o,  rdflib.BNode):
                v['type'] = 'bnode'
            elif isinstance(o,  rdflib.Literal):
                v['type'] = 'literal'
                if o.language:
                    v['lang'] = o.language
                if o.datatype:
                    v['datatype'] = unicode(o.datatype)
            # add the triple
            json[s][p].append(v)
        diff = ET.Element("diff")    
        for s in json[s]:
            element = ET.Element(s)
            for p in s:
                for o in p:
                    element.set(p,o)
            diff.append(element)
        return diff
                
        
    def json_for_graph(self, g):
        """
        Pass in a rdflib.Graph and get back a chunk of JSON using 
        the Talis JSON serialization for RDF:
        http://n2.talis.com/wiki/RDF_JSON_Specification
        """
        json = {}
        # go through all the triples in the graph
        for s,  p,  o in g:
            # initialize property dictionary if we've got a new subject
            if not json.has_key(s):
                json[s] = {}
            # initialize object list if we've got a new subject-property combo
            if not json[s].has_key(p):
                json[s][p] = []
            # determine the value dictionary for the object 
            v = {'value': unicode(o)}
            if isinstance(o,  rdflib.URIRef):
                v['type'] = 'uri'
            elif isinstance(o,  rdflib.BNode):
                v['type'] = 'bnode'
            elif isinstance(o,  rdflib.Literal):
                v['type'] = 'literal'
                if o.language:
                    v['lang'] = o.language
                if o.datatype:
                    v['datatype'] = unicode(o.datatype)
            # add the triple
            json[s][p].append(v)
        return simplejson.dumps(json,  indent = 2)



def main():
    compare = Compare()
    compresult = compare.compare("data/rdf.freebase.com_ns_m.06v1vd/20140704114741.dump", "http://rdf.freebase.com/ns/m.06v1vd")
    print compresult

    
if __name__ ==  "__main__":
    main() 



