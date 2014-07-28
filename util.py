#!/usr/bin/env python
# encoding: utf-8
"""
util.py: 

Created by Peter Kalchgruber on 2014-02-28.
Copyright 2014, All rights reserved.
"""

import re
import yaml
import operator
from xml.sax import SAXParseException
import logging
import rdflib
import urllib, json
import xml.etree.ElementTree as ET

def datasources_to_XML(datasourcelist):
    header = '<?xml version="1.0"?>'
    ET.register_namespace('',"http://mis.cs.univie.ac.at/pka")
    root = ET.Element("response")
    meta = ET.Element("metadata")

    datasources = ET.Element("datasources")
    counter = 0
    for domain, datasource in datasourcelist.iteritems():
        if True or datasource.diff: #only reply with updated datasources 
            datasource = datasource.to_XMLtree()
            datasources.append(datasource)
            counter += 1
    total = ET.Element("totalnumber")
    total.text = "%s" % counter
    meta.append(total)
    root.append(meta)
    root.append(datasources)
    return "%s%s" % (header,ET.tostring(root))

def formatUrl(url):
	directory = url.replace("http://", "")
	directory = directory.replace("/", "_")            
	return "data/%s" % directory


def parse(resource):
    """
    parse and return structured diff
    """
    logging.debug("parse resource %s " %resource)
    struc = None
    if "geonames" in resource.url:
        struc = parse_geonames(resource)
        pass
    elif "freebase" in resource.url:
        struc = parse_freebase(resource)
    else:
        struc = None
        logging.debug("No Parsing Module for domain %s installed" % resource.domain)
    # filename = "%s/%s.rdf" % (directory, timestamp)
    # if download:
    #     fh = open(filename, "wb")
    #     fh.write(json.dumps(struc))
    #     fh.close()
    return struc


def parse_geonames(resource):
    """
    parse geonames content and return population
    """
    try:
        #read xml tree
        g = rdflib.Graph()
        g.parse(data = resource.content)
        gn = rdflib.Namespace("http://www.geonames.org/ontology#")
        for s, p, o in g.triples((None, gn['population'], None)):
            logging.debug("Result of geonames parsing: %s" % o)
            struc = {}
            struc['population'] = str(o)
            return struc
        logging.debug("No parsing result at geonames")
    except SAXParseException as e:
        logging.error("saxparseexception %s" % e)
    return None

def parse_freebase(resource):
    """
    parse freebase api (with url-modification) and return population
    """

    
    match = re.search(".*(\/.\.[a-zA-Z0-9\._]*)", resource.url)
    topic_id = None
    if match:
        topic_id = match.group(1).replace(".", "/")
    else:
        logging.debug("no topicid found in url")
        return None

    DEFAULT_CONFIG_FILE = 'config/default.yaml'
    config = yaml.load(file(DEFAULT_CONFIG_FILE, 'r'))

    api_key = config['parser']['freebaseAPIkey']
    service_url = config['parser']['freebaseServiceurl']

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


def dump_nt_sorted(g): #Source: https://rdflib.readthedocs.org/en/4.1.0/_modules/rdflib/compare.html
    output = "";
    for l in sorted(g.serialize(format='nt').splitlines()):
        if l: output = output + (l.decode('ascii'))
    return output

