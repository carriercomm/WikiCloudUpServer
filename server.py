#!/usr/bin/env python
# encoding: utf-8
"""
server.py: 

Created by Peter Kalchgruber on 2013-09-16.
Copyright 2013, All rights reserved.
"""


import os.path
import logging
import tornado.web
import tornado.httpserver
import tornado.ioloop
import yaml
import datetime
import time

from base import Base
from util import datasources_to_XML

DEFAULT_CONFIG_FILE = 'config/default.yaml'

class HTTPServer():
    """The HTTPServer handles all requests
        
    """
    
    def __init__(self, config):
        """Initializes HTTP server with default settings and handlers"""
        
        logging.basicConfig(level=logging.DEBUG)
        self.settings = dict(
            title=u"ProFuse",
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path="static/",
            autoescape=None,     

        )

        self.threads={}
        favicon_path = 'static/favicon.ico'
        self.handlers = [
            (r"/", HomeHandler,dict(config = config, threads=self.threads)),
            (r"/get", GetRequestHandler,dict(config = config, threads=self.threads)),
            (r"/live", LiveRequestHandler,dict(config = config, threads=self.threads)),
            (r"/test", TestRequestHandler,dict(config = config, threads=self.threads)),
            (r'/favicon.ico', tornado.web.StaticFileHandler, {'path': favicon_path}),
        ]
        
    
    def start(self):
        logging.info("Starting up HTTP Server on port 8080")
        application = tornado.web.Application(
                        handlers = self.handlers, 
                        debug = True,
                        **self.settings)
        self.http_server = tornado.httpserver.HTTPServer(application)
        self.http_server.listen(8080)
        tornado.ioloop.IOLoop.instance().start()
        
    def stop(self):
        logging.info("Stopping HTTP Server")
        #stopping all child threads
        for key, thread in self.threads.iteritems():
            thread.stop()
            logging.debug("Stopping thread: %s" % key)
        tornado.ioloop.IOLoop.instance().stop()
    
class BaseRequestHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ("GET", "POST")
    
    def initialize(self, config, threads):
        self.config = config
        self.threads = threads


class HomeHandler(BaseRequestHandler):
    """Start page handler"""
    def get(self):
        self.render("home.html")

class GetRequestHandler(BaseRequestHandler):
    @tornado.web.asynchronous
    def get(self):
        # extract URL-Parameters
        article = self.get_argument("article");
        last = self.get_argument("last",None); 
        if article in self.threads:
            thread = self.threads[article]
            if thread.completed == 1 and thread.startdate > int(time.time())-5:
                logging.debug("process finished few seconds ago, recovering existing results")
                self.worker_done(thread.datasources)
            elif thread.completed == 1:
                logging.debug("process already finished, starting new process")
                thread = Base(self.config, article, last, self.worker_done)
                thread.start()
                self.threads[article] = thread
            else:
                tstartdate = datetime.datetime.fromtimestamp(thread.startdate).strftime("%d.%m.%Y %H:%M:%S")
                logging.debug("process with article %s running since %s" % (article, tstartdate))
                self.worker_done(thread.datasources)
        else:
            logging.debug("Article: %s first time fetched, starting new process" % article)
            thread = Base(self.config, article, last, self.worker_done)
            thread.start()
            self.threads[article] = thread
            
    def worker_done(self, datasourcelist):
        self.finish(datasources_to_XML(datasourcelist))    

class LiveRequestHandler(BaseRequestHandler):
    """ Requests status about request
        returns already fetched resources
    """      

    def get(self):
        article = self.get_argument("article", None);
        if article is None:
            self.send_error(400)
        if article in self.threads:
            thread = self.threads[article]
            self.finish(datasources_to_XML(thread.live()))
        else:
            self.finish("not yet searched, please start request with <a href=\"/get?article=%s\">http://localhost:8080/get?article=%s" % (article,article))
        
class TestRequestHandler(BaseRequestHandler):
    """ Requests Status about threads
    """      

    def get(self):
        output="";
        for article, thread in self.threads.iteritems():
            output+="%s %s<br>" % (article, thread.completed)
            if (thread.startdate + 60*5) < int(time.time()):
                logging.debug("Thread %s killed, 5 minutes threshold" % article)
                thread.stop()
        self.write(output);

def main():
    config = yaml.load(file(DEFAULT_CONFIG_FILE, 'r'))
    server=HTTPServer(config);
    try:
        server.start()
    except KeyboardInterrupt:
        print "\nStopping server and exiting gracefully..."
    finally:
        server.stop()

if __name__ == "__main__":
    main()    