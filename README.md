PF-Wiki-Cloud-Update-Server
======

Installation
--------
- download files: https://github.com/pedak/WikiCloudUpServer/archive/master.zip
- if not installed, install easy_install: sudo apt-get install python -setuptools
- sudo easy_install tornado
- sudo easy_install pyyaml
- sudo easy_install rdflib
- sudo easy_install simplejson

Start-Up
-------
- Go to project folder
- run ./server.py


Edit config file:
---
- config/default.yaml
-- Insert your API-Keys to parse e.g. freebase data


Start Server
-----
./server.py


Enjoy to be informed on new updates on equivalent resources of the current Wikipedia article.

Technical Details
-----
Server:
- searches sameas.org
- filters for geonames and rdflib
- searches for local dump of file
- compares (datestamp file) with current version
- if they differ, return diff 
- parse currentfile for population
- download current file if current file differs with last checked version
- return results