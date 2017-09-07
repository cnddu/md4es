# -*- coding:utf-8 -*-

import os
import pyinotify
from functions import *
import fnmatch
import hashlib
import markdown2
import json
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch,NotFoundError, RequestsHttpConnection, serializer, compat, exceptions

def suffix_filter(fn):                                                              
    suffixes = ["*.md"]
    for suffix in suffixes:                                                         
        if fnmatch.fnmatch(fn, suffix):                                             
            return False                                                            
    return True  

class JSONSerializerPython2(serializer.JSONSerializer):
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

es = Elasticsearch(serializer=JSONSerializerPython2())
WATCH_PATH = '/home/sjchen/repos/'

if not WATCH_PATH:
  print('Error',"The WATCH_PATH setting MUST be set.")
  sys.exit()
else:
  if os.path.exists(WATCH_PATH):
    print('Watch status','Found watch path: path=%s.' % (WATCH_PATH))
  else:
    print('Error','The watch path NOT exists, watching stop now: path=%s.' % (WATCH_PATH))
    sys.exit()

class OnIOHandler(pyinotify.ProcessEvent):

  #def process_IN_CREATE(self, event):
    #print('Action',"create file: %s " % os.path.join(event.path,event.name))

  def process_IN_DELETE(self, event):
    print('Action',"delete file: %s " % os.path.join(event.path,event.name))
    if not suffix_filter(event.name):
        root_idx = event.pathname.find(WATCH_PATH)
        print(root_idx)
        relative_path = event.pathname[root_idx+len(WATCH_PATH):]
        print(relative_path)
        folders = relative_path.split('/')
        esindex = folders[0]
        estype = folders[1]
        print(esindex)
        print(estype)

        pathsha1 = hashlib.sha1(relative_path).hexdigest()
        print('pathsha1: %s' %pathsha1)
        try:
            res = es.delete(index=esindex, doc_type=estype, id=pathsha1)
            print('delete doc %s => %s' %(relative_path,res))
        except NotFoundError, e:
            print('doc %s found? %s' %(relative_path,e.info['found']))
  #def process_IN_MODIFY(self, event):
    #print('Action',"modify file: %s " % os.path.join(event.path,event.name))
    #print("Modifying:", event.pathname)
  def process_IN_MOVED_TO(self, event):
    if not suffix_filter(event.name):
        #print('Action',"modify file: %s " % os.path.join(event.path,event.name))
        print("Moving to:", event.pathname)
        root_idx = event.pathname.find(WATCH_PATH)
        print(root_idx)
        relative_path = event.pathname[root_idx+len(WATCH_PATH):]
        print(relative_path)
        folders = relative_path.split('/')
        esindex = folders[0]
        estype = folders[1]
        print(esindex)
        print(estype)

        short_path = relative_path[len(esindex)+len(estype)+2:]
        print('short path: %s' %short_path)

        pathsha1 = hashlib.sha1(relative_path).hexdigest()
        print('pathsha1: %s' %pathsha1)

        filesha1 = hashlib.sha1(open(event.pathname, 'rb').read()).hexdigest()
        print('file sha1: %s' %filesha1)
        res = es.indices.exists(index=esindex)
        print('check index exists: %s => %s' %(esindex,res))
        if (res == False):
            res = es.indices.create(index=esindex)
            print('create index: %s => %s' %(esindex,res))

        res = es.indices.exists_type(index=esindex,doc_type=estype)
        print('check type exists: %s => %s' %(estype,res))
        if (res == False):
            #create mapping
            mappingstr = {
                estype: {
                    "_all": {
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word",
                        "term_vector": "no",
                        "store": "false"
                    },
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word",
                            "include_in_all": "true",
                            "boost": 8
                        },
                        "content": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word",
                            "include_in_all": "true",
                            "boost": 8
                        },
                        "filepath": {
                            "type": "text",
                            "include_in_all": "true"
                        },
                        "filehash": {
                            "type": "text",
                            "include_in_all": "true"
                        }
                    }
                }
            }
            #print(mappingstr)
            res = es.indices.put_mapping(index=esindex,doc_type=estype,body=mappingstr)
            print('create mapping %s => %s' %(estype,res))

        #check if need update or totally new
        try:
            docexist = True
            res = es.get(index=esindex, doc_type=estype, id=pathsha1)
            print('search for file id: %s => %s' %(pathsha1,res))
        except NotFoundError, e:
            docexist = e.info['found']
            print('doc %s not found' %(relative_path))

        #add current doc into index
        html = markdown2.markdown_path(event.pathname)
        #print(html)
        soup = BeautifulSoup(html,'html5lib')
        title = ""
        if (soup.h1 != None):
            title = soup.h1.string
        elif (soup.h2 != None):
            title = soup.h2.string
        elif (soup.h3 != None):
            title = soup.h3.string
        elif (soup.h4 != None):
            title = soup.h4.string

        print(title)
        #print(soup.get_text())
        doc = {
            'title':title,
            'content':soup.get_text().replace('\n',''),
            'filepath':relative_path,
            'filehash':filesha1,
            'shortpath':short_path
        }
        res = es.index(index=esindex, doc_type=estype, id=pathsha1, body=doc)
        print('add/update doc %s => %s' %(relative_path,res))

def auto_compile(path = '.'):
  wm = pyinotify.WatchManager()
  mask = pyinotify.IN_CREATE | pyinotify.IN_DELETE | pyinotify.IN_MODIFY | pyinotify.IN_MOVED_TO
  #mask = pyinotify.IN_DELETE | pyinotify.IN_MODIFY
  notifier = pyinotify.ThreadedNotifier(wm, OnIOHandler())
  notifier.start()
  wm.add_watch(path, mask,rec = True,auto_add = True)
  print('Start Watch','Start monitoring %s' % path)
  while True:
    try:
      notifier.process_events()
      if notifier.check_events():
        notifier.read_events()
    except KeyboardInterrupt:
      notifier.stop()
      break

if __name__ == "__main__":
   auto_compile(WATCH_PATH)
