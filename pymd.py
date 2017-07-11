import sys
import time
import hashlib
import markdown2
import json
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch,NotFoundError, RequestsHttpConnection, serializer, compat, exceptions
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

class JSONSerializerPython2(serializer.JSONSerializer):
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

PATH_PREFIX = '/home/sjchen/test/'
es = Elasticsearch(serializer=JSONSerializerPython2())

class MyHandler(PatternMatchingEventHandler):
    patterns=["*.md"]

    def process(self, event):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        print('event type: %s' %event.event_type)

        root_idx = event.src_path.find(PATH_PREFIX)
        print(root_idx)
        relative_path = event.src_path[root_idx+len(PATH_PREFIX):]
        print(relative_path)
        folders = relative_path.split('/')
        esindex = folders[0]
        estype = folders[1]
        print(esindex)
        print(estype)
        
        pathsha1 = hashlib.sha1(relative_path).hexdigest()
        print('pathsha1: %s' %pathsha1)

        if (event.event_type == 'deleted'):
            try:
                res = es.delete(index=esindex, doc_type=estype, id=pathsha1)
                print('delete doc %s => %s' %(relative_path,res))
            except NotFoundError, e:
                print('doc %s found? %s' %(relative_path,e.info['found']))
        elif ((event.event_type == 'modified') or (event.event_type == 'created')):
            filesha1 = hashlib.sha1(open(event.src_path, 'rb').read()).hexdigest()
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
            html = markdown2.markdown_path(event.src_path)
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
            }
            res = es.index(index=esindex, doc_type=estype, id=pathsha1, body=doc)
            print('add/update doc %s => %s' %(relative_path,res))

    def on_modified(self, event):
        print("modified => %s" %(event.src_path))
        self.process(event)

    def on_created(self, event):
        print("created => %s" %(event.src_path))
        self.process(event)

    def on_deleted(self, event):
        print("deleted => %s" %(event.src_path))
        self.process(event)


if __name__ == '__main__':
    args = sys.argv[1:]
    observer = Observer()
    print(args[0])
    observer.schedule(MyHandler(), path=args[0] if args else '.',recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

