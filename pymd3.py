# -*- coding:utf-8 -*-

import os
from functions import *
import fnmatch
import hashlib
import schedule
import markdown2
import json
import time
import requests
import mysql.connector
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch,NotFoundError, RequestsHttpConnection, serializer, compat, exceptions

dbhost = 'rm-bp18giw6ai77y6171o.mysql.rds.aliyuncs.com'
dbuser = 'root'
dbpwd = 'Glj@2017'
dbname = 'love2io'
es = None

github_prefix = 'https://raw.githubusercontent.com/'

class JSONSerializerPython2(serializer.JSONSerializer):
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

def reset_all():
    global dbhost,dbuser,dbpwd,dbname,es
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname)
    cursor = cnx.cursor()
    
    queryRecords = ("SELECT repo FROM a2post")
    cursor.execute(queryRecords)
    records = cursor.fetchall()

    if (records is not None):
        for recordItem in records:
            print(recordItem[0])
            index_delete(recordItem[0])
            index_create(recordItem[0])

    cnx.commit()
    cursor.close()
    cnx.close()

def index_delete(delpath):
    global es
    print("delete index: %s " %delpath)
    folders = delpath.split('/')
    esindex = folders[0].lower()  #ES force index to be lower
    estype = folders[1]
    print(esindex)
    print(estype)

    pathsha1 = hashlib.sha1(delpath.encode('utf-8')).hexdigest()
    print('pathsha1: %s' %pathsha1)
    try:
        res = es.delete(index=esindex, doc_type=estype, id=pathsha1)
        print('delete doc %s => %s' %(delpath,res))
    except NotFoundError, e:
        print('doc %s found? %s' %(delpath,e.info['found']))

def index_create(crindex):
    global es,github_prefix
    print('==============================\n')
    print('create index: %s'  %crindex )
    githuburl = github_prefix+crindex+'/master/SUMMARY.md'
    print('get book summery => '+githuburl)
    summery = requests.get(githuburl)
    print(summery.text)

    #add current doc into index
    html = markdown2.markdown(summery.text)
    print(html)
    soup = BeautifulSoup(html,'html5lib')
    for mddoc in soup.findAll("a"):
        docpath = mddoc['href']
        print(docpath)
        index_doc(crindex,docpath)
        #time.sleep(10000)

def index_doc(crindex,docpath):
    folders = crindex.split('/')
    esindex = folders[0].lower()
    estype = folders[1]
    print(esindex)
    print(estype)

    pathsha1 = hashlib.sha1(docpath.encode('utf-8')).hexdigest()
    print('pathsha1: %s' %pathsha1)

    docurl = github_prefix+crindex+'/master/'+docpath
    print('get book page => '+docurl)
    doccontent = requests.get(docurl)
    print(doccontent.text)

    #add current doc into index
    html = markdown2.markdown(doccontent.text)

    filesha1 = hashlib.sha1(doccontent.text.encode('utf-8')).hexdigest()
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
        print('doc %s not found' %(docpath))

    #add current doc into index
    #html = markdown2.markdown_path(event.pathname)
    
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
        'filepath':docpath,
        'filehash':filesha1
    }
    res = es.index(index=esindex, doc_type=estype, id=pathsha1, body=doc)
    print('add/update doc %s => %s' %(docpath,res))

def auto_check():
    print ("auto check ==>")

if __name__ == "__main__":
    es = Elasticsearch(serializer=JSONSerializerPython2())
    reset_all()

    schedule.every(10).seconds.do(auto_check)

    while True:
        schedule.run_pending()
        time.sleep(1)
