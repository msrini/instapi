import requests
import json
import certifi
from elasticsearch import Elasticsearch
from elasticsearch_dsl import search, Q

class ESInterface():
    def __init__(self, mode):
        self.outp = {}
        if mode == "local":
            #Assume 9200 is the port number
            self.baseURL = "http://localhost"
            self.port = "9200"
            self.es = Elasticsearch([{'host': 'localhost', 'port': self.port}])
        if mode == "remote":
            self.baseURL = 'https://a1b01d200848c62e2c28808e2af3eb6f.us-east-1.aws.found.io'
            self.port = "9243"
            self.es = Elasticsearch([self.baseURL], port=self.port,
                               http_auth="m.srini:zB.^a_bK0U",
                               use_ssl=True,
                               verify_certs=True,
                               ca_certs=certifi.where())
    def writeObj(self, key, data, index, type):
        res = None
        try:
            res = self.es.index(index=index, doc_type=type, id=key, body=data)
            self.log(res["result"])
        except Exception as e:
            self.log(e)
            return
        return
    def readFull(self, filt, index="leastfares", type="ssref"):
        try:
            retObjs = []
            if filt is None:
                query = {"query": {"match_all": {}}}
            else:
                query = {"query": {"match": filt}
                }
            page = self.es.search(
                index=index, doc_type=type, scroll='1m',
                size=10, body=query)
            sid = page['_scroll_id']
            scroll_size = page['hits']['total']
            while scroll_size > 0:
                # Get the number of results that we returned in the last scroll
                scroll_size = len(page['hits']['hits'])
                thispage = page['hits']['hits']
                for i in range(0,scroll_size):
                    retObjs.append(thispage[i]["_source"])
                page = self.es.scroll(scroll_id=sid, scroll='1m')
                sid = page['_scroll_id']
        except Exception as e:
            pass
        #Extend error handling process
        return retObjs
    def readChunk(self, filt, index, type, func, start=0):
        #Read in chunks, execute function for each record in the chunk, iterate it
        try:
            retObjs = []
            if filt is None:
                query = {"query": {"match_all": {}}, "from":start}
            else:
                query = {"query": {"match": filt}, "from": start}

            page = self.es.search(index=index, doc_type=type, scroll='1m', size=10, body=query)
            sid = page['_scroll_id']
            scroll_size = page['hits']['total']
            while scroll_size > 0:
                # Get the number of results that we returned in the last scroll
                scroll_size = len(page['hits']['hits'])
                thispage = page['hits']['hits']
                for i in range(0,scroll_size):
                    res = func(thispage[i]["_source"])
                    self.log(res)
                page = self.es.scroll(scroll_id=sid, scroll='1m')
                sid = page['_scroll_id']
        except Exception as e:
            self.log(e)
            return self.outp
        return self.outp
    def log(self,msg):
        if msg in self.outp:
            self.outp[msg] += 1
        else:
            self.outp.update({msg: 1})
    def count(self, index, type):
        cnt = self.es.count(index, type)
        return cnt["count"]
    def ff(self, flds, index="leastfares", type="ssref"):
        try:
            retObjs = []
            #page = self.es.search(index=index, doc_type=type, scroll='1m', size=10, fields=filt)
            s = search.Search(using=self.es, index=index)
            #s.query("multi_match", fields=['clientid'])
            #s.source(filt)  #flds array
            if flds is not None:
                s.source(flds)
            retObjs = s.execute()

        except Exception as e:
            pass
        # Extend error handling process
        return retObjs
    # def readRestES(self, index, type, rfrom=0):
    #     query = {
    #         "query": { "match_all": {} },
    #         "size": 100,
    #         "from":rfrom
    #     }
    #     searchURL = self.baseURL + ":" + self.port + "/" + index + "/" + type + "/_search"
    #
    #     resp = requests.get(searchURL)
    #     results = json.loads(resp.text)
    #
    #     data = [doc for doc in results['hits']['hits']]
    #     return results
if __name__ == "__main__":
    dh = ESInterface("local")
    filt = ["clientid"]
    x = dh.ff(filt, "app", "clients")
    print ("Done")