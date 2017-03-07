import urllib2
import urllib
import json
import pprint
import base64
import pprint
import redis
import os
from hdfs import InsecureClient
#from confluent_kafka import Producer
#from hdfs import TokenClient
#from hdfs import InsecureClient

#client = TokenClient('http://node-1.testing:50070/', 'root', root='/user/root/data_trentino')
#client = InsecureClient('http://node-1.testing:50070/', 'root', root='/user/root/open_data')
#request = urllib2.Request("http://93.63.32.36/api/3/action/group_list")
#URL_DATI_TRENTINO = "http://dati.trentino.it"
#URL_DATI_GOV = "http://93.63.32.36"
URL_DATI_GOV = "http://192.168.22.11/"

class HeadRequest(urllib2.Request):
     def get_method(self):
         return "HEAD"

class WorkerCrawler:

    def __init__(self, redis_ip, redis_port):
        self.redis_ip = redis_ip
        self.port = redis_port
        self.r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
        self.client =  InsecureClient('http://cdh1:50070/', 'admin', root='/user/admin/open_data')

    def formatUrl(self, url):
        urlSplit = url.rsplit('/', 1)
        urlEnd = urllib.quote(urlSplit[1])
        urlStart = urlSplit[0]
        finalUrl = urlStart + "/" + urlEnd
        return finalUrl

    def consumeData(self):
        red = self.r
        while(red.llen("old_dataset_id") != 0):
            dataset_id = red.lpop("old_dataset_id")
            encRes = urllib.urlencode({"id" : unicode(dataset_id).encode('utf-8')})
            request_info = urllib2.Request(URL_DATI_GOV + "/api/3/action/package_show?" + encRes)
            base64string = base64.b64encode('%s:%s' % ("newdatigovit", "qaz-plm-17"))
            request_info.add_header("Authorization", "Basic %s" % base64string)
            try:
                response_info = urllib2.urlopen(request_info)
                info_dataset = json.loads(response_info.read())
                results = info_dataset['result']
                info = results
                #print json.dumps(info)
                if 'resources' in info:
                    #print info
                    info["m_status_resources"] = "ok"
                    resources = info['resources']
                    name = info['name']
                    idInfo = info['id']
                    for resource in resources:
                        rUrl = resource['url']
                        rFormat = resource['format']
                        rName = resource['name']
                        rId = resource['id']
                        finalUrl = self.formatUrl(rUrl)
                        rInfo = urllib2.Request(finalUrl)
                        rInfo.get_method = lambda : 'HEAD'
                        try:
                            rReq = urllib2.urlopen(rInfo)
                            if rReq.code == 200:
                                resource["m_status"] = "ok"
                            else:
                                resource["m_status"] = "ko"
                        except Exception, e:
                            resource["m_status"] = "ko"
                            print str(e)
                else:
                    print info
                    info["m_status_resources"] = "ko"
                    print "NO RESOURCES"
                with self.client.write('old_dati_gov/dati_gov.json', encoding='utf-8', append=True) as writer:
                        writer.write(json.dumps(info) + '\n')
            except Exception, e:
                print str(e)
                red.lpush("old_dataset_error", dataset_id)


worker = WorkerCrawler("192.168.22.12", 6379)
worker.consumeData()
