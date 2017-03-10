import urllib2
import urllib
import json
import pprint
import base64
import pprint
import redis
import os
from hdfs import InsecureClient
import hdfs
#from confluent_kafka import Producer
#from hdfs import TokenClient
#from hdfs import InsecureClient

#client = TokenClient('http://node-1.testing:50070/', 'root', root='/user/root/data_trentino')
#client = InsecureClient('http://node-1.testing:50070/', 'root', root='/user/root/open_data')
#request = urllib2.Request("http://93.63.32.36/api/3/action/group_list")
#URL_DATI_TRENTINO = "http://dati.trentino.it"
URL_DATI_GOV = "http://www.datigov.it"
#URL_DATI_GOV = "http://192.168.22.11/"

class HeadRequest(urllib2.Request):
     def get_method(self):
         return "HEAD"

class MasterCrawler:

    def __init__(self, url_ckan, redis_ip, redis_port):
        self.ckan = url_ckan
        self.r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
        self.client =  InsecureClient('http://cdh1:50070/', 'admin', root='/user/admin/open_data')

    def formatUrl(url):
        urlSplit = url.rsplit('/', 1)
        lastPart = urlSplit[1]
        urlEnd = ""
        if "?" in lastPart:
            splitted = lastPart.split("?")
            urlEnd = splitted[0] + "?" + urllib.quote(splitted[1])
        else:
            urlEnd = urllib.quote(lastPart)
        urlStart = urlSplit[0]
        finalUrl = urlStart + "/" + urlEnd
        return finalUrl

    def initializeRedis(self):
        content = self.client.content('old_dati_gov/dati_gov.json', strict=False)
        if not content:
            with self.client.write('old_dati_gov/dati_gov.json', encoding='utf-8') as writer:
                writer.write('')
        request = urllib2.Request(URL_DATI_GOV + "/api/3/action/package_list")
        #base64string = base64.b64encode('%s:%s' % ("newdatigovit", "qaz-plm-17"))
        #request.add_header("Authorization", "Basic %s" % base64string)
        response = urllib2.urlopen(request)
        assert response.code == 200
        response_dict = json.loads(response.read())
        # Check the contents of the response.
        assert response_dict['success'] is True
        result = response_dict['result']
        test_res = result #[:2000]
        for res in test_res:
            print res
            self.r.rpush("old_dataset_id", res)

    def consumeData(self):
        red = self.r
        while(red.llen("old_dataset_id") != 0):
            dataset_id = red.lpop("old_dataset_id")
            encRes = urllib.urlencode({"id" : unicode(dataset_id).encode('utf-8')})
            request_info = urllib2.Request(URL_DATI_GOV + "/api/3/action/package_show?" + encRes)
            #base64string = base64.b64encode('%s:%s' % ("newdatigovit", "qaz-plm-17"))
            #request_info.add_header("Authorization", "Basic %s" % base64string)
            info = None
            try:
                response_info = urllib2.urlopen(request_info)
                info_dataset = json.loads(response_info.read())
                results = info_dataset['result']
                info = results
            except Exception, e:
                print str(e)
                print "o qui e perche"
                red.lpush("old_dataset_error", dataset_id)
                #print json.dumps(info)
            if info:
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
                        print rUrl
                        finalUrl = self.formatUrl(rUrl)
                        rInfo = urllib2.Request(finalUrl)
                        rInfo = urllib2.Request(rUrl)
                        rInfo.get_method = lambda : 'HEAD'
                        #rInfo.add_header("Authorization", "Basic %s" % base64string)
                        try:
                            rReq = urllib2.urlopen(rInfo)
                            if rReq.code == 200:
                                resource["m_status"] = "ok"
                            else:
                                resource["m_status"] = "ko"
                        except Exception, e:
                            print info
                            print "sara questo????"
                            resource["m_status"] = "ko"
                            print "sara qui ..."
                            print str(e)
                else:
                        info["m_status_resources"] = "ko"
                        print "NO RESOURCES"
                try:
                    with self.client.write('old_dati_gov/dati_gov.json', encoding='utf-8', append=True) as writer:
                        writer.write(json.dumps(info) + '\n')
                except hdfs.util.HdfsError, ex:
                    print str(ex)



#URL_DATI_TRENTINO = "http://dati.trentino.it"
URL_DATI_GOV = "http://www.dati.gov.it"
#URL_NEW_DATI_GOV = "http://192.168.22.11/"
REDIS_IP = "192.168.22.12"
REDIS_PORT = 6379

crawler = MasterCrawler(URL_DATI_GOV,REDIS_IP,REDIS_PORT)
crawler.initializeRedis()
crawler.consumeData()
