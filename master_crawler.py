import urllib2
import urllib
import json
import pprint
import base64
import pprint
import redis
import os
#from confluent_kafka import Producer
#from hdfs import TokenClient
#from hdfs import InsecureClient

#client = TokenClient('http://node-1.testing:50070/', 'root', root='/user/root/data_trentino')
#client = InsecureClient('http://node-1.testing:50070/', 'root', root='/user/root/open_data')
#request = urllib2.Request("http://93.63.32.36/api/3/action/group_list")
#URL_DATI_TRENTINO = "http://dati.trentino.it"
#URL_DATI_GOV = "http://93.63.32.36"
URL_DATI_GOV = "http://156.54.180.185"

class HeadRequest(urllib2.Request):
     def get_method(self):
         return "HEAD"

class MasterCrawler:

    def __init__(self, url_ckan, redis_ip, redis_port):
        self.ckan = url_ckan
        self.r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)

    def formatUrl(self, url):
        urlSplit = url.rsplit('/', 1)
        urlEnd = urllib.quote(urlSplit[1])
        urlStart = urlSplit[0]
        finalUrl = urlStart + "/" + urlEnd
        return finalUrl

    def initializeRedis(self):
        if not os.path.isfile("new_dati_gov_status.json"):
            with open('new_dati_gov_status.json', 'w') as writer:
                writer.write('')
        request = urllib2.Request(URL_DATI_GOV + "/api/3/action/package_list")
        response = urllib2.urlopen(request)
        assert response.code == 200
        response_dict = json.loads(response.read())
        # Check the contents of the response.
        assert response_dict['success'] is True
        result = response_dict['result']
        test_res = result #[:2000]
        for res in test_res:
            print res
            self.r.rpush("dataset_id", res)

    def consumeData(self):
        red = self.r
        while(red.llen("dataset_id") != 0):
            dataset_id = red.lpop("dataset_id")
            encRes = urllib.urlencode({"id" : unicode(dataset_id).encode('utf-8')})
            request_info = urllib2.Request(URL_DATI_GOV + "/api/3/action/package_show?" + encRes)
            #request_info.add_header("Authorization", "Basic %s" % base64string)
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
                        print finalUrl
                        rInfo = urllib2.Request(finalUrl)
                        try:
                            rReq = urllib2.urlopen(rInfo)
                            if rReq.code == 200:
                                resource["m_status"] = "ok"
                                if "csv" in rFormat.lower():
                                    print "qui passo"
                                    data = rReq.read()
                                    data_dir = "./open_data/" + dataset_id
                                    print data_dir
                                    if not os.path.exists(data_dir):
                                        os.makedirs(data_dir)
                                    file_path = data_dir + "/" + rId + "_" + rFormat + ".csv"
                                    with open(file_path, "wb") as code:
                                        code.write(data)
                                if "json" in rFormat.lower():
                                    data = rReq.read()
                                    data_dir = "./open_data/" + dataset_id
                                    if not os.path.exists(data_dir):
                                        os.makedirs(data_dir)
                                    file_path = data_dir + "/" + rId + "_" + rFormat + ".json"
                                    with open(file_path, "wb") as code:
                                        code.write(data)
                            else:
                                resource["m_status"] = "ko"
                        except Exception, e:
                            resource["m_status"] = "ko"
                            print str(e)
                else:
                    print info
                    info["m_status_resources"] = "ko"
                    print "NO RESOURCES"
            #rData = rReq.read()
                with open('new_dati_gov_status.json','a') as writer:
                    writer.write(json.dumps(info) + '\n')
            except Exception, e:
                print str(e)
                red.lpush("dataset_error", dataset_id)


#URL_DATI_TRENTINO = "http://dati.trentino.it"
#URL_DATI_GOV = "http://93.63.32.36"
URL_NEW_DATI_GOV = "http://156.54.180.185"
REDIS_IP = "localhost"
REDIS_PORT = 6379

crawler = MasterCrawler(URL_NEW_DATI_GOV,REDIS_IP,REDIS_PORT)
crawler.initializeRedis()
crawler.consumeData()
