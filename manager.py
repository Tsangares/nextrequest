import requests, json, logging, time, csv, pymongo, random, socket
import requests.packages.urllib3.util.connection as urllib3_cn
from multiprocessing import Pool,Process

cred = json.load(open('credentials.json'))
db_url = f"{cred['schema']}{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['db']}?authSource={cred['auth']}"
print(db_url)
mongo = pymongo.MongoClient(db_url)

class NextRequest:
    def __init__(self,subdomain,delay=5):
        self.subdomain = subdomain
        self.delay = delay
        self.total_documents = None
        self.total_requests = None
        self.schema = 'https://'
        self.requests_endpoint  = self.schema + self.subdomain + '/client/requests'
        self.documents_endpoint = self.schema + self.subdomain + '/client/documents'

    def start(self):
        self.get_requests()
    #Retry upon 429 Too Many Reqests
    def get(self,url,params=None):
        #logging.info(f"Getting {url} with {params}")
        response = requests.get(url,json=params)
        while response.status_code == 429:
            response = requests.get(url)
            delay = self.delay + random.random()*10
            logging.info(f"Response 429: too many requests. Waiting {delay:.02f} seconds. {self.subdomain}")
            time.sleep(delay)
        if response.status_code == 500:
            logging.error(f"Limit reached {response}")
            return None
        logging.info(f'{response}: {url}')
        return response.json()

    #sort_order could be "asc"
    def get_requests(self,page=1,sort_order="desc"):
        params = {
            "sort_field": "created_at",
            "page_number": page,
            "page_size": 100,
            "sort_order": sort_order
        }
        response = self.get(self.requests_endpoint,params=params)
        self.total_requests = response["total_count"]
        request_ids = [query['id'] for query in response['requests']]
        requests = None
        with Pool(30) as pool:
            requests = pool.map(self.get_request,request_ids)
        output = []
        for metadata,request in zip(response['requests'],requests):
            if request is not None:
                output.append(request | metadata | {'domain': self.subdomain})
        logging.warning(f"Uploading to db {len(output)}.")
        if len(output) == 0: return
        mongo.nextrequest.requests.insert_many(output)
        
        logging.warning(f"Going to page {page}")
        self.get_requests(page+1,sort_order)
            
            
    def get_request(self,request_id):
        db = pymongo.MongoClient(db_url)
        url = f'{self.requests_endpoint}/{request_id}'
        exists = db.nextrequest.requests.find_one({'url': url})
        if not exists:
            response = self.get(f'{self.requests_endpoint}/{request_id}')
            if response is not None:
                response['url'] = url
                return response
        return None
            

def crawl_subdomain(url):
    nr = NextRequest(url)
    nr.start()
def multi_processing():
    processes = []
    logging.info("Initialized deterministic multi processing")    
    for subdomain in mongo.nextrequest.subdomains.find({}):
        url = subdomain['subdomain']        
        p = Process(target=crawl_subdomain,args=(url,))
        processes.append(p)
        time.sleep(.3)
        p.start()
    for p in processes:
        p.join()
    print('done')
def single_processing():
    logging.info("Initialized deterministic single processing")
    for subdomain in mongo.nextrequest.subdomains.find({}):
        logging.info(f'Cralwing {subdomain}')
        url = subdomain['subdomain']
        crawl_subdomain(url)
    print('done')
    
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    single_processing()
