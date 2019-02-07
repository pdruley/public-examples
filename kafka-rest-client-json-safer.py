#REST Proxy Consumer Client Example

#This example uses each consumer instance once for a read request and then deletes it

#choose your favorite http library
import requests
#using this for sleeping
import time

#a modest amount of global vars
json_header = {'Content-Type': 'application/vnd.kafka.v2+json'}
accept_json_header = {'Accept': 'application/vnd.kafka.json.v2+json'}
#we have two local REST Proxy instances in this case
urls = ['http://localhost:8182', 'http://localhost:8282']

def create_consumer_instance(url, header):
  #create a consumer instance and return the base uri

  #define the consumer configuration
  consumer_config = {
    'format': 'json',
    'auto.offset.reset': 'earliest',
    'auto.commit.enable': 'false'
  }
  retries = 0
  #define a consumer group
  consumer_group = 'psdpycg'
  base_uri = None
  while base_uri is None:
    retries = retries + 1
    #there is probably a better way to handle this but retry limits works
    if retries > len(urls):
      print("maximum retries hit")
      base_uri = False
      break
    #build my host uri with REST Proxy url and consumer group
    host = url + '/consumers/' + consumer_group + '/'
    #print("current consumer instance is " + host)
    try:
      consumer_instance = requests.post(host, headers=header, json=consumer_config)
    except:
      print("couln't create consumer instance for host:" + host + " but will try another one")
      #this is a lazy way of simulating a load balancer for multiple instances of REST Proxy
      #it actually works for more than 2 instances
      current_index = urls.index(url)
      if current_index < (len(urls) - 1):
        url = urls[current_index + 1]
      else:
        url = urls[0]
    else:
      base_uri = consumer_instance.json()['base_uri'] 
  return url, base_uri

def subscribe(url, header):
  #subscribe consumer instance to topic
  topics = {'topics': ['rest-test']}
  if url is not None:
    host = url + '/subscription'
    try:
      requests.post(host, headers=header, json=topics)
    except:
      print("couldn't subscribe to " + host)
  else:
    print("url is empty")

def read_messages(url, header):
  #get records and return messages as json
  host = url + '/records'
  try:
    records = requests.get(host, headers=header)
  except:
    print("couldn't read messages from " + host)
    raise
  else:
    messages = records.json()
    return messages

def commit_offsets(messages, url, header):
  #manually commit offset for messages we read
  offsets = []
  body = {}
  if messages is None:
    print("no records")
  else:
    for record in messages:
      offset = dict([ \
        ('topic', record['topic']), \
        ('partition', record['partition']), \
        ('offset', record['offset']) \
        ])    
      offsets.append(offset)
  body['offsets'] = offsets
  host = url + '/offsets'
  try:
    requests.post(host, headers=header, json=body)
  except:
    print("couldn't commit offsets for " + host)
    raise

def delete_consumer(url, header):
  #destroy the consumer instance when you are done
  host = url
  try:
    requests.delete(host, headers=header)    
  except:
    print("couldn't delete consumer " + host)
  
#main
#seed a REST Proxy url
url = urls[0]
try:
    while 1:
      #create consumer instance
      url, base_uri = create_consumer_instance(url, json_header)  
      #subscribe consumer to a topic
      if base_uri == False:
          print("base uri empty, no REST Proxy instances found.")
          break
      subscribe(base_uri, json_header)
      #read messages
      messages = read_messages(base_uri, accept_json_header)
      if not messages:
          #print("messages is empty sleeping for 10 seconds")
          time.sleep(10)
          #cleanup the consumer instance
          delete_consumer(base_uri, json_header)
      else:
          #process messages
          print(messages)
          #commit offsets
          commit_offsets(messages, base_uri, json_header)
          #cleanup the consumer instance
          delete_consumer(base_uri, json_header)
finally:
    #all hope is lost, try to at least delete the consumer instance before closing
    delete_consumer(base_uri, json_header)