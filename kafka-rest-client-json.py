#REST Proxy Consumer Client Example

#TODO
#Need a clean way of of exiting to use delete_consumer
#This would allow us to exit the program without leaving hanging consumer instances

#choose your favorite http library
import requests
#using this for sleeping
import time

#a modest amount of global vars
json_header = {'Content-Type': 'application/vnd.kafka.v2+json'}
accept_json_header = {'Accept': 'application/vnd.kafka.json.v2+json'}

def create_consumer_instance(header):
  #create a consumer instance
  #returns the base uri

  #define the consumer configuration
  consumer_config = {
    'format': 'json',
    'auto.offset.reset': 'earliest',
    'auto.commit.enable': 'false'
  }
  #we have two local instances in this case
  urls = ['http://localhost:8182', 'http://localhost:8282']
  url = urls[0]
  retries = 0
  #define a consumer group
  consumer_group = 'psdpycg'
  base_uri = None
  while base_uri is None:
    retries = retries + 1
    #there is probably a better way to handle this but retry limits works
    if retries > 3:
      print("maximum retries hit")
      base_uri = False
      break
    #build my host uri with REST Proxy url and consumer group
    host = url + '/consumers/' + consumer_group + '/'
    print("current consumer instance is " + host)
    try:
      consumer_instance = requests.post(host, headers=header, json=consumer_config)
    except:
      print("couln't create consumer instance but will try another one")
      #this is a lazy way of simulating a load balancer for two instances of REST Proxy
      if url == urls[0]:
        url = urls[1]
      else:
        url = urls[0]
    else:
      base_uri = consumer_instance.json()['base_uri']  
  return base_uri

def subscribe(url, header):
  #subscribe consumer instance to topic
  #define topics to subscribe consumer to
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
  #get records
  #returns messages as json
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
  else:
    print(host + "deleted")

#main
while 1:
  #create consumer instance
  print("trying to create consumer instance")
  base_uri = create_consumer_instance(json_header)  
  #subscribe consumer to a topic
  if base_uri == False:
    break
  subscribe(base_uri, json_header)
  #read messages
  while 1:
    try:
      messages = read_messages(base_uri, accept_json_header)
    except:
      break
    if not messages:
      print("messages is empty sleeping for 5 seconds")
      time.sleep(5)
    else:
      #process messages
      print(messages)
      #commit offsets
      try:
        commit_offsets(messages, base_uri, json_header)
      except:
        break