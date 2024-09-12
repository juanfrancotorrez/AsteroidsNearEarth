import requests

url = 'https://api.nasa.gov/neo/rest/v1/feed?'
paramurl= 'start_date=2024-09-10&end_date=2024-09-11&'
apikey = 'api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq'

finalurl = url+paramurl+apikey

print (finalurl)


data = requests.get(finalurl)
data = data.json()

print(data)