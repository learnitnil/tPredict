import requests
url = 'http://127.0.0.1:5000/v1/traffic?nodeId=12'

resp = requests.post(url)
print(resp)
print(resp.text)