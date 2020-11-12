import requests
from config import *

try:
    r = requests.get(source_data_url)
except Exception as e:
    printError("Could not get data file from source url: " + source_data_url)
    print(e)
    printElapsedTime()
    quit()

print(r.content)
