from google.cloud import bigquery
import os

class SqlCommander:
    def __init__(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './gohappyshopping-206407-db250b4b8886.json'
    
    def send(self, command):
        client = bigquery.Client()
        query = client.query(command)
        return query.result()    