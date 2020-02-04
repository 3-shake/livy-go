import requests
from pprint import pprint

host = 'http://localhost:8998'
headers = {'Content-Type': 'application/json'}


def delete_all():
    statement_url = "http://localhost:8998/sessions/0/statements"
    r = requests.get(statement_url, headers=headers)
    pprint(r.json())

delete_all()
