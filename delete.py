import requests
from pprint import pprint

host = 'http://localhost:8998'
headers = {'Content-Type': 'application/json'}


def delete_all():
    r = requests.get(host + '/sessions', {}, headers=headers)
    sessions = r.json()['sessions']
    for sess in sessions:
        session_id = sess["id"]
        session_url = f"http://localhost:8998/sessions/${session_id}"
        print(session_url)
        r = requests.delete(session_url, headers=headers)
        pprint(r.json())


delete_all()
