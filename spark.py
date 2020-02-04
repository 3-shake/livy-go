import json
import requests
import textwrap
import sys

from pprint import pprint

host = 'http://localhost:8998'
headers = {'Content-Type': 'application/json'}


def session():
    data = {'kind': 'spark'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    pprint(r.json())

    session_url = host + r.headers['location']
    r = requests.get(session_url, headers=headers)
    pprint(r.json())
    print(f"session_url: ${session_url}")


def run(session_url):
    statements_url = session_url + '/statements'
    data = {'code': '1 + 1'}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    print(f"statements_url: ${statements_url}")
    r.json()

    data = {
        'code': textwrap.dedent("""
        val NUM_SAMPLES = 100000;
        val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
        val x = Math.random();
        val y = Math.random();
        if (x*x + y*y < 1) 1 else 0
        }.reduce(_ + _);
        println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
        """)
    }

    print(f"statements_url: ${statements_url}")
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)

    statement_url = host + r.headers['location']
    print(statement_url)
    r = requests.get(statement_url, headers=headers)
    pprint(r.json())


if __name__ == '__main__':
    args = sys.argv
    print(args)
    if len(args) == 1:
        session()
    else:
        run(args[1])
