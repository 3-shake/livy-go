from pprint import pprint
import json, pprint, requests, textwrap

host = 'htp://localhost:8998'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}

r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)

session_url = host + r.headers['location']
r = requests.get(session_url, headers=headers)
pprint(r.json())
pprint(r.headers)

pprint(r.json())

statements_url = session_url + '/statements'
data = {'code': '1 + 1'}
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint(r.json())

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

r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint(r.json())

statement_url = host + r.headers['location']
r = requests.get(statement_url, headers=headers)
pprint.pprint(r.json())