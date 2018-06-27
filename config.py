import json

with open('env.json') as f:
    obj = json.load(f)

    DBINFO = obj['db']
    ACCOUNT = obj['steem']
