#!/usr/bin/env python

import datetime, sys, os
os.environ[ 'MPLCONFIGDIR' ] = '/tmp/'
st_day = datetime.datetime.today().weekday()
st_time = datetime.datetime.now().time()

f = open("/home/ec2-user/flip/hour_start.txt", "r")
hour_start = int(f.readlines()[0])
f.close()

f = open("/home/ec2-user/flip/hour_end.txt", "r")
hour_end = int(f.readlines()[0])
f.close()

if hour_start == 0:
    sys.exit()

if (st_day > 4) | (st_time < datetime.time(hour_start,30)) | (st_time > datetime.time(hour_end,1)):
    sys.exit()

f = open("/home/ec2-user/flip/stocks.txt", "r")
last_line = f.readlines()[0]
f.close()

#print(last_line[:3])

if last_line[:3] != 'END':
        sys.exit()

f = open("/home/ec2-user/flip/stocks.txt", "w")
f.write('BEG')
f.close()

# API Token for IEX
f = open("/home/ec2-user/flip/iex.txt", "r")
token = f.readlines()[0]
f.close()
token = token.strip()

import os
os.chdir("/home/ec2-user/data")

import requests
import json
import pandas as pd
import numpy as np
#from pandas.io.json import json_normalize


print("Begin API requests:    ",datetime.datetime.now().time())


cs = pd.read_pickle('symbols_full_exNas.pkl')
cs = cs['symbol']
n = int(cs.nunique()/100)
nx = 1
if cs.nunique() % 100 == 0:
    nx = 0

l = []

for i in range(n+nx):
    a = ''
    for x in cs[i*100:(i+1)*100].values:
        a = a+','+x
    l.append(a[1:])

import time
from requests.adapters import HTTPAdapter
s = requests.Session()
s.mount('https://cloud.iexapis.com', HTTPAdapter(max_retries=3))
s.mount('http://cloud.iexapis.com', HTTPAdapter(max_retries=3))

fields = "symbol,latestVolume,latestPrice,previousClose,high,low"
req_base = "https://cloud.iexapis.com/stable/stock/market/batch?token="+token+"&types=quote&filter="+fields+"&symbols="
#print(req_base)

j = dict()
for y in l:
    try:
        req = req_base+y
        comp = requests.get(req).json()
        j = {**j, **comp}
        time.sleep(.01)
    except:
        f = open("/home/ec2-user/flip/stocks.txt", "w")
        f.write('END')
        f.close()
        sys.exit()


print("Start json unpacking:  ", datetime.datetime.now().time())

run_time = time.time()
#print(j)
dfs = pd.DataFrame.from_dict(j, orient='index')
#print(dfs.head())
stk = pd.DataFrame(dtype=float)
for index, row in dfs.iterrows():
    df = pd.DataFrame(row['quote'], index=[0])
    stk = stk.append(df,ignore_index=True,sort=False)


print("Ended json unpacking:  ", datetime.datetime.now().time())

hl = stk[['symbol','previousClose','high','low']].rename(columns={'previousClose':'pClose'})
hl['pClose'] = hl.pClose.astype(float)
hl['high'] = hl.high.astype(float)
hl['low'] = hl.low.astype(float)
hlp = pd.read_pickle('highlow.pkl')
hlp.to_pickle('highlowPrior.pkl')
cx = pd.read_pickle('supp_closes_last.pkl')
cx = cx[['symbol','low85','low28','low5']]
hl['l85'] = 0
hl['l28'] = 0
hl['l5'] = 0
hl = hl.merge(cx,how='left',on='symbol')
hl.loc[(hl['low']<=hl['low85']),'l85'] = 1
hl.loc[(hl['low']<=hl['low28']),'l28'] = 1
hl.loc[(hl['low']<=hl['low5']),'l5'] = 1
hl['lcnt'] = hl['l85']+hl['l28']+hl['l5']
hl = hl[['symbol','pClose','high','low','l85','l28','l5','lcnt']]
hl.to_pickle('highlow.pkl')

stk = stk[['symbol','latestVolume','latestPrice','high','low']]
stk['latestPrice'] = stk.latestPrice.astype('float32')
stk['latestVolume'] = stk['latestVolume'].fillna(0)
stk['latestVolume'] = stk.latestVolume.astype('uint32')
stk['high'] = stk.high.astype('float32')
stk['low'] = stk.low.astype('float32')
stk = stk[stk['latestPrice'] > 0]
stk = stk.rename(columns={'symbol':'symbol','latestVolume':'volume','latestPrice':'price'})
stk['time'] = run_time


print("Start data evaluation: ", datetime.datetime.now().time())

stks = pd.read_pickle("stocks.pkl")
if stks.empty:
    stks = pd.read_pickle("dummy.pkl")
stks['price'] = stks.price.astype('float32')
stks['volume'] = stks.volume.astype('uint32')
stks['high'] = stks.high.astype('float32')
stks['low'] = stks.low.astype('float32')
stks = stks.append(stk,ignore_index=True,sort=False)
stks.dropna(inplace=True)
stks.drop_duplicates(inplace=True)

#tmin = time.time()-620
#stks = stks[stks['time'] > tmin]

#prev = pd.read_pickle('last.pkl')

stks['time'] = stks['time'].astype('category')
stks['symbol'] = stks['symbol'].astype('category')
stks.to_pickle("stocks.pkl")
stks.to_pickle("xstocks.pkl")


stk['time'] = stk['time'].astype('category')
stk['symbol'] = stk['symbol'].astype('category')
stk.to_pickle("last.pkl")

f = open("/home/ec2-user/flip/hour_start.txt", "r")
hour_start = int(f.readlines()[0])
f.close()

f = open("/home/ec2-user/flip/hour_end.txt", "r")
hour_end = int(f.readlines()[0])
f.close()

if st_time < datetime.time(hour_start,1):
    stk.to_pickle("open.pkl")

if st_time < datetime.time(hour_start,31):
    stk.to_pickle("open.pkl")

#if st_time < datetime.time(hour_end,1):
#        stk.to_pickle("close.pkl")

import gc
del [[stk,stks,j,l,cs,n]]
gc.collect()
stk = pd.DataFrame()
stks = pd.DataFrame()
prev = pd.DataFrame()

f = open("/home/ec2-user/flip/stocks.txt", "w")
f.write('END')
f.close()

print("End of process:        ", datetime.datetime.now().time())
