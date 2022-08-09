import csv
import json
from typing import Union

from fastapi import FastAPI
from numpy import rec
import pandas as pd
from pandasql import sqldf
import duckdb
import sys

from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/test/{i}")
def pairs(i:int):
    if (i%2==0):
        return {"result" : "le numéro est pair"}
    else:
        return {"result" : "le numéro est impairs"}


@app.get("/barrages2")
def read_root():
    data= pd.read_csv("barrages.csv")
    js = data.to_json()
    return js 

@app.get("/test2")
def read_root2():
    data= pd.read_csv("barrages.csv")
    js = data.to_json()
    data=json.loads(js)
    print(type(data))
    print(data['Nom_Fr'], data['stock'])
    return data

@app.get("/test")
def read_root4():
    with open("barrages.csv", "w", newline='')as fichier:
        writer=csv.writer(fichier)
        writer.writrow(["Nom_Fr"], ["stock"])
        
    return writer

 
@app.get("/sql")
def sql():
 mysql = lambda q: sqldf(q, globals())
 df=pd.read_csv('barrages.csv')
 mysql("SELECT * FROM df")
 return df

 
@app.get("/barragessql")
def barrages():
    df=pd.read_csv('barrages.csv')
    rec = duckdb.query("SELECT * FROM df").df()
    setsjson = []
    return setsjson

@app.get("/testreq")
def read_rootsql():
  con = duckdb.connect(database=':memory:')
  df = pd.read_csv('barrages.csv')
  #rec = duckdb.query("SELECT * FROM df").df()
  #print(rec)
  setsjson = []
  con.register('test_df_view', df)
  con.execute('SELECT Nom_Fr, max(stock) as stock FROM test_df_view group by Nom_Fr, stock  limit 20')
  rec = con.fetchall()
  new_rec = []
  
  for elt in rec:
   new_rec.append({
       "name" : elt[0],
       "value" : elt[1]
   })
  return new_rec


 
 
