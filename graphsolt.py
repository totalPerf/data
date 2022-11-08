#!/usr/bin/env python
# coding: utf-8

#import data as d
import pandas as pd
import oracledb
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
from prophet import Prophet
import streamlit as st

USERNAME="prformance_datascience"
USERALGO="prformance_iptv"
PASSALGO="Prformance_Iptv"
PASSWORD="Data#Science"
HOST="10.180.39.252"
PORT=1521
SERVICE="PRFORMNC"

dsn_tns = oracledb.makedsn(HOST, PORT, service_name=SERVICE) 
conn = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=dsn_tns)

def querys(querydata): 
    c = conn.cursor()
    #Get query
    c.execute(querydata)
    #Get columns
    colnames = [x[0] for x in c.description]

    df = pd.DataFrame(c, columns=colnames)
    return df


query = """SELECT fecha, olt, mbps_velocidad
FROM administrator.VM_GPON_LAGS_CAPACITY"""
qcapacity = querys(query)
qcapacity['FECHA'] = qcapacity['FECHA'].astype('datetime64[ns]')
qcapacity.head(2)


qcapacity.rename(columns={'FECHA':'ds', 'MBPS_VELOCIDAD':'y'}, inplace=True)

lista = qcapacity.OLT.unique()

select = st.sidebar.selectbox('Select a OLT',lista)

print(qcapacity.isnull().sum(),qcapacity.isna().sum())
qclean = qcapacity.dropna(how="any",axis=0)
print(qclean.isnull().sum(),qclean.isna().sum())

groupqc = qcapacity.groupby(['OLT','ds','y']).count()

resetgqc = groupqc.reset_index()

def proyec(df,periodo):
    m = Prophet()
    m.fit(df)

    future = m.make_future_dataframe(periods=periodo, include_history=True)
    future.tail()

    forecast = m.predict(future)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periodo))

    fig1 = m.plot(forecast)
    st.pyplot(fig=fig1, clear_figure=None)


graphnew = resetgqc[resetgqc.OLT == select][['ds','y']]
graphnew

proyec(graphnew,365)
