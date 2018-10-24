import pyodbc
import pandas as pd
import numpy as np
from pytictoc import TicToc
import datetime
import os
from patsy import dmatrices,dmatrix
from sklearn.linear_model import LogisticRegression
import statsmodels.discrete.discrete_model as sm
now = datetime.datetime.now()
now_tz=datetime.datetime.now().astimezone().tzinfo
now = pd.Timestamp(now).tz_localize(now_tz)


hist_load_df=pd.read_csv('hist_temp.csv')
hist_load_df['latebooking'] = 0
hist_load_df['latedispatch'] = 0
hist_load_df['preStop_OnTime'] = -1
hist_load_df['preStop_Duration'] = -1
hist_load_df['dist'] = hist_load_df['Empty_Dist']  # initialize as empty dist, and will be updated with nextstopdistance when stopsequence>1
hist_load_df['dispatch_Local'] = now

filteroutid = []
speed_highway = 55
speed_local = 35

for i in range(0,len(hist_load_df)):
        #we need to use iloc[i] not loc[i]. otherwise the sequence for prestop will be messed up.
        if (abs(pd.Timestamp(hist_load_df['Appt'].iloc[i]) - pd.Timestamp(hist_load_df['Arrival'].iloc[i])).days >= 1) or \
                pd.Timestamp(hist_load_df.iloc[0].Arrival).strftime('%Y') == '1753':
            filteroutid.append(hist_load_df['LoadID'].iloc[i])
            continue
        loadstop = hist_load_df.iloc[i]
        localtz = loadstop['tz']
        if loadstop['StopSequence'] > 1:
            # prestop=hist_load_df.loc[i-1]
            hist_load_df['dist'].iloc[i] = hist_load_df['NextStopDistance'].iloc[i - 1]
            hist_load_df['preStop_OnTime'].iloc[i] = hist_load_df['ontime'].iloc[i]
            duration = hist_load_df['duration'].iloc[i - 1]
            # hist_load_df['preStop_Duration'].iloc[i] = np.where(duration > 0 and duration < 360, duration, \
            #                                           hist_load_df['hist_Duration'].iloc[i - 1]).tolist()
            hist_load_df['preStop_Duration'].iloc[i] = duration if  duration > 0 and duration < 360 else  hist_load_df['hist_Duration'].iloc[i - 1]
            travel_hour = pd.Timestamp(hist_load_df['Arrival'].iloc[i]) - pd.Timestamp(hist_load_df['Departure'].iloc[i - 1])
            travel_hour = travel_hour.days * 24 + travel_hour.seconds / 3600  # note there may be another way using astype() same with duration calculation
            if hist_load_df['dist'].iloc[i] > speed_highway * travel_hour:  # assume daily miles, 600mile
                filteroutid.append(hist_load_df['LoadID'].iloc[i])
        else:
            #print (i)
            # speed= np.where( hist_load_df.loc[i]['dist'] < 100, speed_local, speed_highway)
            dispatch = pd.Timestamp(hist_load_df['Dispatch_Time'].iloc[i],tz='UTC').tz_convert(localtz)
            Appt = pd.Timestamp(hist_load_df['Appt'].iloc[i],tz=localtz)
            #Appt= pd.Timestamp(hist_load_df.loc[i]['Appt'],tz=localtz)
            Empty_Time = pd.Timestamp(hist_load_df['Empty_Time'].iloc[i],tz=localtz)
            BookTime = pd.Timestamp(hist_load_df['BookTimeUTC'].iloc[i],tz='UTC').tz_convert(localtz)
            if dispatch - Appt >= datetime.timedelta(0,0,0):
                dispatch = Empty_Time
            hist_load_df['dispatch_Local'].iloc[i] = dispatch
            leadtime = Appt - BookTime
            lead_dispatch = Appt - dispatch
            # hist_load_df['latebooking'].iloc[i] = np.where((leadtime.days * 24 * 60 + leadtime.seconds / 60) < 241, 1, 0).tolist()
            # hist_load_df['latedispatch'].iloc[i] = np.where(
            #     (lead_dispatch.days * 24 * 60 + lead_dispatch.seconds / 60) < 121, 1, 0).tolist()
            hist_load_df['latebooking'].iloc[i] = 1 if leadtime.days * 24 * 60 + leadtime.seconds / 60 < 241 else 0
            hist_load_df['latedispatch'].iloc[i] = 1 if lead_dispatch.days * 24 * 60 + lead_dispatch.seconds / 60 < 121 else 0
            hist_load_df['preStop_OnTime'].iloc[i] = 1 - hist_load_df['latedispatch'].iloc[i]

hist_load_select = hist_load_df[~hist_load_df['LoadID'].isin(filteroutid)]
hist_load_select.to_csv("histdata" + now.strftime("%Y%m%d") + '.csv', index=False)
