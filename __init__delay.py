import pyodbc
import pandas as pd
import numpy as np
import math
import datetime
import os
from patsy import dmatrices,dmatrix
from sklearn.linear_model import LogisticRegression
import statsmodels.discrete.discrete_model as sm

# class sklearn.linear_model.LogisticRegression

now = datetime.datetime.now()
now_tz=datetime.datetime.now().astimezone().tzinfo
now = pd.Timestamp(now).tz_localize(now_tz)

#import matplotlib.pyplot as plt
#from statsmodels.distributions.empirical_distribution import ECDF

# dr="D:/ServiceDisruption/Zihan_Data/Model_SQL_R/TaskScheduler/Auto_result/DelayPredictor/"
# today=now.strftime("%b%d")
# lastday=(now+datetime.timedelta(-1)).strftime("%b%d")
# path=dr+today
# if not os.path.exists(path):
#     os.makedirs(path)
# os.chdir(path)

#===========================================================
#Data Preprocessing
#===========================================================
timezones = {'Timezone': [-5, -6, -7, -12, -8, -9, -10],
             'tz': ['America/New_York', 'America/Chicago', 'America/Denver', 'America/Phoenix',
                    'America/Los_Angeles', 'America/Anchorage', 'Pacific/Honolulu']}
timezones_df = pd.DataFrame(data=timezones)

def Process_histdata(hist_load_df):
    hist_load_df['latebooking'] = 0
    hist_load_df['latedispatch'] = 0
    hist_load_df['preStop_OnTime'] = -1
    hist_load_df['preStop_Duration'] = -1
    hist_load_df['dist'] = hist_load_df[
        'Empty_Dist']  # initialize as empty dist, and will be updated with nextstopdistance when stopsequence>1
    hist_load_df['dispatch_Local'] = now
    # hist_load_df['DoW'] = hist_load_df['Appt'].strftime("%w")
    filteroutid = []
    speed_highway = 55
    speed_local = 35
    # detention = 2
    # workin_hour = 4
    for i in hist_load_df.index:
        if (abs(pd.Timestamp(hist_load_df['Appt'].iloc[i]) - pd.Timestamp(
                hist_load_df['Arrival'].iloc[i])).days >= 1) or hist_load_df.loc[
            0].Arrival.strftime('%Y') == '1753':
            filteroutid.append(hist_load_df.loc[i]['LoadID'])
            continue
        loadstop = hist_load_df.loc[i]
        localtz = loadstop['tz']
        if loadstop['StopSequence'] > 1:
            # prestop=hist_load_df.loc[i-1]
            hist_load_df['dist'].iloc[i] = hist_load_df['NextStopDistance'].iloc[i - 1]
            hist_load_df['preStop_OnTime'].iloc[i] = hist_load_df['ontime'].iloc[i]
            duration = hist_load_df['duration'].iloc[i - 1]
            # hist_load_df['preStop_Duration'].iloc[i] = np.where(duration > 0 and duration < 360, duration, \
            #                                           hist_load_df['hist_Duration'].iloc[i - 1]).tolist()[0]
            hist_load_df['preStop_Duration'].iloc[i] = duration if duration > 0 and duration < 360 else hist_load_df['hist_Duration'].iloc[i - 1]
            travel_hour = pd.Timestamp(hist_load_df['Arrival'].iloc[i]) - pd.Timestamp(
                hist_load_df['Departure'].iloc[i - 1])
            travel_hour = travel_hour.days * 24 + travel_hour.seconds / 3600  # note there may be another way using astype() same with duration calculation
            if hist_load_df['dist'].iloc[i] > speed_highway * travel_hour:  # assume daily miles, 600mile
                filteroutid.append(hist_load_df['LoadID'].iloc[i])
        else:
            # print (i)
            hist_load_df['dist'].loc[i]<-hist_load_df['Empty_Dist'].loc[i]
            speed= speed_local if  hist_load_df.loc[i]['dist'] < 100 else speed_highway
            hist_load_df['preStop_Duration'].iloc[i] = hist_load_df['dist'].loc[i]/speed
            dispatch = pd.Timestamp(hist_load_df['Dispatch_Time'].iloc[i], tz='UTC').tz_convert(localtz)
            Appt = pd.Timestamp(hist_load_df['Appt'].iloc[i], tz=localtz)
            # Appt= pd.Timestamp(hist_load_df.loc[i]['Appt'],tz=localtz)
            Empty_Time = pd.Timestamp(hist_load_df['Empty_Time'].iloc[i], tz=localtz)
            BookTime = pd.Timestamp(hist_load_df['BookTimeUTC'].iloc[i], tz='UTC').tz_convert(localtz)
            if dispatch - Appt >= datetime.timedelta(0, 0, 0):
                dispatch = Empty_Time
            hist_load_df['dispatch_Local'].iloc[i] = dispatch
            leadtime = Appt - BookTime
            lead_dispatch = Appt - dispatch
 
            # hist_load_df['latebooking'].iloc[i] = np.where((leadtime.days * 24 * 60 + leadtime.seconds / 60) < 241, 1, 0).tolist()
            # hist_load_df['latedispatch'].iloc[i] = np.where(
            #     (lead_dispatch.days * 24 * 60 + lead_dispatch.seconds / 60) < 121, 1, 0).tolist()
            hist_load_df['latebooking'].iloc[i] = 1 if leadtime.days * 24 * 60 + leadtime.seconds / 60 < 241 else 0
            hist_load_df['latedispatch'].iloc[
                i] = 1 if lead_dispatch.days * 24 * 60 + lead_dispatch.seconds / 60 < 121 else 0
            hist_load_df['preStop_OnTime'].iloc[i] = 1 - hist_load_df['latedispatch'].iloc[i]        
    hist_load_select = hist_load_df[~hist_load_df['LoadID'].isin(filteroutid)]
    hist_load_select.to_csv("histdata" + now.strftime("%Y%m%d") + '.csv', index=False)
    return hist_load_select

def Get_Histload_ALL():
    # if __name__ == "__main__":
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query_randomloads = """
        DECLARE @Date AS DATE = GETDATE()

        IF OBJECT_ID('tempdb..#LdSet') is not null drop table #LdSet;
        CREATE TABLE #LdSet(LoadID INT, Prt VARCHAR(2))
        INSERT INTO #LdSet
        SELECT
        L.[ID] 'LoadID'
        ,CASE WHEN loaddate between DATEADD(day,-15, @date) and @date THEN 'A'
                 WHEN loaddate between DATEADD(day,-30, @date) and DATEADD(day,-16, @date) THEN 'B'
                 ELSE 'C' END
        FROM [Bazooka].[dbo].[Load] L 
        INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
        INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
        INNER JOIN Bazooka.dbo.City CO ON CO.ID = L.OriginCityID
        INNER JOIN Bazooka.dbo.City CD ON CD.ID = L.DestinationCityID 
        where
        Mode = 1 and StateType = 1 and L.ProgressType>=7
        and convert(date,loaddate) between DATEADD(day,-60, @date) and @date
        and (CO.StateID <= 51 AND CD.StateID <= 51)
        AND  L.ShipmentType not in (3,4,6,7)  
        and CUS.[Name] not like '%sears%'
        and CUS.[Name] not like '%upsds%' 
        and CUS.[Name] not like 'UPS%' ORDER BY NEWID()


        CREATE CLUSTERED INDEX IX_LdSet ON #LdSet(LoadID, Prt);

        SELECT * FROM

        (
        SELECT  top 25000
        L.LoadID
        FROM #LdSet L
        WHERE L.Prt = 'A'
        ORDER BY NEWID()
        ) A

        UNION

        Select * FROM
        (SELECT  top 5000
        L.LoadID
        FROM #LdSet L
        WHERE L.Prt = 'B'
        ORDER BY NEWID() )B

        UNION

        Select * FROM
        (SELECT  top 2000
        L.LoadID
        FROM #LdSet L
        WHERE L.Prt = 'C'
        ORDER BY NEWID() )C

        """
    query_histload = """
    set nocount on
    declare @date as date = GETDATE() 
    If(OBJECT_ID('tempdb..#Load_ID_Z') Is Not Null)
    Begin
    Drop Table #Load_ID_Z
    End
    Create Table #Load_ID_Z (LoadID int)
    Insert into #Load_ID_Z (LoadID)

    SELECT  
    L.[ID] 'LoadID'
    FROM [Bazooka].[dbo].[Load] L 
    INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
    INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
    INNER JOIN Bazooka.dbo.City CO ON CO.ID = L.OriginCityID
    INNER JOIN Bazooka.dbo.City CD ON CD.ID = L.DestinationCityID 
    where
    Mode = 1 and StateType = 1 and L.ProgressType>=7
    and convert(date,loaddate) between DATEADD(day,-90, @date) and @date
    and (CO.StateID <= 51 AND CD.StateID <= 51)
    AND  L.ShipmentType not in (3,4,6,7)  
    and CUS.[Name] not like '%sears%'
    and CUS.[Name] not like '%upsds%' 
    and CUS.[Name] not like 'UPS%'    


    --If(OBJECT_ID('tempdb..#HistLoad_Z ') Is Not Null)
    --Begin
    --Drop Table #HistLoad_Z 
    --End
    --Create Table #HistLoad_Z  (LoadID int, Loadtype varchar(10), CustomerID int, NumStops int, ProgressType int, StopSequence int, NextStopDistance float, FacilityID int, CityID int, Statecode varchar(10), Timezone int, PDType int, Code varchar(10), Appt datetime, CarrierID int,Empty_Dist float, Empty_Time datetime, Dispatch_Time datetime, Arrival datetime, Departure datetime,  BookTimeUTC datetime )
    --Insert into #HistLoad_Z   

    select  
    L.[ID] 'LoadID'
    ,convert(date,loaddate) 'LoadDate'
    ,case when L.[RoutingRankType] = 203 then 'S' else 'C' end 'Loadtype'
    ,CUS.ID 'CustomerID'
    ,L.NumStops
    ,L.ProgressType
    ,LS.[Sequence] 'StopSequence'
    ,LS.[NextStopDistance]
    ,LS.[FacilityID]
    ,LS.CityID
    ,LS.StateCode
    ,case when LS.StateCode like 'AZ' then -12 
    when LS.StateCode in ('AL', 'MN', 'MO','MS','WI') then -6
    when LS.StateCode in ('NJ', 'NY', 'OH','PA','SC','NC') then -5
    when LS.StateCode in ('NV','WY') then -7
    when LS.StateCode like 'CA' then -8
    when LS.StateCode like 'FL' and CT.TimeZoneOffset=-6 and CT.Longitude>-84 then -5
    when LS.StateCode like 'ID' and CT.TimeZoneOffset=-8 and CT.Latitude<45 then -7
    when LS.StateCode like 'IN' and CT.TimeZoneOffset=-6 and CT.Latitude>39 and CT.Longitude>-86.46 then -5
    when LS.StateCode like 'KY' and CT.TimeZoneOffset=-6 and CT.Latitude>37.5 and CT.Longitude>-86 then -5          
    when LS.StateCode like 'MI' and (CT.Longitude>-85 or (CT.Longitude>-88 and CT.Latitude>46) or CT.TimeZoneOffset=-4 )  then -5   		
    when LS.StateCode like 'ND' and CT.TimeZoneOffset=-7 and  CT.Longitude>-100 then -6  
    when LS.StateCode like 'NE' and CT.TimeZoneOffset=-7 and  CT.Longitude>-101 then -6   
    when LS.StateCode like 'TN' and CT.TimeZoneOffset=-5 and  CT.Longitude<-85.5 then -6  
    when LS.StateCode like 'TX' and CT.TimeZoneOffset=-7 and  CT.Longitude<-105 then -6
    else  CT.TimeZoneOffset end 'Timezone'
    ,case when LS.[Type] = 1 then 1 else 0 end 'PDType'
    ,case when LS.[ScheduleType] = 1 then 'Appt' when LS.[ScheduleType] = 2 then 'Notice' when LS.[ScheduleType] = 3 then 'Open' 
    else case when LS.[ScheduleOpenTime] = LS.[ScheduleCloseTime] then 'Appt'else 'Open' end
    end 'Code'
    ,case when LS.[ScheduleCloseTime] = '1753-01-01' then 
    case when LS.[Type]=1 then convert(datetime, CONVERT(date, LS.LoadByDate)) + convert(datetime, CONVERT(time, ls.CloseTime)) 
    else convert(datetime, CONVERT(date, LS.DeliverByDate)) + convert(datetime, CONVERT(time, ls.CloseTime)) end
    else LS.[ScheduleCloseTime] end 'Appt'
    ,LC.[CarrierID]
    ,case when LC.ActualDistance<=0.0 then 1.0 else LC.ActualDistance end   'Empty_Dist'
    ,LC.ActualDateTime   'Empty_Time'
    --,LC.[DispatchDateTime] 'Dispatch_Time'
    ,LT.[EntryDateTimeUTC] 'Dispatch_Time'
    ,LS.[ArriveDateTime]  'Arrival'
    ,LS.[DepartDatetime]  'Departure'
    ,LC.[CreateDateUTC] 'BookTimeUTC'
    FROM [Bazooka].[dbo].[Load] L
    INNER JOIN Bazooka.dbo.Loadstop LS on LS.LoadID=L.ID
    INNER JOIN Bazooka.dbo.LoadCarrier LC on LC.LoadID = L.ID and LC.Main = 1 and LC.IsBounced = 0
    INNER JOIN [Bazooka].[dbo].[LoadTracking] LT ON LT.LoadID=L.ID  
    INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
    INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
    LEFT JOIN bazooka.dbo.Conference CF ON Cf.ID = L.ConferenceID
    left join bazooka.dbo.City CT on LS.Cityid=CT.ID
    where  
    L.id in (select distinct * from #Load_ID_Z )
    and L.ID not in (select distinct LoadID from Bazooka.dbo.LoadStop where [Type] = 11)
    and L.ID not in (select distinct loadID from Bazooka.dbo.LoadStop where [NextStopDistance]<0)
    and L.ID not in (select distinct loadID from Bazooka.dbo.LoadStop where LS.[ArriveDateTime] ='1753-01-01' or LS.[DepartDatetime] ='1753-01-01')
    and LT.[EntryDateTimeUTC] in (select max(LT.[EntryDateTimeUTC])  
                     from bazooka.dbo.loadtracking LT
                     where [Action] = 1  and  LT.LoadID  in (select * from #Load_ID_Z)
                     group by LT.[LoadID] )
    order by L.id , StopSequence
    """
    hist_load = pd.read_sql(query_histload, cn)
    hist_load_id = pd.read_sql(query_randomloads, cn)

    hist_load['ontime'] = np.where((hist_load.Arrival - hist_load.Appt) <= datetime.timedelta(0, 3600), 1, 0)
    hist_load['duration'] = (hist_load.Departure - hist_load.Arrival).astype('timedelta64[m]')  # duration in minute

    hist_frate = hist_load.groupby(['FacilityID', 'PDType'])['ontime'].agg(['mean', 'count']).reset_index()
    hist_frate.columns = ['FacilityID', 'PDType', 'f_rate', 'fvolume']
    hist_fduration = hist_load.groupby(['FacilityID', 'PDType'])['duration'].agg(['mean']).reset_index()
    hist_fduration.columns = ['FacilityID', 'PDType', 'hist_Duration']
    hist_load_f = pd.merge(left=hist_frate, right=hist_fduration, left_on=['FacilityID', 'PDType'],
                           right_on=['FacilityID', 'PDType'], how='left')
    hist_load_all = pd.merge(left=hist_load, right=hist_load_f[hist_load_f.fvolume > 10],
                             left_on=['FacilityID', 'PDType'],
                             right_on=['FacilityID', 'PDType'], how='left')

    hist_cityrate = hist_load.groupby(['CityID', 'PDType'])['ontime'].agg(['mean', 'count']).reset_index()
    hist_cityrate.columns = ['CityID', 'PDType', 'cityrate', 'cityvolume']
    hist_cityduration = hist_load.groupby(['CityID', 'PDType'])['duration'].agg(['mean']).reset_index()
    hist_cityduration.columns = ['CityID', 'PDType', 'city_duration']
    hist_load_city = pd.merge(left=hist_cityrate, right=hist_cityduration, left_on=['CityID', 'PDType'],
                              right_on=['CityID', 'PDType'], how='left')
    hist_load_all = pd.merge(left=hist_load_all, right=hist_load_city[hist_load_city.cityvolume > 10],
                             left_on=['CityID', 'PDType'],
                             right_on=['CityID', 'PDType'], how='left')

    hist_carrierrate = hist_load.groupby(['CarrierID', 'PDType'])['ontime'].agg(['mean', 'count']).reset_index()
    hist_carrierrate.columns = ['CarrierID', 'PDType', 'carrier_rate', 'carrier_volume']
    hist_custrrate = hist_load.groupby(['CustomerID', 'PDType'])['ontime'].agg(['mean', 'count']).reset_index()
    hist_custrrate.columns = ['CustomerID', 'PDType', 'cust_rate', 'cust_volume']
    hist_load_all = pd.merge(left=hist_load_all, right=hist_carrierrate[hist_carrierrate.carrier_volume > 10],
                             left_on=['CarrierID', 'PDType'], right_on=['CarrierID', 'PDType'], how='left')
    hist_load_all = pd.merge(left=hist_load_all, right=hist_custrrate[hist_custrrate.cust_volume > 10],
                             left_on=['CustomerID', 'PDType'], right_on=['CustomerID', 'PDType'], how='left')

    hist_load_all['f_rate'].fillna(hist_load_all['cityrate'], inplace=True)
    hist_load_all['f_rate'].fillna(hist_load_all['f_rate'].mean(axis=0), inplace=True)
    hist_load_all['hist_Duration'].fillna(hist_load_all['city_duration'], inplace=True)
    hist_load_all['hist_Duration'].fillna(hist_load_all['duration'].mean(axis=0), inplace=True)
    hist_load_all['carrier_rate'].fillna(hist_load_all['carrier_rate'].mean(axis=0), inplace=True)
    hist_load_all['cust_rate'].fillna(hist_load_all['cust_rate'].mean(axis=0), inplace=True)

    hist_load_select = pd.merge(left=hist_load_all[hist_load_all['LoadID'].isin(hist_load_id.LoadID.tolist())],
                                right=timezones_df,
                                left_on=['Timezone'], right_on=['Timezone'], how='left')
    # hist_load_all.Appt=hist_load_all.Appt.tz_localize(hist_load_all.tz)

    # hist_load_all['Empty_Time']=hist_load_all['Empty_Time'].astimezone(hist_load_all['tz'])

    hist_load_df = hist_load_select[
        ['LoadID', 'Loadtype', 'CarrierID', 'CustomerID', 'FacilityID', 'NumStops', 'ProgressType',
         'StopSequence', 'NextStopDistance', 'StateCode', 'tz', 'PDType', 'Code', 'Appt',
         'Empty_Time', 'Dispatch_Time', 'Arrival', 'Departure', 'BookTimeUTC', 'Empty_Dist',
         'ontime', 'duration', 'hist_Duration', 'f_rate', 'carrier_rate', 'cust_rate']].sort_values(
        by=['LoadID', 'StopSequence'])

    hist_load_df.to_csv("hist_temp.csv", index=False)
    ## This part (ratio) can be set within SQL and saved in SQL to update every week.
    hist_load_f.to_csv('hist_load_f.csv', index=False)
    hist_load_city.to_csv('hist_load_city.csv', index=False)
    hist_custrrate.to_csv('hist_custrrate.csv', index=False)
    hist_carrierrate.to_csv('hist_carrierrate.csv', index=False)
    hist_load_data = Process_histdata(hist_load_df)
    return (hist_load_data)

def HOS_checking(prestop, stop, travtime):
    # travtime unit in seconds
    speed_highway = 55
    speed_local = 35
    travtime = travtime / 3600  # change unit from secs to hrs
    empty_dist = min(prestop.Empty_Dist, 200)
    # b/c 200 from quantile(histdata$Empty_Dist, 0.98)
    speed = speed_local if empty_dist < 100 else speed_highway

    travtime_empty = empty_dist / speed  # unit in hrs

    prestop_tz = prestop['tz']
    stop_tz = stop.tz

    if   prestop['arrived']==1:
        prestop_start = pd.Timestamp(prestop['Arrival'],tz=prestop_tz)

    else:
        prestop_start = pd.Timestamp(prestop['Appt'],tz=prestop_tz)

    if (stop['StopSequence'] == 2):
        loadempty = pd.Timestamp(prestop['Empty_Time'],tz=prestop_tz)

        if (prestop_start - loadempty) >= datetime.timedelta(0, 3600 * 10):
            onduty_start = prestop_start
        # travtime_empty<-0
        else:
            onduty_start = loadempty
    else:
        onduty_start = prestop_start

    if prestop['Departure'].strftime("%Y") != '1753':
        prestop_end = pd.Timestamp(prestop['Departure'],tz=prestop_tz)
        onduty_stop = (prestop_end - onduty_start).days * 24 * 60 + (prestop_end - onduty_start).seconds / 60
        # all units are in minutes
    else:
        onduty_stop = (prestop_start - onduty_start).days * 24 * 60 + (prestop_start - onduty_start).seconds / 60 + \
                      prestop['hist_Duration']
        # all units are in minutes

        prestop_end = prestop_start + datetime.timedelta(0, prestop[
            'hist_Duration'] * 60)  # time + a number should be in unit of seconds

    mealtime = travtime / 4.5 * 40 / 60  # meal time 40 minutes per 4.5 hour; unit in hrs

    onduty_time = np.ceil(onduty_stop / 60 + travtime + mealtime + travtime_empty)  # unit in hrs

    # onduty_stop unit in mins, travtime unit in secs, onduty_time unit in hours
    HOS = 0

    if (onduty_time >= 14 and onduty_time < 28) or (np.ceil(travtime) >= 11 and np.ceil(travtime) < 22):
        HOS = 1
    elif (onduty_time >= 28 and onduty_time < 42) or (np.ceil(travtime) >= 22 and np.ceil(travtime) < 33):
        HOS = 2
    elif (onduty_time >= 42 and onduty_time < 56) or (np.ceil(travtime) >= 33 and np.ceil(travtime) < 44):
        HOS = 3
    elif (onduty_time >= 56 and onduty_time < 70) or (np.ceil(travtime) >= 44 and np.ceil(travtime) < 55):
        HOS = 4
    elif (onduty_time >= 70 and onduty_time < 84) or (np.ceil(travtime) >= 55 and np.ceil(travtime) < 66):
        HOS = 5
    Reason = ""

    if  stop['arrived']  == 0:
        ETA = (datetime.timedelta(0, (HOS * 10 + travtime + mealtime) * 3600) + prestop_end).tz_convert(stop_tz)
        if ETA - pd.Timestamp(stop['Appt'],tz=stop_tz) > datetime.timedelta(0.3600):
            if (HOS > 0):
                Reason = "Scheduling/HOS--ETA_Delay"
            else:
                Reason = "ETA_Delay"
    else:
        ETA = pd.Timestamp(stop['Arrival'],tz=stop_tz)
    ETA=ETA.tz_convert(stop_tz)
    return (ETA, Reason)

def Get_Dynamic_load():
    # might be no difference between iloc and loc, as we have order by in sql query. but need to be verified
    # use iloc will guarantee that the sequence of load and stopsequence that we want and we arranged.
    hist_load_f = pd.read_csv('hist_load_f.csv')
    hist_load_city = pd.read_csv('hist_load_city.csv')
    hist_custrrate = pd.read_csv('hist_custrrate.csv')
    hist_carrierrate = pd.read_csv('hist_carrierrate.csv')
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    load_query = """
        set nocount on
        If(OBJECT_ID('tempdb..#Load_ID_Z') Is Not Null)
        Begin
        Drop Table #Load_ID_Z
        End
        Create Table #Load_ID_Z (LoadID int)
        Insert into #Load_ID_Z (LoadID)

        SELECT distinct 
        L.[ID] 'LoadID'
        FROM [Bazooka].[dbo].[Load] L
        --inner join Bazooka.dbo.Loadstop LS on LS.LoadID=L.ID
        inner join Bazooka.dbo.LoadCarrier LC on LC.LoadID = L.ID and LC.Main = 1 and LC.IsBounced = 0
        INNER JOIN Bazooka.dbo.City CO ON CO.ID = L.OriginCityID
        INNER JOIN Bazooka.dbo.City CD ON CD.ID = L.DestinationCityID
        INNER JOIN [Bazooka].[dbo].[LoadTracking] LT ON LT.LoadID=L.ID  
        where  
        (CO.StateID <= 51 AND CD.StateID <= 51) --Domestic USA Only--
        and Mode = 1 and StateType = 1 and L.ProgressType<6
    	    and convert(date,loaddate) between DATEADD(day, -7, getdate())  and DATEADD(day,2, getdate()) 
      --  and convert(date,loaddate) between convert(date,GETDATE()) and  DATEADD(day,2, getdate()) 
        AND  L.ShipmentType not in (3,4,6,7) 
        and L.ID not in (select distinct LoadID from Bazooka.dbo.LoadStop where [Type] = 11)
        and L.ID not in (select distinct loadID from Bazooka.dbo.LoadStop where [NextStopDistance]<0)

        SELECT 
        L.[ID] 'LoadID'
        ,L.LoadDate
        ,case when L.[RoutingRankType] = 203 then 'S' else 'C' end 'Loadtype'
        ,CUS.[ID]    'CustomerID'
        ,CUS.[Name]   'Customer'
        --,COALESCE(Cf.[Description],'unknown') 'Conference'
    	,case when CAR.ContractVersion like 'TMS FILE' or  CAR.ContractVersion like 'UPS%' then 'Managed' else 'Brokered' end 'Load_M_B'
        ,L.NumStops
        ,L.ProgressType
        ,LS.[Sequence]   'StopSequence'
        ,LS.[NextStopDistance]
        ,LS.[FacilityID]
        ,LS.CityID
        ,LS.StateCode
        ,case when LS.StateCode like 'AZ' then -12 
        when LS.StateCode in ('AL', 'MN', 'MO','MS','WI') then -6
        when LS.StateCode in ('NJ', 'NY', 'OH','PA','SC') then -5
        when LS.StateCode in ('NV','WY') then -7
        when LS.StateCode like 'CA' then -8
        when LS.StateCode like 'FL' and CT.TimeZoneOffset=-6 and CT.Longitude>-84 then -5
        when LS.StateCode like 'ID' and CT.TimeZoneOffset=-8 and CT.Latitude<45 then -7
        when LS.StateCode like 'IN' and CT.TimeZoneOffset=-6 and CT.Latitude>39 and CT.Longitude>-86.46 then -5
        when LS.StateCode like 'KY' and CT.TimeZoneOffset=-6 and CT.Latitude>37.5 and CT.Longitude>-86 then -5          
        when LS.StateCode like 'MI' and (CT.Longitude>-85 or (CT.Longitude>-88 and CT.Latitude>46) or CT.TimeZoneOffset=-4 )  then -5   		
        when LS.StateCode like 'ND' and CT.TimeZoneOffset=-7 and  CT.Longitude>-100 then -6  
        when LS.StateCode like 'NE' and CT.TimeZoneOffset=-7 and  CT.Longitude>-101 then -6   
        when LS.StateCode like 'TN' and CT.TimeZoneOffset=-5 and  CT.Longitude<-85.5 then -6  
        when LS.StateCode like 'TX' and CT.TimeZoneOffset=-7 and  CT.Longitude<-105 then -6
        else  CT.TimeZoneOffset end 'Timezone'
        ,case when LS.[Type] = 1 then 1 else 0 end 'PDType'
        ,case when LS.[ScheduleType] = 1 then 'Appt' when LS.[ScheduleType] = 2 then 'Notice' when LS.[ScheduleType] = 3 then 'Open' 
        else case when LS.[ScheduleOpenTime] = LS.[ScheduleCloseTime] then 'Appt'else 'Open' end
        end 'Code'
        ,case when LS.[ScheduleCloseTime] = '1753-01-01' then 
        case when LS.[Type]=1 then convert(datetime, CONVERT(date, LS.LoadByDate)) + convert(datetime, CONVERT(time, ls.CloseTime)) 
        else convert(datetime, CONVERT(date, LS.DeliverByDate)) + convert(datetime, CONVERT(time, ls.CloseTime)) end
        else LS.[ScheduleCloseTime] end 'Appt'
        ,LC.[CarrierID]
        ,case when LC.ActualDistance<=0.0 then 1.0 else LC.ActualDistance end   'Empty_Dist'
        ,LC.ActualDateTime   'Empty_Time'
        ,LT.[EntryDateTimeUTC] 'Dispatch_Time'
        ,LS.[ArriveDateTime]  'Arrival'
        ,LS.[DepartDatetime]  'Departure'
        ,[BookByDateTimeUTC] 'BookTimeUTC'
        FROM [Bazooka].[dbo].[Load] L
        inner join Bazooka.dbo.Loadstop LS on LS.LoadID=L.ID
        inner join Bazooka.dbo.LoadCarrier LC on LC.LoadID = L.ID and LC.Main = 1 and LC.IsBounced = 0
        INNER JOIN [Bazooka].[dbo].[LoadTracking] LT ON LT.LoadID=L.ID 
        INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
        INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
        LEFT JOIN bazooka.dbo.Conference CF ON Cf.ID = L.ConferenceID 
        LEFT JOIN bazooka.dbo.City CT on LS.Cityid=CT.ID
    	left join Bazooka.dbo.Carrier CAR on CAR.ID = LC.CarrierID 
        where
        L.id in (select * from #Load_ID_Z )
        and CUS.[Name] not like '%sears%'
        and CUS.[Name] not like '%upsds%'
        and CUS.[Name] not like 'UPS%'
        and LT.[EntryDateTimeUTC] in (
        select max(LT.[EntryDateTimeUTC])  
        from bazooka.dbo.loadtracking LT
        where [Action] = 1  and  LT.LoadID in (select * from #Load_ID_Z )
        group by LT.[LoadID]     )
        order by L.id , StopSequence
        """
    daily_load = pd.read_sql(load_query, cn)
    daily_load['ontime'] = np.where((daily_load.Arrival - daily_load.Appt) <= datetime.timedelta(0, 3600), 1, 0)

    daily_load['duration'] = (daily_load.Departure - daily_load.Arrival).astype('timedelta64[m]')  # duration in minute

    daily_load = pd.merge(left=daily_load, right=hist_load_f[hist_load_f.fvolume > 10],
                          left_on=['FacilityID', 'PDType'],
                          right_on=['FacilityID', 'PDType'], how='left')
    daily_load = pd.merge(left=daily_load, right=hist_load_city[hist_load_city.cityvolume > 10],
                          left_on=['CityID', 'PDType'],
                          right_on=['CityID', 'PDType'], how='left')
    daily_load = pd.merge(left=daily_load, right=hist_carrierrate[hist_carrierrate.carrier_volume > 10],
                          left_on=['CarrierID', 'PDType'], right_on=['CarrierID', 'PDType'], how='left')
    daily_load = pd.merge(left=daily_load, right=hist_custrrate[hist_custrrate.cust_volume > 10],
                          left_on=['CustomerID', 'PDType'], right_on=['CustomerID', 'PDType'], how='left')

    daily_load['f_rate'].fillna(daily_load['cityrate'], inplace=True)
    daily_load['f_rate'].fillna(daily_load['f_rate'].mean(axis=0), inplace=True)
    daily_load['hist_Duration'].fillna(daily_load['city_duration'], inplace=True)
    daily_load['hist_Duration'].fillna(daily_load['duration'].mean(axis=0), inplace=True)
    daily_load['carrier_rate'].fillna(daily_load['carrier_rate'].mean(axis=0), inplace=True)
    daily_load['cust_rate'].fillna(daily_load['cust_rate'].mean(axis=0), inplace=True)

    daily_load_select = pd.merge(left=daily_load, right=timezones_df,
                                 left_on=['Timezone'], right_on=['Timezone'], how='left')

    daily_load_select.to_csv("temp_daily" + now.strftime("%Y%m%d") + '.csv', index=False)
    daily_load_select.to_csv("temp_daily.csv", index=False)

    # columnnames=daily_load_select.columns.tolist()
    # daily_load_df=pd.DataFrame(columns=columnnames )

    daily_load_df = daily_load_select
    daily_load_df['latebooking'] = 0
    daily_load_df['latedispatch'] = 0
    daily_load_df['preStop_OnTime'] = -1
    daily_load_df['preStop_Duration'] = -1
    daily_load_df['dist'] = daily_load_df[
        'Empty_Dist']  # initialize as empty dist, and will be updated with nextstopdistance when stopsequence>1
    daily_load_df['dispatch_Local'] = now
    # daily_load_df['DoW'] = daily_load_df['Appt'].strftime("%w")
    daily_load_df['ETA_ontime'] = -1
    daily_load_df['ETA'] = now
    daily_load_df['Reason'] = ""
    daily_load_df['arrived']=1
    daily_load_df.Customer = daily_load_df.Customer.replace(",", "")
    filteroutid = []
    speed_highway = 55
    speed_local = 35
    detention = 2
    workin_hour = 4

    #for i in daily_load_df.index:
    # not sure if we really need iloc. because loc. and iloc might be equal due to the order by in sql query
    # but it may be safer to use iloc
    #for i in daily_load_df.index:
    for i in range(0, len(daily_load_df)):
        # if(abs(daily_load_df.loc[i]['Appt']-daily_load_df.loc[i]['Arrival']).days>=1 ) :
        #     filteroutid.append(daily_load_df.loc[i]['LoadID'])
        #     continue
        loadstop = daily_load_df.iloc[i]
        localtz = loadstop['tz']
        localAppt = pd.Timestamp(daily_load_df['Appt'].iloc[i], tz=localtz)
        localArri = pd.Timestamp(daily_load_df['Arrival'].iloc[i], tz=localtz)
        daily_load_df['Appt'].iloc[i]=daily_load_df['Appt'].iloc[i].tz_localize(localtz)
        daily_load_df['Arrival'].iloc[i]=daily_load_df['Arrival'].iloc[i].tz_localize(localtz)
        now_local = now.tz_convert(localtz)
        daily_load_df['ETA'].iloc[i]= now_local
        if pd.Timestamp(daily_load_df['Arrival'].iloc[i]).strftime("%Y") == '1753':
            daily_load_df['ontime'].iloc[i] = 0 if now_local - localAppt > datetime.timedelta(0, 3600) else -1
            daily_load_df['arrived'].iloc[i]=0
            ### If we want to assign value, .loc[i] should be after the column name
            daily_load_df['Reason'].iloc[i] = "DataMising-OverDue" if now_local - localAppt > datetime.timedelta(0,
                                                                                                                3600) else ""
            # daily_load_df.loc[i]['ontime']=np.where ((pd.Timestamp(now).tz_localize(now_tz) - localAppt)
            #                                          > datetime.timedelta(0, 3600), 0, -1  )
            # daily_load_df.loc[i]['Reason'] = np.where ((pd.Timestamp(now).tz_localize(now_tz) - localAppt)> datetime.timedelta(0, 3600), "DataMising-OverDue", "")
            # ETA = now.tz_convert(localtz)  # initialization
        if daily_load_df.iloc[i]['StopSequence'] > 1:
            daily_load_df['dist'].iloc[i] = daily_load_df['NextStopDistance'].iloc[i - 1]
            speed = speed_local if daily_load_df['dist'].iloc[i] < 100 else speed_highway
            pretz = daily_load_df['tz'].iloc[i - 1]
            preAppt = pd.Timestamp(daily_load_df.iloc[i - 1]['Appt'], tz=pretz).tz_convert(localtz)
            preDept = pd.Timestamp(daily_load_df.iloc[i - 1]['Departure'], tz=pretz).tz_convert(localtz)
            preArrv = pd.Timestamp(daily_load_df.iloc[i - 1]['Arrival'], tz=pretz).tz_convert(localtz)
            travtime = daily_load_df['dist'].iloc[i] / speed * 3600  # unit in seconds

            travel_hour = (localArri - preDept).days * 24 + (localArri - preDept).seconds / 3600
            if daily_load_df['dist'].iloc[i] > speed * travel_hour and localArri.strftime(
                    "%Y") != '1753' and preDept.strftime("%Y") != '1753':
                filteroutid.append(daily_load_df['LoadID'].iloc[i])
                continue

            if preArrv.strftime("%Y") == '1753':
                daily_load_df['preStop_OnTime'].iloc[i] = daily_load_df['f_rate'].iloc[i - 1]
                daily_load_df['preStop_Duration'].iloc[i] = daily_load_df['hist_Duration'].iloc[i - 1]

                ##when data is missing, just using historical data. not mandatoryily made the ontime rate = 1 for prestop
                # if ( now - localAppt > datetime.timedelta(0, 3600)) :
                #     daily_load_df.loc[i-1]['ontime'] = 0
                #     daily_load_df.loc[i ]['preStop_OnTime'] = 0
                #     daily_load_df.loc[i - 1]['Reason'] = "DataMising-OverDue"

            elif preDept.strftime("%Y") == '1753':
                daily_load_df['preStop_OnTime'].iloc[i] = daily_load_df['ontime'].iloc[i - 1]
                daily_load_df['preStop_Duration'].iloc[i] = daily_load_df['hist_Duration'].iloc[i - 1]
            else:
                daily_load_df['preStop_OnTime'].iloc[i] = daily_load_df['ontime'].iloc[i - 1]
                duration = daily_load_df['duration'].iloc[i - 1]
                daily_load_df['preStop_Duration'].iloc[i] = duration if duration > 0 and duration < 360 else \
                daily_load_df['hist_Duration'].iloc[i - 1]

            ETA, daily_load_df['Reason'].iloc[i] = HOS_checking(daily_load_df.iloc[i - 1], daily_load_df.iloc[i], travtime)

        else:
            daily_load_df['dist'].iloc[i] = daily_load_df['Empty_Dist'].iloc[i]
            speed = speed_local if daily_load_df.iloc[i]['dist'] < 100 else speed_highway
            # print (daily_load_df.loc[i]['dist'],speed_local,speed_highway,speed)
            dispatch = pd.Timestamp(daily_load_df.iloc[i]['Dispatch_Time'], tz='UTC').tz_convert(localtz)
            if dispatch - localAppt >= datetime.timedelta(0, 0, 0):
                dispatch = pd.Timestamp(daily_load_df.iloc[i]['Empty_Time'], tz=localtz)
            daily_load_df['dispatch_Local'].iloc[i] = dispatch
            bookingtime = pd.Timestamp(daily_load_df['BookTimeUTC'].iloc[i], tz='UTC').tz_convert(localtz)

            daily_load_df['latebooking'].iloc[i] = 1 if localAppt - bookingtime <= datetime.timedelta(0, 3600 * 4) else 0
            daily_load_df['latedispatch'].iloc[i] = 1 if localAppt - dispatch <= datetime.timedelta(0, 3600 * 2) else 0
            daily_load_df['preStop_OnTime'].iloc[i] = 1 - daily_load_df['latedispatch'].iloc[i]
            # If already arrived, no need to check ETA. Otherwise, ETA is important
            travel_second = daily_load_df['dist'].iloc[i] / speed * 3600
            ETA = dispatch + datetime.timedelta(0, travel_second)
        if localArri.strftime("%Y") != '1753':
            ETA = localArri
        daily_load_df['ETA_ontime'].iloc[i] = 1 if ETA - localAppt <= datetime.timedelta(0, 3600) else 0
        daily_load_df['ETA'].iloc[i]= ETA
        # if ETA - localAppt <= datetime.timedelta(0, 3600):
        #     daily_load_df['ETA_ontime'].loc[i] = 1
        # else:
        #     daily_load_df['ETA_ontime'].loc[i] = 0
        #      daily_load_df.loc[i]['ETA_ontime'] = 1 if ETA- localAppt <= datetime.timedelta(0, 3600) else 0

        if daily_load_df['ETA_ontime'].iloc[i] == 0 and daily_load_df['StopSequence'].iloc[i - 1] > 1:
            if localArri.strftime("%Y") == '1753':
                daily_load_df['Reason'].iloc[i] = "Prestop-DataMissing"
    daily_load_select = daily_load_df[~daily_load_df['LoadID'].isin(filteroutid)]
    daily_load_select['latebooking'].fillna(0, inplace=True)
    daily_load_select.to_csv("testdata" + now.strftime("%Y%m%d") + ".csv", index=False)
    return (daily_load_select)

def Ontime_Score(testData,threshold):
    relaxation = 0.98
    threshold=threshold * relaxation
    A_P = 1
    D_M = 0
    # thresholds<-quantile(testData$ontime_prob,c(D_level,C_level,B_level,A_level),names=FALSE)
    thresholdscore = 50.0
    for i in range(len(testData)):
        if  testData['ontime_prob'].iloc[i] > threshold:
            lowerbound = threshold
            upperbound = A_P
            L_score = thresholdscore / 20
            H_score = 5
        else:
            lowerbound = D_M
            upperbound = threshold
            H_score  = thresholdscore / 20
            L_score = 0

        testData['SafeScore'].iloc[i]=(L_score+(testData['ontime_prob'].iloc[i]-lowerbound) * (H_score-L_score) / (upperbound-lowerbound))
    return (testData)

def Reason(coef,outputdata,inputdata,flag):
    if flag==1:
        names = ["preStop_OnTime", "preStop_Duration", "dist", "f_rate", "cust_rate"]
        reasons = ["PreStop_Delay", "PreStop_Duration", "Distance", "Facility", "Customer"]
    if flag==2:
        names = ["preStop_OnTime", "preStop_Duration", "dist", "f_rate", "cust_rate","carrier_rate","latebooking"]
        reasons = ["PreStop_Delay", "PreStop_Duration", "Distance", "Facility", "Customer","Carrier","Late_Book"]
    if flag==3:
        names = ["preStop_OnTime", "preStop_Duration", "dist", "f_rate", "cust_rate","carrier_rate"]
        reasons = ["PreStop_Delay", "PreStop_Duration", "Distance", "Facility", "Customer","Carrier"]
    refervalue =[]
    coef = coef[0][3:]  # there is dimensional problem, coef is a 2d matrix, we need to be careful with that.
    #coef=coef.fillna(0, inplace=True)
    for i in range (0,len(names)):
        refervalue.append(pd.DataFrame.mean(inputdata[names[i]]))
    for i in outputdata.index:
        delta = (outputdata[names].loc[i]-refervalue).tolist() * coef
        #output['Reason'].loc[i] = np.where(output['Reason'].loc[i] == "" or output['Reason'].loc[i].isnull(),reasons[np.argmin(delta)],output['Reason'].loc[i])
        # we do not use np.where as it returns an array. we could use tolist() to change the one element array into a number, but it may be resource consuming
        outputdata['Reason'].loc[i] =reasons[np.argmin(delta)]  if outputdata['Reason'].loc[i] == ""  else outputdata['Reason'].loc[i]
    return (outputdata)

def GML_stage(inputdata,outputdata,flag):
    # for stage 1, no carrier is booked
    # instead of using C(Code) but dummies, will make sure there is no dimensional change, i.e. maybe some dataset do not have all the levels of categories, if we use C(), it will be one dimension less than the trainning set
    if flag==1:
        X_test = dmatrix('Appt_dummy + Notice_dummy+ preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate', outputdata,
                         return_type='dataframe')
        y, X = dmatrices('ontime ~ Appt_dummy + Notice_dummy + preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate', inputdata,
                         return_type='dataframe')
    elif flag==2:
        y, X = dmatrices(
            'ontime ~ Appt_dummy + Notice_dummy + preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate + carrier_rate + latebooking',
            inputdata, return_type='dataframe')
        X_test = dmatrix(
            'Appt_dummy + Notice_dummy+ preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate + carrier_rate + latebooking',
            outputdata, return_type='dataframe')
    else:
        y, X = dmatrices(
            'ontime ~ Appt_dummy + Notice_dummy+ preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate + carrier_rate', inputdata,
            return_type='dataframe')
        X_test = dmatrix('Appt_dummy + Notice_dummy + preStop_OnTime + preStop_Duration + dist + f_rate + cust_rate + carrier_rate',
                         outputdata, return_type='dataframe')
    model = LogisticRegression(fit_intercept=False)
    mdl = model.fit(X, np.ravel(y))
    rate_result = model.predict(X_test)
    rate = model.predict_proba(X_test)[:,1]
    # dummy_ranks = pd.get_dummies(inputdata['Code'], prefix='Code')
    # cols_to_keep = ['preStop_OnTime','preStop_Duration','dist','f_rate','cust_rate']
    # data = inputdata[cols_to_keep].join(dummy_ranks.ix[:, 'Code_1':])
    # data['intercept'] = 1.0
    return rate,rate_result,model.coef_


def Get_Results(traindata,testdata,flag):
    for Type in set(traindata['PDType'].tolist()):
        if len(testdata[testdata['PDType']==Type].axes[0])> 0:
            rate, delay_result,coef = GML_stage(traindata[traindata['PDType'] == Type ], testdata[testdata['PDType'] == Type],flag)
            testdata['ontime_prob'][testdata['PDType'] == Type] = rate
            testdata['ontime_pred'][testdata['PDType'] == Type] = delay_result
            threshold = pd.DataFrame.mean(traindata[traindata.PDType == Type]['ontime'])
            testdata[testdata['PDType'] == Type] = Ontime_Score (testdata[testdata['PDType'] == Type], threshold)
            testdata['ontime_pred'][testdata['PDType'] == Type] = (rate > threshold * 0.98)
            testdata["Reason"][(testdata['ETA_ontime'] == 0) &  (testdata['arrived'] == 0 )& (testdata['Reason'].isin(["","NAN"])) ] = "ETA_Delay"
            testdata["ontime_pred"][testdata['ETA_ontime'] == 0] = -2
            if len(testdata[(testdata['PDType']==Type) & (testdata['SafeScore']<2.5) & (testdata['arrived'] == 0)].axes[0])>0:
                testdata[(testdata['PDType']==Type)  &(testdata['ontime_prob']<0.95)& (testdata['arrived'] == 0)] = Reason(coef,testdata[(testdata['PDType']==Type)  &(testdata['ontime_prob']<0.95)& (testdata['arrived']== 0)],traindata[traindata['PDType']==Type],flag)
    return (testdata)


def Generate_Results(daily_results):
    progress =["Available", "Covered", "Dispatched", "Loading Start", "Picked Up", "Unloading Start", "Delivered",
                  "Invoiced"]
    # loadtype<-c
    result_file="DelayPredictor" + now.strftime("%Y%m%d%H%M")+".csv"
    stoptype =["PickUp", "Delivery"]

    columnnames  =['LoadID','ProgressType','Parent_Customer','LoadType','NextStop_Type','NextStop_City/State','ETA','RiskScore','LeadingReason']
    result_df = pd.DataFrame( columns=columnnames)
    loadids=set (daily_results['LoadID'].tolist())
    for lid in loadids:
        loaddata=daily_results[daily_results['LoadID']==lid]
        if len(loaddata[loaddata['arrived']== 0])>0:
            nextstop=loaddata[loaddata['arrived']== 0].iloc[0]   #needs to verify whether it is loc or iloc
            stop_result={'LoadID':lid,
                            'ProgressType': progress[int(loaddata.iloc[0]['ProgressType'])],
                            'Parent_Customer':loaddata.iloc[0]['Customer'],
                            'LoadType':loaddata.iloc[0]['Load_M_B'],
                            'NextStop_Type': stoptype[int(nextstop['PDType'])],
                            'NextStop_City/State':nextstop['StateCode'],
                            'ETA':str(nextstop['ETA']),   # there is a problem in ETA data type
                            'RiskScore':int(nextstop['RiskScore']),
                            'LeadingReason':nextstop['Reason']
                                        }
            result_df=result_df.append(stop_result, ignore_index=True)
        else:
            result_df =result_df.append({'LoadID':lid,
                            'ProgressType': progress[int(loaddata.iloc[0]['ProgressType'])],
                            'Parent_Customer':loaddata.iloc[0]['Customer'],
                            'LoadType':loaddata.iloc[0]['Load_M_B'],
                            'NextStop_Type':'-',
                            'NextStop_City/State':'-',
                            'ETA':'-',
                            'RiskScore':'-',
                            'LeadingReason':'-'}, ignore_index=True)
    result_df.to_csv(result_file,index=False)
    return(0)


def Roll_Prediction_Model(trainData,testData):
    #Stage 1
    #testData_1 = testData[testData['ProgressType'] == 1]
    # if  len(testData_1.axes[0])>0:
    #     testData[(testData['ProgressType'] == 1) & (testData['StopSequence'] == 1)]=Get_Results(trainData[trainData['StopSequence'] == 1],testData[(testData['ProgressType'] == 1) & (testData['StopSequence'] == 1)],1)
    #     for k in range(2,max(testData_1.NumStops)):
    #         for i in range(len(testData_1)):
    #             if testData[testData['ProgressType']==1]['StopSequence'].iloc[i]== k:
    #                 if testData[testData['ProgressType']==1]['arrived'].iloc[i-1] == 0:
    #                     testData[testData['ProgressType'] == 1]['preStop_OnTime'].iloc[i] =testData[testData['ProgressType'] == 1]['ontime_prob'].iloc[i-1]
    #         testData[(testData['ProgressType'] == 1) & (testData['StopSequence'] == k)]=Get_Results(trainData[trainData['StopSequence'] > 1],
    #                                                                 testData[(testData['ProgressType'] == 1) & (testData['StopSequence'] == k)], 1)
    testData_1 = testData[testData['ProgressType'] == 1]
    if len(testData_1.axes[0])>0:
        testData_1[testData_1['StopSequence'] == 1]=Get_Results(trainData[trainData['StopSequence'] == 1],testData_1[testData_1['StopSequence'] == 1],1)
        max_stop = max(testData_1.NumStops) + 1
        nrow_testData_1 = len(testData_1)
        for k in range(2, max_stop):
            for i in range(nrow_testData_1):
                if testData_1['StopSequence'].iloc[i] == k:
                    if testData_1['arrived'].iloc[i-1] == 0:
                         testData_1['preStop_OnTime'].iloc[i]= testData_1['ontime_prob'].iloc[i-1]
            testData_1[testData_1['StopSequence']== k]=Get_Results(trainData[trainData ['StopSequence'] > 1], testData_1[testData_1['StopSequence'] == k],1)

    #Stage 2
    testData_2 = testData[testData['ProgressType']>1]
    testData_2[testData_2['StopSequence'] == 1]=Get_Results(trainData[trainData['StopSequence'] == 1], testData_2[testData_2['StopSequence'] == 1],2)
    #testData[(testData['ProgressType'] > 1) & (testData['StopSequence'] == 1)]=Get_Results(trainData[trainData['StopSequence'] == 1],testData[(testData['ProgressType'] > 1) & (testData['StopSequence'] == 1)], 1)
    max_stop=max(testData_2.NumStops)+1
    nrow_testData_2=len(testData_2)
    for k in range(2, max_stop):
        #for i in testData_2.index:
        for i in range(nrow_testData_2):
            if testData_2['StopSequence'].iloc[i] == k:
                if testData_2['arrived'].iloc[i-1] == 0:
                    testData_2['preStop_OnTime'].iloc[i] = testData_2['ontime_prob'].iloc[i - 1]
        testData_2[testData_2['StopSequence'] == k] = Get_Results(trainData[trainData['StopSequence'] > 1],testData_2[testData_2['StopSequence'] == k], 3)
    # for k in range(2, max_stop):
    #     #for i in testData_2.index:
    #     for  i in range(nrow_testData_2): 
    #         if testData_2['StopSequence'].iloc[i] == k:
    #             if testData_2['arrived'].iloc[i - 1] == 0:
    #                 testData_2['preStop_OnTime'].iloc[i] = testData_2['ontime_prob'].iloc[i - 1]
    #     testData_2[testData_2['StopSequence']==k]=Get_Results(trainData[trainData['StopSequence'] > 1], testData_2[testData_2['StopSequence'] == k],3)
        #testData_2_pred.append(Get_Results(trainData[trainData['StopSequence'] > 1], testData_2[testData_2['StopSequence'] == k],3))
    testData_df=pd.concat([testData_1,testData_2])
    testData_df['RiskScore'] = 100-testData_df['SafeScore']*20


    #numstop = max(testData_df['NumStops'])

    #results = testData_df[["LoadID", "LoadDate", "Customer", "Load_M_B", "ProgressType", "StopSequence",
     #                        "SafeScore", "Reason", "Arrival"]].sort(['LoadDate', 'LoadID'], ascending=[1, 0])
    testData_df['CheckScore'][(testData_df['ETA_ontime']==0)&(testData_df['StopSequence']==1) & (testData_df['ETA'] -  testData_df['Appt'] <= datetime.timedelta(0,90*60)) ]=80
    testData_df['CheckScore'][(testData_df['ETA_ontime']==0) & (testData_df['StopSequence']==1) & (testData_df['ETA'] -  testData_df['Appt'] > datetime.timedelta(0,90*60)) ]=20
    testData_df['CheckScore'][(testData_df['ETA_ontime']==0) & (testData_df['StopSequence']>1) ]=20
    testData_df['CheckScore'][( testData_df['arrived']>0)  & (testData_df['Arrival'] -  testData_df['Appt'] > datetime.timedelta(0,60*60)) ]=100
    testData_df['CheckScore'][( testData_df['arrived']>0)  & (testData_df['Arrival'] -  testData_df['Appt']  <= datetime.timedelta(0,60*60)) ]=0
 
    results=testData_df[["LoadID", "LoadDate", "Customer","ProgressType", "StopSequence", "ontime_prob", "ontime_pred",
          "SafeScore", "Reason"]]

    results['RiskScore_2']=np.ceil(results['SafeScore'])
    results.to_csv("OnTime_Predict" + now.strftime("%Y%m%d%H%M") + ".csv",index=False)
    testData_df.to_csv("testdata" + now.strftime("%Y%m%d%H%M") + ".csv",index=False)

    Generate_Results(testData_df)

    return (testData_df)


def get_dummies(data):
    dummies=pd.get_dummies(data['Code'])
    data['Appt_dummy']=dummies['Appt'].tolist()
    data['Notice_dummy']=dummies['Notice'].tolist()
    data['Open_dummy']=dummies['Open'].tolist()
    return data

#def Delay_predict( ):
if __name__ == "__main__":
    filename="histdata"+ now.strftime("%Y%m%d") +".csv"
    if ((os.path.isfile(filename))==False):
        trainData=Get_Histload_ALL()
        testData=Get_Dynamic_load()
        testloadid=testData['LoadID']
        #testloadid.to_csv("dailyid.csv",index=False)
    else:
        trainData = pd.read_csv(filename)
        testData=Get_Dynamic_load()
    testData=get_dummies(testData)
    trainData=get_dummies(trainData)
    testData['ontime_prob']=-1
    testData['ontime_pred']=-1
    testData['SafeScore']=-1
    testData['RiskScore']=-1
    testData['CheckScore']=-1
    
    testData = Roll_Prediction_Model(trainData,testData)

    
    #testData_df=testData.sort_values(by=['LoadID', 'StopSequence'])
    #testData_df.to_csv("dailydata_temp.csv",index = False)
    #Evaluation(testData)
    #return(0)
