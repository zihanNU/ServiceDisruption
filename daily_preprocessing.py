import pyodbc
import pandas as pd
import numpy as np
import math
import datetime
import os
from patsy import dmatrices,dmatrix
from sklearn.linear_model import LogisticRegression
import statsmodels.discrete.discrete_model as sm


timezones = {'Timezone': [-5, -6, -7, -12, -8, -9, -10],
             'tz': ['America/New_York', 'America/Chicago', 'America/Denver', 'America/Phoenix',
                    'America/Los_Angeles', 'America/Anchorage', 'Pacific/Honolulu']}
timezones_df = pd.DataFrame(data=timezones)
now = datetime.datetime.now()
now_tz=datetime.datetime.now().astimezone().tzinfo
now = pd.Timestamp(now).tz_localize(now_tz)



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

    if  pd.Timestamp(prestop['Arrival']).strftime("%Y")  != '1753':
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

    if pd.Timestamp(stop['Arrival']).strftime("%Y") == '1753':
        ETA = (datetime.timedelta(0, (HOS * 10 + travtime + mealtime) * 3600) + prestop_end).tz_convert(stop_tz)
        if ETA - pd.Timestamp(stop['Appt'],tz=stop_tz) > datetime.timedelta(0.3600):
            if (HOS > 0):
                Reason = "Scheduling/HOS--ETA_Delay"
            else:
                Reason = "ETA_Delay"
    else:
        ETA = pd.Timestamp(stop['Arrival'],tz=stop_tz)
    return (ETA, Reason)


if __name__ == "__main__":
#def Get_Dynamic_load():
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

    SELECT distinct top 100
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
    daily_load = pd.merge(left=daily_load, right=hist_carrierrate[hist_carrierrate.cvolume > 10],
                          left_on=['CarrierID', 'PDType'], right_on=['CarrierID', 'PDType'], how='left')
    daily_load = pd.merge(left=daily_load, right=hist_custrrate[hist_custrrate.cvolume > 10],
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


    #columnnames=daily_load_select.columns.tolist()
    #daily_load_df=pd.DataFrame(columns=columnnames )

    daily_load_df=daily_load_select
    daily_load_df['latebooking']=0
    daily_load_df['latedispatch']=0
    daily_load_df['preStop_OnTime']=-1
    daily_load_df['preStop_Duration'] = -1
    daily_load_df['dist']=  daily_load_df['Empty_Dist']   #initialize as empty dist, and will be updated with nextstopdistance when stopsequence>1
    daily_load_df['dispatch_Local'] =now
    #daily_load_df['DoW'] = daily_load_df['Appt'].strftime("%w")
    daily_load_df ['ETA_ontime'] =  -1
    daily_load_df['ETA']=now
    daily_load_df['Reason']=""
    

    daily_load_df.Customer= daily_load_df.Customer.replace(",", "")
    filteroutid=[]
    speed_highway = 55
    speed_local = 35
    detention = 2
    workin_hour = 4

    for i in daily_load_df.index:
 
        # if(abs(daily_load_df.loc[i]['Appt']-daily_load_df.loc[i]['Arrival']).days>=1 ) :
        #     filteroutid.append(daily_load_df.loc[i]['LoadID'])
        #     continue
        loadstop=daily_load_df.loc[i]
        localtz =  loadstop['tz']
        localAppt= pd.Timestamp(daily_load_df['Appt'].loc[i],tz=localtz)
        localArri=pd.Timestamp(daily_load_df['Arrival'].loc[i],tz=localtz)
        now_local=now.tz_convert(localtz)
        if pd.Timestamp(daily_load_df['Arrival'].loc[i]).strftime("%Y")=='1753':
            daily_load_df['ontime'].loc[i]= 0 if now_local- localAppt > datetime.timedelta(0, 3600) else -1
            daily_load_df['Reason'].loc[i] = "DataMising-OverDue" if now_local - localAppt > datetime.timedelta(0, 3600) else "" 
            # daily_load_df.loc[i]['ontime']=np.where ((pd.Timestamp(now).tz_localize(now_tz) - localAppt)
            #                                          > datetime.timedelta(0, 3600), 0, -1  )
            # daily_load_df.loc[i]['Reason'] = np.where ((pd.Timestamp(now).tz_localize(now_tz) - localAppt)> datetime.timedelta(0, 3600), "DataMising-OverDue", "")
            #ETA = now.tz_convert(localtz)  # initialization
        if daily_load_df.loc[i]['StopSequence']>1:
            daily_load_df['dist'].loc[i]=daily_load_df['NextStopDistance'].loc[i-1]
            speed= speed_local if daily_load_df['dist'].loc[i]<100 else speed_highway 
            pretz=daily_load_df['tz'].loc[i-1]
            preAppt=pd.Timestamp(daily_load_df.loc[i-1]['Appt'],tz=pretz).tz_convert(localtz)
            preDept = pd.Timestamp(daily_load_df.loc[i-1]['Departure'],tz=pretz).tz_convert(localtz)
            preArrv = pd.Timestamp(daily_load_df.loc[i-1]['Arrival'],tz=pretz).tz_convert(localtz)
            travtime = daily_load_df['dist'].loc[i] / speed * 3600 #unit in seconds

            travel_hour = (localArri -  preDept).days*24+   (localArri -  preDept).seconds/3600
            if  daily_load_df['dist'].loc[i]>speed*travel_hour and localArri.strftime("%Y")!= '1753' and preDept.strftime("%Y")!= '1753':
                filteroutid.append(daily_load_df['LoadID'].loc[i])
                continue

            if  preArrv.strftime("%Y")==  '1753'  :
                daily_load_df['preStop_OnTime'].loc[i] = daily_load_df['f_rate'].loc[i-1]
                daily_load_df['preStop_Duration'].loc[i] = daily_load_df['hist_Duration'].loc[i-1]

                ##when data is missing, just using historical data. not mandatoryily made the ontime rate = 1 for prestop
                # if ( now - localAppt > datetime.timedelta(0, 3600)) :
                #     daily_load_df.loc[i-1]['ontime'] = 0
                #     daily_load_df.loc[i ]['preStop_OnTime'] = 0
                #     daily_load_df.loc[i - 1]['Reason'] = "DataMising-OverDue"

            elif  preDept.strftime("%Y")==  '1753'  :
                daily_load_df['preStop_OnTime'].loc[i] = daily_load_df['ontime'].loc[i-1]
                daily_load_df['preStop_Duration'].loc[i] = daily_load_df['hist_Duration'].loc[i-1]
            else:
                daily_load_df['preStop_OnTime'].loc[i] = daily_load_df['ontime'].loc[i-1]
                duration = daily_load_df['duration'].loc[i-1]
                daily_load_df['preStop_Duration'].loc[i] = duration if duration>0 and duration<360 else daily_load_df['hist_Duration'].loc[i-1]

            ETA,daily_load_df['Reason'].loc[i]=HOS_checking( daily_load_df.loc[i-1],daily_load_df.loc[i],travtime )

        else:
            daily_load_df['dist'].loc[i] = daily_load_df['Empty_Dist'].loc[i]
            speed = speed_local if  daily_load_df.loc[i]['dist'] < 100 else  speed_highway
            #print (daily_load_df.loc[i]['dist'],speed_local,speed_highway,speed)
            dispatch = pd.Timestamp(daily_load_df.loc[i]['Dispatch_Time'],tz='UTC').tz_convert(localtz)
            if dispatch - localAppt >= datetime.timedelta(0,0,0) :
                dispatch =  pd.Timestamp(daily_load_df.loc[i]['Empty_Time'],tz=localtz) 
            daily_load_df['dispatch_Local'].loc[i]= dispatch
            bookingtime = pd.Timestamp(daily_load_df['BookTimeUTC'].loc[i],tz='UTC').tz_convert(localtz)

            daily_load_df['latebooking'].loc[i] = 1 if localAppt - bookingtime <= datetime.timedelta(0, 3600*4) else 0
            daily_load_df['latedispatch'].loc[i] = 1 if localAppt - dispatch <= datetime.timedelta(0, 3600*2) else 0
            daily_load_df['preStop_OnTime'].loc[i] = 1 - daily_load_df['latedispatch'].loc[i]
            # If already arrived, no need to check ETA. Otherwise, ETA is important
            travel_seond= daily_load_df['dist'].loc[i]/ speed * 3600
            ETA =   dispatch + datetime.timedelta(0,travel_seond)
        if  localArri.strftime("%Y") != '1753':
            ETA = localArri
        if ETA - localAppt <= datetime.timedelta(0, 3600):
            daily_load_df['ETA_ontime'].loc[i] = 1
        else:
            daily_load_df['ETA_ontime'].loc[i] = 0
        #      daily_load_df.loc[i]['ETA_ontime'] = 1 if ETA- localAppt <= datetime.timedelta(0, 3600) else 0

        if  daily_load_df['ETA_ontime'].loc[i] ==0 and daily_load_df['StopSequence'].loc[i-1] >1:
            if   localArri.strftime("%Y")  == '1753':
                daily_load_df['Reason'].loc[i] = "Prestop-DataMissing"
    daily_load_select=daily_load_df[~daily_load_df['LoadID'].isin(filteroutid)]
    daily_load_select['latebooking'].fillna(0, inplace=True)
    daily_load_select.to_csv("testdata" + now.strftime("%Y%m%d") +".csv", index=False)
    #return (daily_load_select)





 


