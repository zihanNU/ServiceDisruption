import pyodbc
import pandas as pd
import numpy as np
import math
import datetime
import os
from patsy import dmatrices, dmatrix
from sklearn.linear_model import LogisticRegression
import statsmodels.discrete.discrete_model as sm

# class sklearn.linear_model.LogisticRegression

now = datetime.datetime.now()
now_tz = datetime.datetime.now().astimezone().tzinfo
now = pd.Timestamp(now).tz_localize(now_tz)

# ===========================================================
# Data Preprocessing
# ===========================================================
bounce_reason = {'FaultType': [0,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,3,3,3,3],
             'ReasonType': [0,1,2,3,4,6,12,13,7,8,1,2,3,4,5,7,8,9,10,13,10,11,12,13],
             'Reason':['Carrier','Carrier','Carrier','Carrier','Carrier','Carrier','Carrier','Carrier','Carrier_Reps','Cust_Reps','Carrier','Carrier',
                       'Carrier','Carrier','Customer','Carrier_Reps','Cust_Reps','Carrier','Facility','Carrier_Reps','Facility','Facility','Customer','Customer']    }
bounce_reason_df = pd.DataFrame(data=bounce_reason)



def Appt_time(APPT_PU):
    appt_hour = pd.Timestamp(APPT_PU).hour
    if appt_hour in range(7,10):
        return 'EarlyMorning'
    elif appt_hour in range(21,24):
        return 'Night'
    elif appt_hour in range(10,20):
        return 'Daytime'
    else:
        return 'Dawn'

def Weekday(time):
    time_ts=pd.Timestamp(time)
    return time_ts.weekday_name
    
def Get_Histload_ALL( ):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    sql_hist = """
         If(OBJECT_ID('tempdb..#Temptable_Hist') Is Not Null)
                Drop Table #Temptable_Hist
        
        
        SELECT H.*                    
        ,case when H.Prebook_Type<3 then  CU_R.Bounce_Rate_Customer_Ave  
        when H.Prebook_Type=3  then  0.5*CU_R.Bounce_Rate_Customer_Ave + 0.5*COALESCE(CU_R.Bounce_Rate_Customer,CU_R.Bounce_Rate_Customer_Ave) 
        else COALESCE(CU_R.Bounce_Rate_Customer,CU_R.Bounce_Rate_Customer_Ave)  end 'cust_rate' 
        ,case when H.Prebook_Type<3 then C_R.Bounce_Rate_Carrier_Ave
        when H.Prebook_Type =3  then COALESCE( C_R.Bounce_Rate_Carrier, C_R.Bounce_Rate_Carrier_Ave )*0.5+ C_R.Bounce_Rate_Carrier_Ave *0.5 
        else COALESCE( C_R.Bounce_Rate_Carrier, C_R.Bounce_Rate_Carrier_Ave )
        end 'c_rate',
        Cor.Bounce_Rate_Corridor  'cor_rate',   
        case when H.Prebook_Type<3 then F_C.Bounce_Rate_Facility_Ave  else  COALESCE( F_C.Bounce_Rate_Facility, F_C.Bounce_Rate_Facility_Ave ) end  'fac_rate', 
        case when H.Prebook_Type<3 then Bounce_Rate_CarReps_Ave else COALESCE(Bounce_Rate_CarReps, Bounce_Rate_CarReps_Ave) end 'crep_rate' 
        into #Temptable_Hist			 
        FROM [ResearchScience].[dbo].[Bounce_Histload] H   
        left join [ResearchScience].[dbo].[Bounce_Customer_Rate] CU_R	on CU_R.CustomerID=H.CustomerID and CU_R.Prebook_Type=H.Prebook_Type
        left join [ResearchScience].[dbo].[Bounce_Carrier_Rate] C_R on C_R.CarrierID=H.CarrierID and  C_R.Prebook_Type=H.Prebook_Type
        left join [ResearchScience].[dbo].[Bounce_Facility_Rate] F_C on F_C.facilityid=H.FacilityID and F_C.Prebook_Type=H.Prebook_Type
        left join [ResearchScience].[dbo].[Bounce_Corridor_Rate] Cor on Cor.Corridor=H.Corridor and Cor.Prebook_Type=H.Prebook_Type
        left join [ResearchScience].[dbo].[Bounce_CarrierReps_Rate] Car_Rep on Car_Rep.CarrierReps=H.CarrierReps and Car_Rep.Prebook_Type=H.Prebook_Type
         
         ALTER Table #Temptable_Hist
         Drop column CreateDate, UpdateDate		
        
          select * from #Temptable_Hist X
                         where X.c_rate is not NULL 
                         and X.cor_rate is not NULL
                         and X.crep_rate is not NULL
                         and X.cust_rate is not NULL
                         and X.fac_rate is not NULL
        """
    hist_load = pd.read_sql(sql=sql_hist, con=cn)

    return (hist_load)

if __name__ == "__main__":
#def Get_dailyload( ):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    sql_cor = """select Corridor,Prebook_Type,Bounce_Rate_Corridor 'cor_rate' from [ResearchScience].[dbo].[Bounce_Corridor_Rate]"""
    hist_load_cor = pd.read_sql(sql=sql_cor,con=cn)

    sql_fac = """select FacilityID,Prebook_Type,Bounce_Rate_Facility, Bounce_Rate_Facility_Ave from [ResearchScience].[dbo].[Bounce_Facility_Rate]"""
    hist_load_fac = pd.read_sql(sql=sql_fac,con=cn)

    sql_cust= """select CustomerID,Prebook_Type,Bounce_Rate_Customer, Bounce_Rate_Customer_Ave from [ResearchScience].[dbo].[Bounce_Customer_Rate]"""
    hist_custrrate = pd.read_sql(sql=sql_cust,con=cn)

    sql_carrier = """select CarrierID,Prebook_Type, Bounce_Rate_Carrier, Bounce_Rate_Carrier_Ave from [ResearchScience].[dbo].[Bounce_Carrier_Rate]"""
    hist_carrierrate = pd.read_sql(sql=sql_carrier,con=cn)

    sql_crep ="""select CarrierReps,Prebook_Type,Bounce_Rate_CarReps, Bounce_Rate_CarReps_Ave from [ResearchScience].[dbo].[Bounce_CarrierReps_Rate]"""
    hist_crep = pd.read_sql(sql=sql_crep,con=cn)

    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;uid=BazookaAccess;pwd=C@y0te')

    sql_load="""   
        set nocount on
        Declare @Today as datetime =   getdate() 
        Declare @Date0 as datetime =   dateadd(day, 3, @today)
                               
        If(OBJECT_ID('tempdb..#Load_Bounce_ID') Is Not Null)
        Begin
        Drop Table #Load_Bounce_ID
        End
        Create Table #Load_Bounce_ID (LoadID int, LoadDate date , ProgressType int, EquipmentType varchar(20), Loadtype varchar(10), Corridor  varchar(100), FacilityID INT, Code_PU varchar(10), APPT_PU datetime, Code_DO varchar(10), APPT_DO datetime)
        Insert into #Load_Bounce_ID
                               
        SELECT   L.[ID], L.LoadDate
        ,L.ProgressType
        ,L.EquipmentType
        ,case when L.[RoutingRankType] = 203 then 'S' else 'C' end 'Loadtype'
        ,RCO.ClusterNAME + '-'+ RCD.ClusterName 'Corridor'
        ,lsp.FacilityID
        ,case when LSP.[ScheduleType] = 1 then 'Appt' when LSP.[ScheduleType] = 2 then 'Notice' when LSP.[ScheduleType] = 3 then 'Open' 
        else case when LSP.[ScheduleOpenTime] = LSP.[ScheduleCloseTime] then 'Appt'else 'Open' end
        end 'Code_PU' 
        , case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
        convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
        else LSP.[ScheduleCloseTime] end 'Appt_PU'
        ,case when LSD.[ScheduleType] = 1 then 'Appt' when LSD.[ScheduleType] = 2 then 'Notice' when LSD.[ScheduleType] = 3 then 'Open' 
        else case when LSD.[ScheduleOpenTime] = LSD.[ScheduleCloseTime] then 'Appt'else 'Open' end
        end 'Code_DO' 
        , case when LSD.[ScheduleCloseTime] = '1753-01-01' then 
        convert(datetime, CONVERT(date, LSD.LoadByDate)) + convert(datetime, CONVERT(time, LSD.CloseTime)) 
        else LSD.[ScheduleCloseTime] end 'Appt_DO' 	  
        FROM Bazooka.dbo.[Load] L                  
        inner join Bazooka.dbo.loadstop LSP on  LSP.ID=L.OriginLoadStopID
        inner join Bazooka.dbo.loadstop LSD on  LSD.ID=L.DestinationLoadStopID
        LEFT JOIN analytics.Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode 
        LEFT JOIN analytics.Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode 
        where
        ProgressType <4 and Mode = 1 and StateType = 1 and  LoadDate between @Today and @Date0
        AND  L.ShipmentType not in (3,4,6,7)
        AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
        AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
        and L.Miles>0 
                               
        
		If(OBJECT_ID('tempdb..#Load_Bounce_Z') Is Not Null)
		Begin
		Drop Table #Load_Bounce_Z
		End
		Create Table #Load_Bounce_Z (LoadID int, LoadDate datetime, ProgressType int, EquipmentType varchar(20),  Loadtype varchar(10), Corridor  varchar(100), FacilityID INT, Code_PU varchar(10), APPT_PU datetime, Code_DO varchar(10), APPT_DO datetime, LCEntryID int,  PowerType varchar(20), CarrierType varchar(10), CarrierID int, CustomerID int,  Prebook_Type int, BookTime datetime,  CarrierReps int)--, CustReps int  )
		Insert into #Load_Bounce_Z  		                                           
                              
                               
        SELECT  LB_ID.*
        ,LC.ID 'LCEntryID'
        ,NULL 'PowerType'
        ,NULL 'CarrierType'
        ,NULL
        ,LCUS.CustomerID
        ,NULL  'Prebook_Type'
        ,NULL 'BookTime'
        ,NULL 'CarrierReps'
        --,CusRP.EmployeeID 'CustReps'   
        --,C.ContractVersion            
        FROM #Load_Bounce_ID LB_ID
        left join [Bazooka].[dbo].[LoadCarrier] LC on LB_ID.LoadID=LC.LoadID and IsBounced=0 and main=1
        --left JOIN Bazooka.dbo.Carrier C ON C.ID = LC.CarrierID
        --left JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = LB_ID.LoadID AND LR.EntityType = 13 AND LR.EntityID = LC.ID
        INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = LB_ID.LoadID AND LCUS.Main = 1 
        INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
        LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
        --left join bazooka.dbo.LoadRep CarRP on CarRP.EntityID= LC.ID and CarRP.Main=1 and CarRP.EntityType=13
        --inner join bazooka.dbo.LoadRep CusRP on CusRP.EntityID= LCUS.ID and CusRP.Main=1 and CusRP.EntityType=12
        where
        ProgressType=1 and LC.ID is null 
        and CUS.[Name] not like 'UPS%'   
                               
        union
                               
        SELECT  LB_ID.*
        ,LC.ID 'LCEntryID'
        , case   when C.EquipmentPowerUnits >500 then 'National'
                     when C.EquipmentPowerUnits  between 51 and 500 then 'Medium'
                     when C.EquipmentPowerUnits  between 4 and 50 then 'Small'
                     when C.EquipmentPowerUnits  <=3 then 'Small'
                     end 'PowerType' 
        ,'S' 'CarrierType'
		--,CASE WHEN LR.OriginalQuoteRateLineItemID = 0 THEN 'S' ELSE 'C' END 'CarrierType'
        ,LC.[CarrierID]
        ,LCUS.CustomerID
        ,case 
        when (DATEDIFF(hour , [BookByDateTime], Appt_PU ) <=4) then 6
        when (DATEDIFF(hour , [BookByDateTime], Appt_PU ) <=24) then 5
        when (DATEDIFF(day , [BookByDateTime], Appt_PU ) =1) then 4
        when (DATEDIFF(day , [BookByDateTime], Appt_PU ) <=3) then 3
        when (DATEDIFF(day , [BookByDateTime], Appt_PU ) <=7) then 2
        when (DATEDIFF(day , [BookByDateTime], Appt_PU ) <=14) then 1
        else 0 end  'Prebook_Type'
        ,LC.[BookByDateTime]
        ,CarRP.EmployeeID  'CarrierReps'
        --,CusRP.EmployeeID 'CustReps'   
        --,C.ContractVersion            
        FROM #Load_Bounce_ID LB_ID
        INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = LB_ID.LoadID AND LCUS.Main = 1 
        INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
		left join [Bazooka].[dbo].[LoadCarrier] LC on LB_ID.LoadID=LC.LoadID and IsBounced=0 and LC.main=1
        left JOIN Bazooka.dbo.Carrier C ON C.ID = LC.CarrierID
        left JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = LB_ID.LoadID AND LR.EntityType = 13 AND LR.EntityID = LC.ID
        --LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
        left join bazooka.dbo.LoadRep CarRP on CarRP.EntityID= LC.ID and CarRP.Main=1 and CarRP.EntityType=13
        --inner join bazooka.dbo.LoadRep CusRP on CusRP.EntityID= LCUS.ID and CusRP.Main=1 and CusRP.EntityType=12
        where ProgressType>1  and
        C.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
        and (NOT C.ContractVersion like 'UPS%' )
        AND LC.CarrierID NOT IN (32936 ,244862,244863,244864,244866,244867,100225)  
        and LR.OriginalQuoteRateLineItemID = 0 
        and CUS.[Name] not like 'UPS%'  



		select LoadID,LoadDate, ProgressType , Loadtype,
		case when datepart(hour,APPT_PU) between 10 and 20 then 'Daytime' when datepart(hour,APPT_PU) between 7 and 9 then 'EarlyMorning' 
		when datepart(hour,APPT_PU) between 21 and 24 then 'Night' else 'Dawn' end 'Appt_PU_Time',
		case when datepart(hour,APPT_DO) between 10 and 20 then 'Daytime' when datepart(hour,APPT_DO) between 7 and 9 then 'EarlyMorning' 
		when datepart(hour,APPT_DO) between 21 and 24 then 'Night' else 'Dawn' end 'Appt_DO_Time',
		datename(dw, #Load_Bounce_Z.APPT_PU) 'Appt_PU_DOW',
		datename(dw, #Load_Bounce_Z.APPT_DO) 'Appt_DO_DOW',
		CarrierID, CustomerID,FacilityID, Code_PU, APPT_PU, Code_DO, APPT_DO, Prebook_Type,  Corridor,  BookTime,  CarrierReps,  
		COALESCE(Cost1,Cost2,0) 'Cost' 
		from #Load_Bounce_Z
		left join (SELECT
		EntityID,
		SUM(LRD.Amount) 'Cost1'
		FROM  Bazooka.dbo.LoadRateDetail LRD 
		WHERE LRD.EDIDataElementCode IN  ('405','FR',  'PM' ,'MN' )
		and LRD.EntityID in (Select LCEntryID from  #Load_Bounce_Z) 
		GROUP BY LRD.EntityID ) LC1 on #Load_Bounce_Z.LCEntryID = LC1.EntityID
		left join
		(select
		LoadCarrierId,
		SUM(LCMRD.Amount) 'Cost2'
		FROM Bazooka.dbo.LoadCarrierManagedRateDetail LCMRD 
		INNER JOIN Bazooka.dbo.RateCostCode RC ON RC.RateCostCodeId = LCMRD.RateCostCodeId
		WHERE RC.EDIDataElementCode IN  ('405','FR',  'PM' ,'MN'  )
		and LoadCarrierId in (Select LCEntryID from  #Load_Bounce_Z)
		GROUP BY  LCMRD.LoadCarrierId) LC2  ON  LC2.LoadCarrierId = #Load_Bounce_Z.LCEntryID  
        """
    daily_load = pd.read_sql(sql=sql_load, con=cn)

    daily_load = pd.merge(left=daily_load, right=hist_load_cor,
                          left_on=['Corridor', 'Prebook_Type'],
                          right_on=['Corridor', 'Prebook_Type'], how='left')

    daily_load = pd.merge(left=daily_load, right=hist_load_fac,
                          left_on=['FacilityID',  'Prebook_Type'],right_on=['FacilityID', 'Prebook_Type'], how='left')
    daily_load['fac_rate'][daily_load['Prebook_Type']>=3] = daily_load[daily_load['Prebook_Type']>=3].Bounce_Rate_Facility.combine_first(daily_load[daily_load['Prebook_Type']>=3].Bounce_Rate_Facility_Ave)
    daily_load['fac_rate'][daily_load['Prebook_Type']<3] = daily_load['Bounce_Rate_Facility_Ave']

    daily_load = pd.merge(left=daily_load, right=hist_carrierrate,
                          left_on=['CarrierID',  'Prebook_Type'], right_on=['CarrierID', 'Prebook_Type'], how='left')
    daily_load['c_rate'][daily_load['Prebook_Type']> 3]= daily_load[daily_load['Prebook_Type']> 3].Bounce_Rate_Carrier_Ave.combine_first(daily_load[daily_load['Prebook_Type']> 3].Bounce_Rate_Carrier_Ave)
    daily_load['c_rate'][daily_load['Prebook_Type']==3] = daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Carrier_Ave.combine_first(daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Carrier_Ave *0.5+ daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Carrier*0.5)
    daily_load['c_rate'][daily_load['Prebook_Type']< 3] =  daily_load['Bounce_Rate_Carrier_Ave'][daily_load['Prebook_Type']< 3]


    daily_load = pd.merge(left=daily_load, right=hist_custrrate,
                          left_on=['CustomerID',  'Prebook_Type'], right_on=['CustomerID', 'Prebook_Type'], how='left')
    daily_load['cust_rate']=daily_load.Bounce_Rate_Customer.combine_first(daily_load.Bounce_Rate_Customer_Ave)
    daily_load['cust_rate'][daily_load['Prebook_Type']==3] = 0.5 * daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Customer_Ave + 0.5 * daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Customer.combine_first(daily_load[daily_load['Prebook_Type']==3].Bounce_Rate_Customer_Ave)
    daily_load['cust_rate'][daily_load['Prebook_Type']< 3]= daily_load['Bounce_Rate_Customer_Ave'][daily_load['Prebook_Type']< 3]

    daily_load = pd.merge(left=daily_load, right=hist_crep,
                          left_on=['CarrierReps',  'Prebook_Type'], right_on=['CarrierReps', 'Prebook_Type'], how='left')

    daily_load['crep_rate']=daily_load.Bounce_Rate_CarReps.combine_first(daily_load.Bounce_Rate_CarReps_Ave)
    daily_load['crep_rate'][daily_load['Prebook_Type']< 3] = daily_load['Bounce_Rate_CarReps_Ave'][daily_load['Prebook_Type']< 3]

    daily_load['c_rate'].fillna(daily_load['c_rate'].mean(axis=0), inplace=True)
    daily_load['cor_rate'].fillna(daily_load['cor_rate'].mean(axis=0), inplace=True)
    daily_load['crep_rate'].fillna(daily_load['crep_rate'].mean(axis=0), inplace=True)
    daily_load['cust_rate'].fillna(daily_load['cust_rate'].mean(axis=0), inplace=True)
    daily_load['fac_rate'].fillna(daily_load['fac_rate'].mean(axis=0), inplace=True)
     
    daily_load.to_csv("testdata" + now.strftime("%Y%m%d") + '.csv', index=False)
    daily_load.to_csv("testdata.csv", index=False)
    #return (daily_load)

def Ontime_Score(testData, threshold):
    A_P = 1
    D_M = 0
    # thresholds<-quantile(testData$ontime_prob,c(D_level,C_level,B_level,A_level),names=FALSE)
    thresholdscore = 50.0
    for i in range(len(testData)):
        if testData['ontime_prob'].iloc[i] > threshold:
            lowerbound = threshold
            upperbound = A_P
            L_score = thresholdscore / 20
            H_score = 5
        else:
            lowerbound = D_M
            upperbound = threshold
            H_score = thresholdscore / 20
            L_score = 0

        testData['SafeScore'].iloc[i] = (
                L_score + (testData['ontime_prob'].iloc[i] - lowerbound) * (H_score - L_score) / (
                upperbound - lowerbound))
    return (testData)


def Reason(coef, outputdata, inputdata, flag):
    if flag == 0:
        reasons = ["Facility","Customer" , "Corridor", "Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" ]
        names = ["fac_rate", "cust_rate", "cor_rate", "DO_Monday", "DO_Tuesday","DO_Wednesday","DO_Thursday","DO_Friday","DO_Saturday","PU_Monday","PU_Tuesday", "PU_Wednesday","PU_Thursday","PU_Friday","PU_Saturday" ]
    elif flag == 1:
        reasons = ["Facility", "Corridor", "PowerType", "Carrier", "Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" , "Cost"]
        names = ["fac_rate", "cor_rate", "PowerType", "c_rate", "DO_Monday", "DO_Tuesday","DO_Wednesday","DO_Thursday","DO_Friday","DO_Saturday","PU_Monday","PU_Tuesday", "PU_Wednesday","PU_Thursday","PU_Friday","PU_Saturday", "Cost"]
    elif flag == 2:
        reasons = ["Facility", "Corridor", "Carrier_Reps","Carrier","PowerType", "Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" , "Cost"]
        names = ["fac_rate", "cor_rate","crep_rate", "c_rate","PowerType","DO_DOW_dummy", "PU_DOW_dummy", "Cost"]
    elif flag == 3:
        reasons = ["Facility", "Corridor","Carrier","Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" , "Cost"]
        names = ["fac_rate", "cor_rate","c_rate","DO_Monday", "DO_Tuesday","DO_Wednesday","DO_Thursday","DO_Friday","DO_Saturday","PU_Monday","PU_Tuesday", "PU_Wednesday","PU_Thursday","PU_Friday","PU_Saturday",  "Cost"]
    elif flag == 4:
        reasons = ["Facility", "Corridor","Carrier_Reps","Carrier","Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" , "Cost"]
        names = ["fac_rate", "cor_rate","crep_rate","c_rate","DO_Monday", "DO_Tuesday","DO_Wednesday","DO_Thursday","DO_Friday","DO_Saturday","PU_Monday","PU_Tuesday", "PU_Wednesday","PU_Thursday","PU_Friday","PU_Saturday",  "Cost"]
    elif flag == 5:
        reasons = ["Facility", "Corridor","Carrier_Reps","Carrier","Delivery Appt","Delivery Appt", "Delivery Appt", "Delivery Appt", "Delivery Appt","Delivery Appt", "Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt","Pick-up Appt" , "Cost"]
        names = ["fac_rate", "cor_rate","crep_rate","c_rate","DO_Monday", "DO_Tuesday","DO_Wednesday","DO_Thursday","DO_Friday","DO_Saturday","PU_Monday","PU_Tuesday", "PU_Wednesday","PU_Thursday","PU_Friday","PU_Saturday", "Cost"]
    elif flag == 6:
        reasons = ["Facility", "Carrier","PowerType", "Cost"]
        names = ["fac_rate", "cor_rate","PowerType ", "Cost"]
    elif flag == 7:
        reasons = [ "Carrier","PowerType", "Cost"]
        names = ["c_rate ", "PowerType ", "Cost"]
    else:
        reasons = [ "Customer","PowerType","Carrier_Reps","Carrier", "Pick-up Appt","Pick-up Appt","Pick-up Appt", "Cost"]
        names = ["cust_rate","PowerType","crep_rate","c_rate","PU_EarlyMorning","PU_Night","PU_Daytime","Cost"]
    refervalue = []
    coef = coef[0][1:]  # there is dimensional problem, coef is a 2d matrix, we need to be careful with that.
    # coef=coef.fillna(0, inplace=True)
    for i in range(0, len(names)):
        refervalue.append(pd.DataFrame.mean(inputdata[names[i]]))
    for i in range(0,len(outputdata)):
        delta = (outputdata[names].iloc[i] - refervalue).tolist() * coef
        # output['Reason'].loc[i] = np.where(output['Reason'].loc[i] == "" or output['Reason'].loc[i].isnull(),reasons[np.argmin(delta)],output['Reason'].loc[i])
        # we do not use np.where as it returns an array. we could use tolist() to change the one element array into a number, but it may be resource consuming
        outputdata['Reason'].iloc[i] = reasons[np.argmin(delta)] if outputdata['Reason'].iloc[i] == "" else outputdata['Reason'].iloc[i]
    return (outputdata)


def GML_stage(inputdata, outputdata, flag):
    # for stage 1, no carrier is booked
    # instead of using C(Code) but dummies, will make sure there is no dimensional change, i.e. maybe some dataset do not have all the levels of categories, if we use C(), it will be one dimension less than the trainning set
    if flag == 1:
        X_test = dmatrix('fac_rate + cor_rate + PowerType + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
                         outputdata,return_type='dataframe')
        y, X = dmatrices(
            'IsBounced ~fac_rate + cor_rate + PowerType + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            inputdata,return_type='dataframe')
    elif flag == 2:
        y, X = dmatrices(
            'IsBounced ~ fac_rate + cor_rate + crep_rate + c_rate + PowerType + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            inputdata, return_type='dataframe')
        X_test = dmatrix(
            'fac_rate + cor_rate + crep_rate + PowerType + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            outputdata, return_type='dataframe')
    elif flag == 3:
        y, X = dmatrices(
            'IsBounced ~ fac_rate + cor_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            inputdata, return_type='dataframe')
        X_test = dmatrix(
            'fac_rate + cor_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            outputdata, return_type='dataframe')
    elif flag == 4:
        y, X = dmatrices('IsBounced ~ fac_rate + cor_rate + crep_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
        inputdata,return_type='dataframe')
        X_test = dmatrix('fac_rate + cor_rate + crep_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
        outputdata, return_type='dataframe')
    elif flag == 5:
        y, X = dmatrices('IsBounced ~ fac_rate + cust_rate + cor_rate + crep_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            inputdata,return_type='dataframe')
        X_test = dmatrix('fac_rate + cust_rate + cor_rate + crep_rate + c_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday + Cost',
            outputdata, return_type='dataframe')
    elif flag == 6:
        y, X = dmatrices('IsBounced ~ fac_rate + c_rate + PowerType + Cost',
            inputdata,return_type='dataframe')
        X_test = dmatrix('fac_rate + c_rate + PowerType + Cost',outputdata, return_type='dataframe')
    elif flag == 7:
        y, X = dmatrices('IsBounced ~  c_rate + PowerType + Cost', inputdata, return_type='dataframe')
        X_test = dmatrix('c_rate + PowerType + Cost', outputdata, return_type='dataframe')
    else:
        y, X = dmatrices('IsBounced ~ cust_rate + PowerType + crep_rate + c_rate + PU_EarlyMorning + PU_Night + PU_Daytime + Cost',
            inputdata,return_type='dataframe')
        X_test = dmatrix('cust_rate + PowerType + crep_rate + c_rate + PU_EarlyMorning + PU_Night + PU_Daytime + Cost',
                         outputdata, return_type='dataframe')
    model = LogisticRegression(fit_intercept=False)
    model.fit(X, np.ravel(y))
    rate_result = model.predict(X_test)
    rate = model.predict_proba(X_test)[:, 1]
    outputdata['bounce_prob']=rate
    outputdata['bounce_pred'] = rate_result
    outputdata = Reason(model.coef_,outputdata,inputdata, flag)
    return outputdata

 
def GML_Cluster_Pre(inputdata,outputdata):
    y, X = dmatrices('IsBounced ~ fac_rate + cust_rate + cor_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday',
            inputdata,return_type='dataframe')
    X_test = dmatrix('fac_rate + cust_rate + cor_rate + DO_Monday + DO_Tuesday + DO_Wednesday + DO_Thursday + DO_Friday + DO_Saturday + PU_Monday + PU_Tuesday + PU_Wednesday + PU_Thursday + PU_Friday + PU_Saturday',
            outputdata, return_type='dataframe')
    model = LogisticRegression(fit_intercept=False)
    model.fit(X, np.ravel(y))
    rate_result = model.predict(X_test)
    rate = model.predict_proba(X_test)[:, 1]
    outputdata['bounce_prob']=rate
    outputdata['bounce_pred'] = rate_result
    outputdata = Reason(model.coef_,outputdata,inputdata, 0)
    return outputdata



def Get_Results(input_all, output_all):
    output1=output_all[(output_all['Loadtype'] == 'C') & (output_all['ProgressType'] > 1)]
    input1=input_all[input_all['Loadtype'] == 'C']
    output2=output_all[(output_all['Loadtype'] == 'S') & (output_all['ProgressType'] > 1)]
    input2 = input_all[input_all['Loadtype'] == 'S']
    output_pre=output_all[output_all['ProgressType'] == 1]
    output_pre=GML_Cluster_Pre(input_all, output_pre)
    range_k=set(output_all[output_all['ProgressType'] > 1][['Prebook_Type']])

    for  k in range_k :
        if (k == 0 or k == 1):
            print (k)
            if (len(output1[output1['Prebook_Type'] == k ]))>0:
                output1[output1['Prebook_Type'] == k ] = GML_stage(input1[input1['Prebook_Type'] <= 1], output1[output1['Prebook_Type'] == k],1)
            if (len(output2[output2['Prebook_Type'] == k ]))>0:
                output2[output2['Prebook_Type'] == k ] = GML_stage(input2[input2['Prebook_Type'] <= 1], output2[output2['Prebook_Type'] == k],2)
        elif (k == 2):
            print (k)
            if (len(output1[output1['Prebook_Type'] == k ]))>0:
                output1[output1['Prebook_Type'] == k ] = GML_stage(input1[input1['Prebook_Type']== k], output1[output1['Prebook_Type'] == k],3)
            if (len(output2[output2['Prebook_Type'] == k ]))>0:
                output2[output2['Prebook_Type'] == k ] = GML_stage(input2[input2['Prebook_Type']== k], output2[output2['Prebook_Type'] == k],4)

        elif (k == 3):
            print (k)
            if (len(output1[output1['Prebook_Type'] == k ]))>0:
                output1[output1['Prebook_Type'] == k ] = GML_stage(input1[input1['Prebook_Type']== k], output1[output1['Prebook_Type'] == k],5)
            if (len(output2[output2['Prebook_Type'] == k ]))>0:
                output2[output2['Prebook_Type'] == k ] = GML_stage(input2[input2['Prebook_Type']== k], output2[output2['Prebook_Type'] == k],5)

        elif (k in [4, 5, 6]):
            print (k)
            if (len(output1[output1['Prebook_Type'] == k ]))>0:
                output1[output1['Prebook_Type'] == k ] = GML_stage(input1[input1['Prebook_Type']>=4], output1[output1['Prebook_Type'] == k],6)
            if (len(output2[output2['Prebook_Type'] == k ]))>0:
                output2[output2['Prebook_Type'] == k ] = GML_stage(input2[input2['Prebook_Type']>=4], output2[output2['Prebook_Type'] == k],7)
        if (len(output2[output2['Prebook_Type'] == 5]))>0:
            output2[output2['Prebook_Type'] == 5] = GML_stage(input2[input2['Prebook_Type'] >= 4],
                                                          output2[output2['Prebook_Type'] == 5], 8)

    testData_df = pd.concat([output_pre, output1])
    testData_df = pd.concat([testData_df, output2])
    testData_df['RiskScore'] = 100 - testData_df['SafeScore'] * 20
    results = testData_df[
        ["LoadID", "LoadDate",  "ProgressType",  "bounce_prob", "bounce_pred","SafeScore", "Reason"]]
    results.to_csv("Bounce_Predict" + now.strftime("%Y%m%d%H%M") + ".csv", index=False)
    testData_df.to_csv("testdata" + now.strftime("%Y%m%d%H%M") + ".csv", index=False)   
    return (testData_df)

def get_dummies(data):
    dummies = pd.get_dummies(data['Appt_DO_DOW'])
    data['DO_Monday'] = dummies['Monday'].tolist()
    data['DO_Tuesday'] = dummies['Tuesday'].tolist()
    data['DO_Wednesday'] = dummies['Wednesday'].tolist()
    data['DO_Thursday'] = dummies['Thursday'].tolist()
    data['DO_Friday'] = dummies['Friday'].tolist()
    data['DO_Saturday'] = dummies['Saturday'].tolist()
    #data['DO_Sunday'] = dummies['Sunday'].tolist()
    dummies2 = pd.get_dummies(data['Appt_PU_DOW'])
    data['PU_Monday'] = dummies2['Monday'].tolist()
    data['PU_Tuesday'] = dummies2['Tuesday'].tolist()
    data['PU_Wednesday'] = dummies2['Wednesday'].tolist()
    data['PU_Thursday'] = dummies2['Thursday'].tolist()
    data['PU_Friday'] = dummies2['Friday'].tolist()
    data['PU_Saturday'] = dummies2['Saturday'].tolist()
    #data['PU_Sunday'] = dummies2['Sunday'].tolist()
    dummies3 = pd.get_dummies(data['Appt_PU_Time'])
    data['PU_EarlyMorning'] = dummies3['EarlyMorning'].tolist()
    data['PU_Night'] = dummies3['Night'].tolist()
    data['PU_Daytime'] = dummies3['Daytime'].tolist()
    #data['PU_Dawn'] = dummies3['Dawn'].tolist()
    return data


#def test(): 
if __name__ == "__main__":
    filename = "hist_temp.csv"
    if ((os.path.isfile(filename)) == False):
        trainData = Get_Histload_ALL(cn)
        testData = Get_dailyload(cn)
    else:
        trainData = pd.read_csv(filename)
        testData = Get_dailyload(cn)
    testData = get_dummies(testData)
    trainData = get_dummies(trainData)
    testData['bounce_prob'] = -1
    testData['bounce_pred'] = -1
    testData['SafeScore'] = -1
    testData['RiskScore'] = -1
    testData['Reason']=''
    Get_Results(trainData,testData)
    
