import csv
import json
import os
import arrow
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.set_keyspace("e39_taxi")


#0           1          2              3             4              5        6           7           8             
#"TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID","TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"

os.chdir('/')

with open("train.csv", "rt", encoding="utf8") as csvfile:
    reader = csv.reader(csvfile,delimiter=',')
    for row in reader:
       row=next(reader)
       trip_temp=json.loads(row[8])
       a=arrow.get(row[5])
       CQLString2=""
       CQLString5=""
       CQLString3=""
       CQLString4=""
       CQLString7=""
       
       valid= True;
       if(row[1]=='B' and row[4] and row[3] ):
               if(trip_temp):
                   tp1=tuple(next(iter(trip_temp)))
                   tp2=tuple(trip_temp.pop())
                   keys1 = tuple(float(str(t)[:7]) for t in tp1)
                   keys2 = tuple(float(str(t)[:7]) for t in tp2)
                   CQLString="(TripId, CallType, start, arrival,startAll, arrivalAll, standId, taxiId,  hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),str('\''+str(keys1)+'\''),str('\''+str(keys2)+'\''),str('\''+str(tp1)+'\''),str('\''+str(tp2)+'\''),row[3],row[4],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)
                   CQLString3 = "INSERT INTO startarrivalsorted "+CQLString
                   CQLString4 = "INSERT INTO startsorted "+CQLString
                   CQLString7 = "INSERT INTO arrivalsorted "+CQLString                   
               else:
                   CQLString="(TripId, CallType, standId, taxiId,  hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),row[3],row[4],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)    
               CQLString2 = "INSERT INTO standsorted "+CQLString
       elif(row[1]=='A'):
               if(trip_temp):
                   tp1=tuple(next(iter(trip_temp)))
                   tp2=tuple(trip_temp.pop())
                   keys1 = tuple(float(str(t)[:7]) for t in tp1)
                   keys2 = tuple(float(str(t)[:7]) for t in tp2)
                   CQLString="(TripId, CallType, start, arrival,startAll, arrivalAll,  taxiId, originCall,  hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),str('\''+str(keys1)+'\''),str('\''+str(keys2)+'\''),str('\''+str(tp1)+'\''),str('\''+str(tp2)+'\''),row[4],row[2],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)
                   CQLString3 = "INSERT INTO startarrivalsorted "+CQLString
                   CQLString4 = "INSERT INTO startsorted "+CQLString
                   CQLString7 = "INSERT INTO arrivalsorted "+CQLString
               else:
                   CQLString="(TripId, CallType, taxiId, originCall,  hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),row[4],row[2],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)    
               CQLString5 = "INSERT INTO callsorted "+CQLString
       elif(row[1]=='C' and row[4]):
               if(trip_temp):
                   tp1=tuple(next(iter(trip_temp)))
                   tp2=tuple(trip_temp.pop())
                   keys1 = tuple(float(str(t)[:7]) for t in tp1)
                   keys2 = tuple(float(str(t)[:7]) for t in tp2)
                   CQLString="(TripId, CallType, start, arrival,startAll, arrivalAll,  taxiId, hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),str('\''+str(keys1)+'\''),str('\''+str(keys2)+'\''),str('\''+str(tp1)+'\''),str('\''+str(tp2)+'\''),row[4],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)
                   CQLString3 = "INSERT INTO startarrivalsorted "+CQLString
                   CQLString4 = "INSERT INTO startsorted "+CQLString
                   CQLString7 = "INSERT INTO arrivalsorted "+CQLString
               else:
                   CQLString="(TripId, CallType, taxiId, hours, day, DayType, month, year) VALUES ({0},{1},{2},{3},{4},{5},{6},{7});".format(str('\''+row[0]+'\''),str('\''+row[1]+'\''),row[4],a.hour,a.day,str('\''+row[6]+'\''),a.month,a.year)
       else:
           valid=False
       if(valid): 
           CQLString1 = "INSERT INTO timeSorted "+CQLString
           CQLString6 = "INSERT INTO taxisorted "+CQLString
           CQLString8 = "INSERT INTO calltypesorted "+CQLString
           session.execute("BEGIN BATCH " + CQLString1+CQLString2+CQLString3+CQLString4+CQLString5+CQLString6+CQLString7+CQLString8+ "APPLY BATCH" )

    print('Over !')                                 


