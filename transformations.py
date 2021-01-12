import pandas as pd
import math
from math import pi

Cov = pd.read_csv(r"C:\Users\kumar\Downloads\sorted_data\sorted_data.csv", sep=",", names = ["medallion","hack_license","pickup_datetime","dropoff_datetime","trip_time_in_secs","trip_distance","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","payment_type","are_amount","Surcharge","mta_tax","tip_amount","tolls_amount","total_amount"])


def deg2rad(deg) :
        return deg * (math.pi/180)
		
def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2) :
    R = 6371 
    dLat = deg2rad(lat2-lat1)  
    dLon = deg2rad(lon2-lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) *  math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c 
    return d
	
Cov['pickup_longitude'] = Cov['pickup_longitude'].astype(float)
Cov['pickup_latitude'] = Cov['pickup_latitude'].astype(float)
Cov['dropoff_latitude'] = Cov['dropoff_latitude'].astype(float)
Cov['dropoff_longitude'] = Cov['dropoff_longitude'].astype(float)
Cov["pickup_datetime"] = pd.to_datetime(Cov["pickup_datetime"], format="%Y-%m-%d %H:%M:%S")
Cov["dropoff_datetime"] = pd.to_datetime(Cov["dropoff_datetime"], format="%Y-%m-%d %H:%M:%S")
	
Cov = Cov[(df[['pickup_longitude', 'pickup_latitude']] != 0).all(axis=1)]
	 
Cov = Cov[(Cov[['dropoff_longitude', 'dropoff_latitude']] != 0).all(axis=1)]
	  
for name, ds in zip(["Cov"],[Cov]):
    
    print("---------------")
    print("{}\n".format(name))
    print(ds.isnull().sum())
    print("\n")
	
	
pd.concat([Cov.head(),Cov.tail()],axis=0)
	
	
Cov["year"] = Cov["pickup_datetime"].progress_apply(lambda x: x.year)
Cov["month"] = Cov["pickup_datetime"].progress_apply(lambda x: x.month)
Cov["day"] = Cov["pickup_datetime"].progress_apply(lambda x: x.day)
Cov["hour"] = Cov["pickup_datetime"].progress_apply(lambda x: x.hour)
Cov["minute"] = Cov["pickup_datetime"].progress_apply(lambda x: x.minute)
	
Cov['distance'] = getDistanceFromLatLonInKm(Cov['pickup_longitude'], Cov['pickup_latitude'],Cov['dropoff_longitude'],Cov['dropoff_latitude'])
	 
	 