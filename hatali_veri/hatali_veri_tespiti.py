import random
import psycopg2
import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os


# MongoDB sinifi
class mongoDB():
    def __init__(self,host, port, dbName):
        client = MongoClient(host, port)
        db = client.nyc
        self.collection = db[dbName]

        try:
            client.admin.command('ismaster')
            print("Connected to MongoDB Server\n\n")
        except ConnectionFailure:
            print("Mongodb Server not available\n\n")


#  Kac yolculugun baslangic ve bitis zamani ayni? 
    def sameStartEndTime(self):
        query = {}
        query["$expr"] = {
            u"$eq": [
                u"$properties.tpep_pickup_datetime",
                u"$properties.tpep_dropoff_datetime"
            ]
        }
        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        result = cursor.count()
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result
    
#  Kac yolculugun ucreti <= x? (x = 0)
    def totalPrice_LTE2X(self, x):       
        query = {}
        query["properties.total_amount"] = {
            u"$lte": x
        }

        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        result = cursor.count()
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result
    
#  Kac yolculukta x kisi bulunmakta? (x = 0)
    def numPassengers_Equal2X(self, x):
        query = {}
        query["properties.passenger_count"] = x

        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        finish_time = datetime.datetime.now()
        result = cursor.count()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result

# Kac yolculugun yolculuk suresi (SANIYE) >= threshold? 
    def numLongTrips(self, threshold):
        pipeline = [
            {
                u"$project": {
                    u"properties": 1.0,
                    u"dateDifference": {
                        u"$subtract": [
                            u"$properties.tpep_dropoff_datetime",
                            u"$properties.tpep_pickup_datetime"
                        ]
                    }
                }
            },
            {
                u"$match": {
                    u"dateDifference": {
                        u"$gte": threshold
                    }
                }
            },
            {
                u"$count": u"passing_scores"
            }
        ]

        start_time = datetime.datetime.now()

        cursor = self.collection.aggregate(
            pipeline,
            allowDiskUse=True
        )

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        for doc in cursor:
            result = doc

        del cursor
        return timediff, result
		
# Kac yolculugun başlangıç ve bitiş konumları aynı? 
    def sameStartEndLocation(self):
        pipeline = [
            {
                u"$match": {
                    u"geometry_pk": {
                        u"$exists": True
                    },
                    u"geometry_do": {
                        u"$exists": True
                    },
                    u"properties.ID_Postgres": {
                        u"$exists": True
                    }
                }
            },
            {
                u"$project": {
                    u"geometry_pk": 1.0,
                    u"geometry_do": 1.0,
                    u"properties.ID_Postgres": 1.0,
                    u"AyniMi?": {
                        u"$eq": [
                            u"$geometry_pk.coordinates",
                            u"$geometry_do.coordinates"
                        ]
                    }
                }
            },
                   {
                       u"$match": {
                           u"AyniMi?": True
                       }
                   }
        ]

        start_time = datetime.datetime.now()
        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = 0
        for doc in cursor:
            result = result + 1
        finish_time = datetime.datetime.now()
        
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result

# -----------------------------------------------------------------------
#   -------------   Postgres

class postgres():
    def __init__(self, dbName, userName, pswd, host, port):
        try:
            self.conn = psycopg2.connect(database=dbName,
                            user=userName,
                            password=pswd,
                            host=host,
                            port=port)
            print("Connected to PostgreSQL Server")
        except:
            print("Postgres connection failed!")

    # -- How many trips start and end at the same location?
    def sameStartEndLocation(self):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where l_pickup = l_dropoff"

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()

        cur.close()
        return timediff, result


#  Kac yolculugun baslangic ve bitis zamani ayni? 
    def sameStartEndTime(self):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where t_pickup = t_dropoff"

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()

        cur.close()
        return timediff, result


#  Kac yolculugun ucreti <= x? (x = 0)
    def totalPrice_LTE2X(self, x):
        # x: total price
        # Retrives the query result : How many "Total amount" value is less than or equal to input 'x'
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where  total <= {}".format(x)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result

#  Kac yolculukta x kisi bulunmakta? (x = 0)
    def numPassengers_Equal2X(self, x):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where  num_passengers = {}".format(x)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result

# Kac yolculugun yolculuk suresi (SANIYE) >= threshold? 
    def numLongTrips(self, threshold):
        cur = self.conn.cursor()

        query = "select count(*) "\
                "from trips " \
                "where DATE_PART('day', t_dropoff - t_pickup) * 60 * 60 * 24 + " \
                      "DATE_PART('hour', t_dropoff - t_pickup) * 60 * 60 + " \
                      "DATE_PART('minute', t_dropoff - t_pickup) * 60 + " \
                      "DATE_PART('second', t_dropoff - t_pickup) >= {}".format(threshold)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result


