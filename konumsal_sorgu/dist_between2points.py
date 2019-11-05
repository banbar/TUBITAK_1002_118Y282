# -*- coding: utf-8 -*-
import psycopg2
import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Girdi:
#55152, 99108, 23056, 74285, 1303, 2305, 35645, 78542, 64251, 75123
tripID = 64251 #Nokta 1
tripID2= 75123 # Nokta 2
maxDistance=15000 # MongoDB sorgusu için maksimum uzaklık parametresi
# Baglanti ayarlari:
connPostgres = ["postgres", "postgres", "1234", "127.0.0.1", "5433"]
connMongoDB = ["localhost", 27017, "nyc"]

class mongoDB():
    def __init__(self,host, port, dbName):
        client = MongoClient(host, port)
        self.db = client[dbName]

        try:
            client.admin.command('ismaster')
            print("Connected to MongoDB Server\n\n")
            print("Avaiable collections within the database:", self.db.list_collection_names() )
            # nyc2015 
            self.nyc2015 = self.db.nyc_singleDay_23may
        except ConnectionFailure:
            print("Mongodb Server not available\n\n")

    def dist_calculate(self, tripID,tripID2,maxDistance):
        # Bu fonksiyon ile girdi yolculugun baslangic noktasinin enlem/boylami bulunuyor
        document = self.nyc2015.find_one({"properties.nid": tripID})
        # Yolculugun baslangic noktasi koordinatlari 
        x = document['geometry_pk']['coordinates'][0]
        y = document['geometry_pk']['coordinates'][1]

        # MongoDB sorgusu
        query = [{
            u"$geoNear": {
                u"near": {
                    u"type": u"Point",
                    u"coordinates": [
                        x, y
                    ]
                },
                u"distanceField":"dist.calculated",
                u"includeLocs":"dist.location",
                u"maxDistance":maxDistance
            }
        }]

        # Calisma suresi 
        start_time = datetime.datetime.now()
        cursor = self.nyc2015.aggregate(query)
        
        distance=-1
        result=list(cursor)
        # Sonuç içerisinde istenilen nokta aranıyor
        for i in range(len(result)):
            if tripID2 == result[i]["properties"]["nid"]:
                distance=result[i]["dist"]["calculated"]
                continue
        # Eğer belirtilen mesafede nokta mevcut değilse nokta bulunamadı yazıyor
        if distance==-1:
            distance="Nokta bulunamadı!"
        
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()
        
        return timediff,distance


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


    def dist_calculate(self, tripID,tripID2):
        cur = self.conn.cursor()
        query = "SELECT st_distance(a.l_pickup,b.l_pickup,true) " \
            "FROM day_2015_05_23 a,day_2015_05_23 b " \
            "where a.nid={} and b.nid={}".format(tripID, tripID2)

        # Calisma suresinin kaydedilmesi 
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # Bulunan komsularin kaydedilmesi
        rows = cur.fetchall()
        
        cur.close()
        return timediff, rows


# Baglanti nesnelerinin yaratilmasi 
M = mongoDB(*connMongoDB)
P = postgres(*connPostgres)

# İki nokta arası mesafe - Postgres
timediff, distance_p = P.dist_calculate(tripID, tripID2)

print("Postgres time: "+ str(timediff)+" saniye"+"\n"+"Postgres Distance: "+str(distance_p)+" Metre")


# iki nokta arası mesafe - MongoDB
timediff,distance2 = M.dist_calculate(tripID,tripID2,maxDistance)
print("Postgres time: "+ str(timediff)+" saniye"+"\n"+"Distance: "+str(distance2)+" Metre")
