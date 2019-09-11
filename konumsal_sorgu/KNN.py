#----------------------------------------------------------------------
# Bu kod ile rastgele secilen bir noktanın en yakin k komsusu bulunmaktadir 
#----------------------------------------------------------------------
import psycopg2
import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from haversine import haversine, Unit

# Girdi:
kValue = 30
randomID = 55116350
# Baglanti ayarlari:
connPostgres = ["postgres", "postgres", "1234", "127.0.0.1", "5433"]
connMongoDB = ["localhost", 27017, "nyc"]


# Bu fonksiyon ile girdi yolculugun baslangic noktasinin enlem/boylami donduruluyor 
def findCoordinates(M_conn, tripID):
    document = M_conn.nyc2015.find_one({"properties.ID_Postgres": tripID})
    #print(document)
    x = document['geometry_pk']['coordinates'][0]
    y = document['geometry_pk']['coordinates'][1]
    return x, y

class mongoDB():
    def __init__(self,host, port, dbName):
        client = MongoClient(host, port)
        self.db = client[dbName]

        try:
            client.admin.command('ismaster')
            print("Connected to MongoDB Server\n\n")
            print("Avaiable collections within the database:", self.db.list_collection_names() )
            # nyc2015 
            self.nyc2015 = self.db.nyc2015
        except ConnectionFailure:
            print("Mongodb Server not available\n\n")

    def k_NN(self, tripID, k):
        # Kaydin getirilmesi 
        document = self.nyc2015.find_one({"properties.ID_Postgres": tripID})
        # Yolculugun baslangic noktasi koordinatlari 
        x = document['geometry_pk']['coordinates'][0]
        y = document['geometry_pk']['coordinates'][1]
        # print(x,y) - OK

        # MongoDB sorgusu
        query = {}
        query["geometry_pk"] = {
            u"$nearSphere": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        x, y
                    ]
                }
            }
        }

        # Calisma suresi 
        start_time = datetime.datetime.now()
        cursor = self.nyc2015.find(query).limit(k)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        k_NN = set()
        for doc in cursor:
            k_NN.add(doc['properties']['ID_Postgres'])

        del cursor
        return timediff, k_NN


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


    def k_NN_v2(self, tripID,k):
        cur = self.conn.cursor()
        query = "SELECT id " \
            "FROM trips " \
            "ORDER BY l_pickup <-> (select l_pickup from trips where id = {})" \
            "limit {}".format(tripID, k)

        # Calisma suresinin kaydedilmesi 
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # Bulunan komsularin kaydedilmesi
        rows = cur.fetchall()
        k_NN = set()
        for row in rows:
            k_NN.add(row[0])

        cur.close()
        return timediff, k_NN



connPostgres = ["postgres", "postgres", "1234", "127.0.0.1", "5433"]
connMongoDB = ["localhost", 27017, "nyc"]

# Baglanti nesnelerinin yaratilmasi 
M = mongoDB(*connMongoDB)
P = postgres(*connPostgres)


# Haversine uzaklığı analizi 


maxH_P = 0 # Max Haversine uzakligi - Postgres
maxH_M = 0 # Max Haversine uzakligi - MongoDB


maxH_P_neighID = -1
maxH_M_neighID = -1


tripX, tripY = findCoordinates(M, randomID)
trip = (tripY, tripX)

# KNN - Postgres-v2
t_Postgres, set_Postgres = P.k_NN_v2(randomID, kValue)

# KNN - MongoDB
t_MongoDB, set_MongoDB = M.k_NN(randomID, kValue)

# Eslesme yuzdesi
intersection = set_Postgres.intersection(set_MongoDB)


# Haversine uzakligi analizi
set_Postgres = list(set_Postgres)
for i in range(len(set_Postgres)):

    neighX, neighY = findCoordinates(M, set_Postgres[i])
    neigh = (neighY, neighX)
    # Haversine uzakliginin hesaplanmasi:
    d = haversine(trip, neigh, unit=Unit.METERS) # (lat-lon) - (lat-lon)
    if(d > maxH_P):
        maxH_P = d
        maxH_P_tripID = randomID
        maxH_P_neighID = set_Postgres[i]

set_MongoDB = list(set_MongoDB)
for i in range(len(set_MongoDB)):
    neighX, neighY = findCoordinates(M, set_MongoDB[i])
    neigh = (neighY, neighX)
    d = haversine(trip, neigh, unit=Unit.METERS)  # (lat-lon) - (lat-lon)
    if (d > maxH_M):
        maxH_M = d
        maxH_M_tripID = randomID
        maxH_M_neighID = set_Postgres[i]

print("Max Haversine - Postgres: ", maxH_P)
print("Max Haversine - MongoDB: ", maxH_M)

