#----------------------------------------------------------------------
# Bu kod ile bir noktanin hangi poligon icinde kaldigi tespit edilmektedir 
#----------------------------------------------------------------------
import psycopg2
import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from haversine import haversine, Unit

# Girdi:
randomID = 556677
# Baglanti ayarlari:
connPostgres = ["postgres", "postgres", "1234", "127.0.0.1", "5433"]
connMongoDB = ["localhost", 27017, "nyc"]


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
            self.nyc2015 = self.db.nyc2015
        except ConnectionFailure:
            print("Mongodb Server not available\n\n")

    def pip(self,tripID):
        document = self.nyc2015.find_one({"properties.ID_Postgres": tripID})
        xP = document['geometry_pk']['coordinates'][0]
        yP = document['geometry_pk']['coordinates'][1]

        # Baslangic poligonunun bulunmasi 
        queryPickup = {}
        queryPickup["geometry"] = {
            u"$geoIntersects": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        xP, yP
                    ]
                }
            }
        }

        # Calisma suresi: 
        start_time = datetime.datetime.now()

        cursorPickup = self.nyc2015.find(queryPickup)

        # Nokta mevcut poligonlarin dışındaysa: 
        flag = 0

        for doc in cursorPickup:
            flag = 1
            polygonID = doc['properties']['LocationID']
        if (flag == 0):
            polygonID = None


        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        del cursorPickup

        return timediff, polygonID


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


    def pip(self, tripID):
        cur = self.conn.cursor()

        q_pip = "SELECT z1.gid as Origin \n" \
                "FROM trips t \n" \
                "FULL JOIN zones z1 ON ST_Contains(z1.geom, t.l_pickup) \n" \
                "WHERE t.id = {}".format(tripID)

        start_time = datetime.datetime.now()
        cur.execute(q_pip)
        finish_time = datetime.datetime.now()

        polygonID = cur.fetchall()[0][0]

        cur.close

        return (finish_time - start_time).total_seconds(), polygonID



# Baglanti nesneleri 
M = mongoDB(*connMongoDB)
P = postgres(*connPostgres)

t_M, polygonID_M = M.pip(randomID)

print("MongoDB")
print("\tExecution Time: ", t_M)
print("\tPolygon ID: ", polygonID_M)

t_P, polygonID_P = P.pip(randomID)

print("Postgres")
print("\tExecution Time:  ", t_P)
print("\tPolygon ID: ", polygonID_P)




