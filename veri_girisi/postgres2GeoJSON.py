# Import the necessary modules
import psycopg2
import datetime

# In order to have a suitable temporal attribute in MongoDB, 'Z' must be added to the end of the date in MongoDB.
def rearrangeTimeFormat(t):
    # print(t)
    date = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    # print(str(date.isoformat()))
    # Adding the 'Z' at the end:
    s = str(date.isoformat())
    s = ''.join((s, 'Z'))
    # print(s)
    return s


def postgres2GeoJSON(conn, chunkSize, chunkID):
        # conn: Postgres connection object 
        # chunkSize: number of records to be converted to GeoJSON to ease the RAM operations
        # chunkID: the GeoJSON file is going to be saved by using this ID. starts from ZERO
        template = \
            '''
            {
            "type" : "Feature",
                "geometry_pk" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
                "properties": {
                    "ID_Postgres": %s,
                    "VendorID" : %s,
                    "passenger_count" : %s,
                    "store_and_fwd_flag" : "%s",
                    "RatecodeID" : %s,
                    "trip_distance" : %s,
                    "payment_type" : %s,
                    "fare_amount" : %s,
                    "extra" : %s,
                    "mta_tax" : %s,
                    "tip_amount" : %s,
                    "tolls_amount" : %s,
                    "improvement_surcharge" : %s,
                    "total_amount" : %s,
                    "tpep_pickup_datetime" : ISODate("%s"),
                    "tpep_dropoff_datetime" : ISODate("%s")},
                "geometry_do" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
            },

            '''

        # the head of the geojson file
        output = \
            '''
            '''

        outFileHandle = open("nyc2015_json_%s.geojson" % str(chunkID), "a")

        cur = conn.cursor()

        cur.execute(""" SELECT *
                            FROM staging
                            where id > {} and id <= {}
                            order by id """.format(chunkID * chunkSize, (chunkID + 1) * chunkSize))

        rows = cur.fetchall()
        c = 0
        for row in rows:
            record = ''
            id = row[0]
            vendorID = row[1]
            t_pickup = rearrangeTimeFormat(str(row[2]))
            t_dropoff = rearrangeTimeFormat(str(row[3]))
            passenger_count = row[4]
            trip_distance = row[5]
            pickup_longitude = row[6]
            pickup_latitude = row[7]
            ratecodeID = row[8]
            store_and_fwd_flag = row[9]
            dropoff_longitude = row[10]
            dropoff_latitude = row[11]
            payment_type = row[12]
            fare_amount = row[13]
            extra = row[14]
            mta_tax = row[15]
            tip_amount = row[16]
            tolls_amount = row[17]
            improvement_surcharge = row[18]
            total_amount = row[19]

            record += template % (pickup_longitude,
                                  pickup_latitude,
                                  id,
                                  vendorID,
                                  passenger_count,
                                  store_and_fwd_flag,
                                  ratecodeID,
                                  trip_distance,
                                  payment_type,
                                  fare_amount,
                                  extra,
                                  mta_tax,
                                  tip_amount,
                                  tolls_amount,
                                  improvement_surcharge,
                                  total_amount,
                                  t_pickup,
                                  t_dropoff,
                                  dropoff_longitude,
                                  dropoff_latitude)

            # Add the record to the GeoJSON file
            outFileHandle.write(record)

            c += 1

        # the tail of the geojson file
        output += \
                '''
                '''

        outFileHandle.write(output)
        outFileHandle.close()

        del rows
        cur.close()



# Generating the GeoJSON files to import MongoDB

# 1: Connect to the database
try:
    conn = psycopg2.connect(database="nyc2015",
                        user="postgres",
                        password="12345Aa",
                        host="127.0.0.1",
                        port="5432")

    print("Successfully Connected")
except:
    print("Connection failed")

# 2: Create a cursor, which would help executing the SQL statements
cur = conn.cursor()

# 3: Obtain the number of records in the table
sql = 'SELECT count(*) from yellow'
cur.execute(sql)
numRecords = cur.fetchone()
#numRecords = 146112989 #testing the script


# Determine the number of GeoJSON files to be generated based on
# i) the number of records and ii) chunk size
chunkSize = 2000000 #each GeoJSON file would contain this number of records
numChunks = results[0]//chunkSize + 1
#numChunks = 5 #testing the script

for i in range(numChunks):
    postgres2GeoJSON(conn, chunkSize, i)


