-- Assume that all attributes are text based
-- Yellow taxi data are obtained from: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

CREATE TABLE trips_text(
                id serial NOT NULL,
                vendorid text,
                t_pickup text,
                t_dropoff text,
                num_passengers text,
                trip_distance text,
                l_pickup_lon text,
                l_pickup_lat text,
                ratecodeid text,
                flag_store text,
                l_dropoff_lon text,
                l_dropoff_lat text,
                payment_type text,
                fare_amount text,
                extra text,
                mta_tax text,
                surcharge text,
                tip text,
                tolls text,
                total text,
                constraint "PK_text" PRIMARY KEY (id))



-- Optimal trip table design

CREATE TABLE trips(
                id serial NOT NULL,
                vendorid character varying(1),
                t_pickup timestamp without time zone,
                t_dropoff timestamp without time zone,
                num_passengers smallint,
                trip_distance real,
                l_pickup_lon double precision,
                l_pickup_lat double precision,
                ratecodeid character(2),
                flag_store character(1),
                l_dropoff_lon double precision,
                l_dropoff_lat double precision,
                payment_type character(1),
                fare_amount real,
                extra real,
                mta_tax real,
                surcharge real,
                tip real,
                tolls real,
                total real,
                constraint "PK" PRIMARY KEY (id))



-- Create the pickup and dropoff geometry column
CREATE TABLE trips_geom(
                id serial NOT NULL,
                vendorid character varying(1),
                t_pickup timestamp without time zone,
                t_dropoff timestamp without time zone,
                num_passengers smallint,
                trip_distance real,
                l_pickup_lon double precision,
                l_pickup_lat double precision,
                ratecodeid character(2),
                flag_store character(1),
                l_dropoff_lon double precision,
                l_dropoff_lat double precision,
                payment_type character(1),
                fare_amount real,
                extra real,
                mta_tax real,
                surcharge real,
                tip real,
                tolls real,
                total real,
                l_pickup geometry(point, 4326),
                l_dropoff geometry(point, 4326),
                constraint "PK_2" PRIMARY KEY (id)
                )


-- Insert into the new taxi table and generate the geometry
insert into trips_geom(id, vendorid, t_pickup, t_dropoff, num_passengers, 
		        trip_distance, l_pickup_lon, l_pickup_lat, ratecodeid, flag_store, 
		        l_dropoff_lon, l_dropoff_lat, payment_type, fare_amount, extra, mta_tax, 
		        surcharge, tip, tolls, total, 
		        l_pickup, l_dropoff)

	select id, vendorid, t_pickup, t_dropoff, num_passengers, 
		        trip_distance, l_pickup_lon, l_pickup_lat, ratecodeid, flag_store, 
		        l_dropoff_lon, l_dropoff_lat, payment_type, fare_amount, extra, mta_tax, 
		        surcharge, tip, tolls, total,
		        ST_SetSRID(ST_Point(l_pickup_lon,l_pickup_lat),4326),
		        ST_SetSRID(ST_Point(l_dropoff_lon,l_dropoff_lat),4326)
	      
	from trips


-- Rename: trips_geom --> trips

-- Sample k-NN
SELECT id
FROM trips
ORDER BY l_pickup <-> (select l_pickup from yellow_geom where id = 11000000)
limit 10




