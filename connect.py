import psycopg2
from config import config

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
create extension postgis
""",
        """
        CREATE TABLE landmarks5
(
   gid serial NOT NULL,
   city character varying(50),
  city_ascii character varying(50),
  state_id character varying(100),
  state_name character varying(50),
  county_fips character varying(50),
  county_name character varying(50),
  county_fips_all character varying(100),
  county_name_all character varying(100),
  lat character varying(1000),
  lng character varying(1000),
  population character varying(10),
  density character varying(50),
  source character varying(100),
  military character varying(50),
  incorporated character varying(100),
  timezone character varying(100),
  ranking character varying(100),
  zips character varying(2000),
  id character varying(2000),
  the_geom geometry,
  CONSTRAINT landmarks5_pkey PRIMARY KEY (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'POINT'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4326)
)
        """,
        """
CREATE INDEX landmarks5_the_geom_gist
  ON landmarks5
  USING gist
  (the_geom )
""",
        """
copy landmarks5(city,city_ascii,state_id,state_name,county_fips,county_name,county_fips_all,county_name_all,lat,lng,population,density,source,military,incorporated,timezone,ranking,zips,id) FROM '/home/unhmguest/Desktop/uscities.csv' DELIMITERS ',' CSV HEADER;
""",
                """
UPDATE landmarks5
SET the_geom = ST_GeomFromText('POINT(' || lng || ' ' || lat || ')',4326)
""" )

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            exe = cur.execute(command)
        # close communication with the PostgreSQL database server
        statement = """
SELECT
ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), landmarks5.the_geom) AS planar_degrees,
city,
population
FROM landmarks5
ORDER BY planar_degrees ASC
LIMIT 5
"""
        cur.execute(statement)
        records = cur.fetchall()
        for row in records:
            print(row)
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()
