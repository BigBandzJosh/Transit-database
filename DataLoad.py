import pyproj
import fiona
import shapely
import shapely.geometry
from shapely.ops import transform
import psycopg
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

def connect_db():
  try:
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_password)
    print(f'Success! Connected to the DB. {db_name}')
    return conn
  except psycopg.Error as e:
    print(f'Error connecting to the database: {e}')
    return None
  
def transform_geomtry():
  with fiona.open('transit.gpkg', mode='r') as gpkg:
    
    source_crs = pyproj.CRS.from_wkt(gpkg.crs_wkt)
    target_crs = pyproj.CRS.from_epsg(2961)
    conversion = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True).transform

    with fiona.open('transit_transformed.gpkg', 'w', driver='GPKG', crs=target_crs, schema=gpkg.schema) as output:

      features = []
      for feature in gpkg:
        geom = shapely.geometry.shape(feature['geometry'])
        geom = transform(conversion, geom)
        geom = shapely.geometry.mapping(geom)

        feature = {
          'geometry': geom,
          'properties': feature['properties']
        }
        features.append(feature)
      output.writerecords(features)
      
       
def test_read_gpkg(file_path):
    
    try:
        # Open the GeoPackage file
        with fiona.open(file_path, 'r') as source:
            print(f"Number of features: {len(source)}")
            print(f"CRS: {source.crs}")
            print(f"Driver: {source.driver}")
            print("Layer schema:")
            print(source.schema)
            
            # Print the first feature
            first_feature = next(iter(source), None)
            print("First feature:")
            print(first_feature)
    except Exception as e:
        print(f"An error occurred: {e}")

    
def update_table():
    sql = '''
    UPDATE transit.transit_route 
    SET route = ST_GeomFromText(%s, 2961)
    WHERE route_number_full IS NOT NULL
    '''

    with fiona.open('transit_transformed.gpkg', mode='r') as gpkg:
        with conn.cursor() as cur:
            for feature in gpkg:
                # Convert the geometry to WKT (Well-Known Text)
                geom = shapely.geometry.shape(feature['geometry']).wkt
                
                # Execute SQL with the geometry as a parameter
                cur.execute(sql, (geom,))
        
        # Commit changes to the database
        conn.commit()
        print('Data loaded successfully!')


# def print_all_tables():
#     query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'transit'"
#     with conn.cursor() as cur:
#         cur.execute(query)
#         tables = cur.fetchall()
#         for table in tables:
#             print(table[0])

if __name__ == '__main__':
  conn = connect_db()
  file = 'transit_transformed.gpkg'
  test_read_gpkg(file)

  # transform_geomtry()

  # Is there a way to print all the tables in the databse?

  # print_all_tables()

  # update_table()
  

    
