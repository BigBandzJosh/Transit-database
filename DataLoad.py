import pyproj
import fiona
import shapely
import shapely.geometry
from shapely.ops import transform
import psycopg
from dotenv import load_dotenv
import os

"""
This script demonstrates how to read a GeoPackage file using Fiona, transform the geometry to a different CRS, and write the transformed data to a new GeoPackage file. then insert the data into a PostgresSQL/PostGIS database.
"""



load_dotenv()

db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')


"""
  This function connects to the database and returns the connection object.
  """
def connect_db():
  try:
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_password)
    print(f'Success! Connected to the DB. {db_name}')
    return conn
  except psycopg.Error as e:
    print(f'Error connecting to the database: {e}')
    return None
  
"""
  This function reads a GeoPackage file and prints some information about it.
  It will transform the geometry to a different CRS and write the transformed data to a new GeoPackage file.
  """
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
      
       
"""
This function reads a GeoPackage file and prints some information about it.
This function was mostly just for testing purposes and to make sure I converted to the right CRS or see the data.
"""
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

    

"""
This function reads a GeoPackage file and updates the information in the transit_route table in the database.
"""
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


"""
This function reads a GeoPackage file and inserts the information into the transit_priority_corridor table in the database.
"""
def insert_into_table():
    sql = '''
    INSERT INTO transit.transit_priority_corridor (identifier, name, corridor)
    VALUES (%s, %s, ST_GeomFromText(%s, 2961))
    '''
    with fiona.open('transit_transformed.gpkg', mode='r') as gpkg:
        with conn.cursor() as cur:
            for feature in gpkg:
                # Get the properties and geometry
                props = feature['properties']
                geom = shapely.geometry.shape(feature['geometry']).wkt
                
                # Execute SQL with the properties and geometry as parameters
                cur.execute(sql, (props['OBJECTID'], props['ROUTE_FULL'], geom))
            
        # Commit changes to the database
        conn.commit()
        print('Data loaded successfully!')

"""
I wrote this function before realizing I needed to do this part in SQL, I left the function in the code just for future reference.
"""
# def insert_into_table2():
#     sql = '''
#     INSERT INTO transit.transit_route_priority_corridor (route_number_full, identifier)
#     VALUES (%s, %s)
#     '''
#     with fiona.open('transit_transformed.gpkg', mode='r') as gpkg:
#         with conn.cursor() as cur:
#             for feature in gpkg:
#                 # Get the properties
#                 props = feature['properties']
                
#                 # Execute SQL with the properties and geometry as parameters
#                 cur.execute(sql, (props['ROUTE_FULL'], props['OBJECTID']))
            
#         # Commit changes to the database
#         conn.commit()
#         print('Data loaded successfully!')


"""
Basic function to print all the tables in the database.
"""
# def print_all_tables():
#     query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'transit'"
#     with conn.cursor() as cur:
#         cur.execute(query)
#         tables = cur.fetchall()
#         for table in tables:
#             print(table[0])


"""
Main function to run the script, I commented out my test functions and left the main functions to run.
"""
if __name__ == '__main__':
  conn = connect_db()
  # file = 'transit_transformed.gpkg'
  # test_read_gpkg(file)

  transform_geomtry()

  # print_all_tables()

  update_table()
  insert_into_table()
  # insert_into_table2()
  

    
