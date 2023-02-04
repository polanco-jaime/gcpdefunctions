import sys, os

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__


 
# blockPrint()
 
# enablePrint()
 
import pip
import os

def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])  
lista = ['geopandas', 'geopy' ]        
for i in lista:
  import_or_install(i)


from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import geopandas
from geopy.geocoders import Nominatim
from geopy import geocoders
from geopy.geocoders import GoogleV3
import time

class from_shp_to_csv_in_gs:
  def __init__(self, file_name = '' , project_id = None, bucket = None):
    self.file_name = file_name
    self.name_in_gs = file_name.split('/')[-1].split('.')[0] + '.parquet'
    self.buket_gs = bucket
  def reading_shp(self):
    shp_file = geopandas.read_file(self.file_name)
    shp_file = shp_file.to_crs(4326)
    shp_file['centroid']  = shp_file.centroid
    shp_file=shp_file.applymap(str )
    shp_file.to_parquet( self.name_in_gs )
#     shp_file.to_csv( self.name_in_gs , sep = "|")
    if project_id != None:
      from google.colab import auth
      auth.authenticate_user()
      name_document = "/content/{}*".format(self.name_in_gs)
      command = ('gsutil mv '+ name_document + ' gs://{}/'.format(self.buket_gs))
      import os 
      os.system(command)
