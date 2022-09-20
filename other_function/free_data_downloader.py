from google.cloud import bigquery
from google.oauth2 import service_account as sa
from datetime import datetime
import pandas as pd
import pandas_gbq
import os
from datetime import datetime
from pytz import timezone
from google.colab import files

project_id = "lee-javeriana"
soucer_ = "lee-javeriana.downloading_data"

#Setting client
downloading = {
  "type": "service_account",
  "project_id": project_id,
  "private_key_id": "d2044d8660b03fd31d5e54ed94be8b0f4ecc0d96",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDEMMvbWm9Ol156\n+d+AqacvbHKqHDoARaNWSSUXgFwzJEeVeXILJXNCyX6Vi5+Txt99Is5CdyqhuhHt\nNNHqs4EVG/2t1rEXCO80X+vL/oaOWLXEBnSbUEgo+TGf6o/YqtUXTEcWZfG6BSDc\nDqJzpJeWGFU5zZepoZPhXCYDQrp8hbwl82B2jFFTNRztl3TWrEgLqASOYBALGNWs\n51LjgOXMip968fsPlKGdKoR7wYpbyEAKomn4lcofNpS4iBfjPE4eR2J9bL4jp18K\nFT6iagCd1RML99hWVeHs3c7DK47Z2JG500xXc5FE4a6E3NQgpGCaNWfIQu4rMltp\n/9Qyhu8zAgMBAAECggEAWwwCLyQC78gTkqKPdJhG/B0qAYPj7uhiatK7IfLx1tIJ\nXnbj1ARnPyFfW2tWfTAX22zhp7rB0BgX5atTl8YCK5e33Ir72CLPT9EMDrlqHHlS\nsUwFpWZWdp1eyEYEx5pQFeXSP7TDv35Nx5ILFfvWVVOpiY+9yH0xsa4f/fdjsBFY\nEQ0tdTTr7XBq22lWy6CO4hxUhoYqp31dimhty70SRM7qQfRQrWuuAumF3RLUgsgy\nrmBY8/ajEjHBi7/RloKVKH1xgtQX3SMYeHztKIfQ3i/a69MTMb5hBurU04v/2vPT\nLznJ/Wct+cHfUnHjMocJuQNHI9ZlF7/YCipvUqRcgQKBgQDqC7//gVL6ELM+JTqs\nHk0/ZC2TLId3j6q7Rna9FjktRGhYG2BAX7m+EIOexMshFdRqtMeJCKp+DuXODB8V\nVg7uNkvZ52hQClsZZRLV5b5HVnLA2JcFSbSvlTfCP0T8P9flx0XS2/wlosQpkZER\nk9BZQjm3lPR+PP6NYYAqrQMc4QKBgQDWmAKa90H78n2gIGvZXHpaBp2nrsGj91Bd\nb7uL/G0Gk6wcCMzYnuIMWdU8Wyiy7UNdA/J8bviwRYnuTkLRgp7kNxUFTzchGt9O\nfHGWD0TNtzm1ElDKHsfb0dmy5x2Ktw+4bgHw8hacftk/OEXe9od2xRQDPCGqtv6g\nerc8mB6akwKBgQCr7pXNDSP3r+z86jx/1ILK3rzcMQoTOuchTFJN9dVq+6Xrk7DR\nmYDJrzgBmm/ejjrNaKBseoFEuYz4IM90zROKyzhNi0GGCxWBQk7j8zIlFcyW/oVy\nzQJ/Og8ME46KAByKICmbDR/eRgP0xYrcbsnPyGa+Bh7V3djh445Ty2VOAQKBgQDQ\n8uCOjbVFIn9qHEeHNQG5iPmXnZTVF+m7oSDnlFmAbufFwFfdkQ8f4ZS8mTKOznGq\nLz68JL2nvX4peTCcmegm5O9l5RUT+ft3i7p519Ix0HezNOtPcxs9kh68kUd1mvwG\no67mMoMLzIOyiBOYn0mVvx3WbIWHmXljPzoOGTyrWwKBgQCqlnvolGK1EqZA4Oqc\nxaOJ6A0v1ihZu4M/2c8Gk+Hu6T8Abmxk3njqF+nH3gn4DYOHUzSLMzEGyUlGhFSU\nS07Iyn6Y3DiVBuCA1oNXOUQ5XBavhMEtt1TMhkV8y7iyjo1CspcqqwmauYPfiESi\niS29SE1AVrTlCCnLhoCSMFijuA==\n-----END PRIVATE KEY-----\n",
  "client_email": "downloading-data@lee-javeriana.iam.gserviceaccount.com",
  "client_id": "116050703601294628271",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/downloading-data%40lee-javeriana.iam.gserviceaccount.com"
}


credentials = sa.Credentials.from_service_account_info( downloading   )  
client = bigquery.Client(credentials=credentials, project= "lee-javeriana")
# datetime object containing current date and time
fecha_y_hora_actuales = datetime.now()
zona_horaria = timezone('America/Bogota')
fecha_y_hora_bogota = fecha_y_hora_actuales.astimezone(zona_horaria ).strftime("%Y-%m-%d %H:%M")

def table_exist(tabla = None):
  try:
    temp = client.get_table("lee-javeriana.downloading_data.{}".format(  tabla))
    Table_exist = 'Yes'
  except Exception as e:
    Table_exist = 'No'
    # print(e)
    # print("The table '{}' you are looking for dosen't exist, please contact with jaime.polanco@javeriana.edu.co and  k.castaneda@uniandes.edu.co".format(tabla))
  return Table_exist


  
def download_table(tabla = ''):
    # change
    sql = "SELECT * FROM `{}.{}`  ".format(soucer_,  tabla)
    df = pandas_gbq.read_gbq(sql, credentials=credentials,
                                  dialect='standard',
                                   use_bqstorage_api=True)
    df.to_csv('{}.csv'.format(tabla), index=False,header=True)
    files.download('{}.csv'.format(tabla))

    print()

def load_log(nombre = '', email = '', Downloaded_table = '',  fecha_y_hora_bogota = None ):
  client = bigquery.Client(credentials=credentials, project= "lee-javeriana")
  table_id = "lee-javeriana.downloading_data.log_downloads"
  rows_to_insert = [
      {"Name": nombre , "Email": email, "Downloaded_table": Downloaded_table, 'audit_field':str(fecha_y_hora_bogota) },
  ]

  errors = client.insert_rows_json(
      table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
  )  # Make an API request.
  if errors == []:
      print("New rows have been added.")
  else:
      print("Encountered errors while inserting rows: {}".format(errors))

# set data
nombre = input("Please type your name and last name: " )
uso_de_datos = input("Hey {},do you allow us to save your data? (keeping this record, we would observe the use of these tables) (y, n) ".format(nombre) )
 
if (uso_de_datos.upper() in 'YES'):
  while True:
    try:
      email = input("Thanks {}, Could you type your email: ".format(nombre) )
      if( ('@' not in email or  '.co' not in email ) or (len(email.split("@")[0]) <=6)  ):
  
          raise TypeError("The email you said does not correspond to an email.") 
      break
    except TypeError as t:
      print(t)
      print( "Could you please type again your email")

  
  while True:
    try:
      tabla_name = input("Enter the tables's name your are looking for (The table name is here:   https://bit.ly/free_use_tables ) :")
      if table_exist(tabla = tabla_name) == 'No':
        raise TypeError(  """The table '{}' you are looking for dosen't exist.
              The table name is here:   https://bit.ly/free_use_tables, 
               or if you can't find the data you need, please write to:
               Jaime Polanco <<jaime.polanco@javeriana.edu.co>> and
               Karen Castaneda  <<k.castaneda@uniandes.edu.co>> """.format(tabla_name)
               )
      break
    except TypeError as t:
      print(t)


  table_info = client.get_table("{}.{}".format(soucer_,  tabla_name))

  cols =    (len(["{0} {1}".format(schema.name,schema.field_type) for schema in table_info.schema]))
  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_- Can you imagine this to be a joke?  -_-_-" ) 
  print("-_-  Maybe you can imagine that I am hacking you  -_-" ) 
  print("-_- -_- -_-  I'm kidding. i'm just bored -_- -_- -_- " ) 

  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_-    Thinking and over thinking -_- -_- -_-" ) 
  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_-    Thinking and over thinking -_- -_- -_-" ) 

  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print("-_- -_- -_- -_- -_- -_- -_- -_-   -_- -_- -_- -_- -_-" )
  print(">>" )
  print("Table you are looking for is {}".format ( tabla_name ) )
  print(">>" )
  print("Table has {} columns".format ( cols ) )
  print(">>" )
  print("Table has {} rows".format(table_info.num_rows))
  print(">>" )
  print("Table Description: {}".format(table_info.description))
  print(">>" )
  try:
    df = download_table(tabla = tabla_name )
    import time
    resting_time = cols*(table_info.num_rows)  /1000000
    print("the system will rest {} minutes, while the table get be downloaded".format( round(resting_time/60,0) ) )
    time.sleep(resting_time)
    
    load_log(nombre = nombre,  email = email, Downloaded_table = tabla_name , fecha_y_hora_bogota = fecha_y_hora_bogota)  
  except:
    pass

else: 
  print("Thank you {}, sadly for us it can be very interesting to be able to track the data we provide you".format(nombre))
  pass

import os
os.system("rm ./*.*")
