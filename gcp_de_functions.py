#!/usr/bin/env python
## FUNCTION CLASS DESTINATED TO DOWNLOAD TABLES FROM GS, SHAREPOINT, FTP ORMONGO  
# Created 12/02/2022 by Jaime Polanco
# Last modified  08/03/2022 by Jaime Polanco
# sudo git clone https://github.com/JAPJ182/GCP_data_eng_functions.git

##############################################
# Requirements
############################################## 
from google.cloud import storage
from google.cloud import bigquery
import time
from datetime import date, timedelta
from datetime import datetime

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email
import pandas as pd
import os
import requests
import zipfile
import glob
import re
import openpyxl
import chardet
import patool 
### conection requirements 
!pip3 install py-topping
!pip3 install Office365-REST-Python-Client==2.2.1
!pip3 install pymongo
 

from tqdm import tqdm
import ftplib
from io import BytesIO
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pymongo

class descarga:
    
    #### Cuenta de servicio proveniente del json
 
    ### Constructor
    def __init__(self,tabla = "",bucket="" ,project_id="" , service_account = """""""", source = "", 
                Usuario="",Contrasena="",Servidor="",Puerto="",Archivos=[""] ):  
        
        # Instance Variable 
        self.tabla = tabla
        self.bucket = bucket
        self.project_id = project_id
        self.cuenta = service_account 
        self.source = source.upper()
        self.Usuario= Usuario
        self.Contrasena = Contrasena
        self.Servidor = Servidor
        self.Puerto = Puerto
        self.Archivos = Archivos
        
        
        ### setting conection and getting the last table    
    def blod_add(self):
        
        if self.source=='GCP':
            
            client = storage.Client(self.cuenta )
            bucket = client.get_bucket(self.bucket)
            blobs = bucket.list_blobs()
            print("Conection with {} is ok".format(bucket))

            data = [] 
            for item in blobs:
                if (self.bucket == "proyecto-covid-sds.appspot.com") and (self.tabla.upper() in item.name.upper() ) :     
                    df = [item.name , # NAME  1
                          pd.to_datetime(item.name.split('---')[-2].split('/')[-1].split('.')[-2], format = '%d-%m-%Y-%H:%M')  ,  # DATE 2
                          item.name.split('/')[-1].upper()  , # file_name 3
                          '.ZIP' in item.name.upper()  , # El archivo es zip 4
                          '---' in item.name.upper() ,  # El archivo fue importado por el servicio web 5
                          item.public_url , # URL 6
                          self.tabla in item.name ,   #consolidado 8 df1 7
                         ]
                    #print("\r File {} has been found it in: the fronted bucket ".format(self.tabla))
                    data.append(df)
    #            else:
    #                pass

                elif (self.tabla.upper() in item.name.upper()) and (self.bucket != "proyecto-covid-sds.appspot.com"):
                    try:
                        df = [item.name , # NAME  1 
                              pd.to_datetime(item.name.replace('otro.','').split('/')[-1].split('.')[1], format = '%d-%m-%Y')  ,  # DATE 2
                              item.name.split('/')[-1]  , # file_name 3 
                              '.ZIP' in item.name.upper()  , # El archivo es zip 4
                              '---' in item.name.upper() ,  # El archivo fue importado por el servicio web 5
                              item.public_url , # URL 6
                              self.tabla in item.name , ## Base de trabajadores de la salud  7
                             ]
                        #print("\r File {} has been found it in: the {} bucket ".format(self.tabla, self.bucket))
                        data.append(df)
                    except Exception as e:
                        print(e)
                else:
                    pass

            df =pd.DataFrame( data , columns = ['name', 'date_','file_name','zip','---', 'url', 'tabla' ]).sort_values(by=['date_'] ,   ascending = False).reset_index(drop=True) 
            try:
                df_interest = df.iloc[0,0]
                self.df_auditoria = df.iloc[0,1]
                return [df_interest,self.df_auditoria ]

            except IndexError as error:
                print("The file name referenced as {} doesnt exist".format(self.tabla))
                print(error)
        else:
            print("The object you are looking for is not a file into GCP ")

        #print("The file of interest {} corresponding to the date {} was found successfully".format(df.iloc[0,1].split("---")[-1] ,  df.iloc[0,1].split("---")[0] )
    #### Descarga de ultimo archivo            
    def download_rep_blob( self ):
        
        if self.source=='GCP': 
            client = storage.Client(self.cuenta )
            bucket = client.bucket(self.bucket)
            self.source_blob_name = self.blod_add()
            self.formato = '.' + self.source_blob_name[0].split('.')[-1] 
            blob1 = bucket.blob(self.source_blob_name[0])
            folder = self.tabla
            self.bucket_dest = 'archivos_cargados'
            self.destination_file_name = self.tabla.replace('.xlsx', '').replace('.csv', '').replace('.XLSX', '').replace('.CSV', '').replace('.xls', '')  + self.formato
            def upload_blob(self ):
                Bucket = client.bucket(self.bucket_dest)
                blob2 = Bucket.blob(self.tabla+'/'+self.source_blob_name[0])
                blob2.upload_from_filename(self.destination_file_name)

            def delete_blob(self ):
                BUCKET = client.bucket(self.bucket)
                blob3 = BUCKET.blob(self.source_blob_name[0])
                blob3.delete()
            try:
                blob1.download_to_filename( self.destination_file_name)
                upload_blob(self.bucket_dest,self.tabla, self.destination_file_name, self.source_blob_name[0])
                #delete_blob(self.bucket, self.source_blob_name)
                print('ok')
            except: 
                'error'
            print(
                "Blob {} downloaded to {}.".format(self.source_blob_name[0], self.destination_file_name ) 
                + "        " +
                "El archivo {} se elimino correctamente del bucket {}.".format(self.source_blob_name[0], self.bucket )
            )
            return   [self.destination_file_name, self.source_blob_name[1]  ]     
        else:
            print("The object you are looking for is not a file into GCP ")
            
    def Conexion( self ):
        

        if (self.source == 'FTP')  or (self.source == 'SHAREPOINT') or (self.source ==  'MONGO'): 
            self.Archivo_Objeto=[]
            ########################################### AUTENTICACION CON SHAREPOINT ###########################################
            if(self.source =="SHAREPOINT"):
                    for i, self.Archivo in tqdm(enumerate(self.Archivos)):
                        
                        print(f"Autenticando con sharepoint el archivo {self.Archivo}")
                        url = self.Servidor+self.Archivo
                        ctx_auth = AuthenticationContext(url)
                        if ctx_auth.acquire_token_for_user(self.Usuario, self.Contrasena):
                            ctx= ClientContext(url, ctx_auth)
                            web = ctx.web
                            ctx.load(web)
                            ctx.execute_query()
                            print(f"Autenticacion con sharepoint de archivo {self.Archivo} exitosa")
                        print(f"Descargando archivo {self.Archivo} de Sharepoint...")
                        response = File.open_binary(ctx, url)
                        # self.Archivo_Objeto.append(BytesIO())
                        # self.Archivo_Objeto[-1].write(response.content)
                        # self.Archivo_Objeto[-1].seek(0)
                        
                        open("./{}".format(self.Archivo.split("/")[-1].split('?')[0]), "wb").write(response.content)
                        print("Descargado archivo {} de Sharepoint con éxito".format(self.Archivo.split("/")[-1].split('?')[0]) )
                        return [(self.Archivo.split("/")[-1].split('?')[0]) , datetime.today().strftime('%Y-%m-%d')]
            ########################################### AUTENTICACION CON FTP ###########################################
            elif(self.source =="FTP"):
                    FTP = ftplib.FTP(self.Servidor, self.Usuario, self.Contrasena)
                    print("Autenticado con éxito en FTP")
                    for i, Archivo in tqdm(enumerate(self.Archivos)):
                        self.Archivo_Objeto.append(BytesIO())
                        print(f"Descargando archivo {self.Archivo} de FTP...")
                        FTP.retrbinary(f'RETR {Archivo}', self.Archivo_Objeto[-1].write)
                        print(f"Descargado archivo {self.Archivo} de FTP con éxito")
                  ########################################### AUTENTICACION CON MONGO ###########################################
            elif(self.source =="MONGO"):
                    uri = f"mongodb://{Usuario}:{Contrasena}@{self.Servidor}:{self.Puerto}"
                    self.Database = pymongo.MongoClient(uri,unicode_decode_error_handler='ignore')         
                    
        else:
            print("The object you are looking for is not foundit into a Sharepoint, mongo or FTP object")
            
            
class read_load_class:
    def __init__(self,tabla_de_interes = [] ,Dataset="",correos=[""],project_id="",SENDGRID_API_KEY="" , service_account = """"""""):  
        
        # Instance Variable 
        self.tabla = tabla_de_interes[0]
        self.auditoria = tabla_de_interes[1]
        self.emil = correos
        self.project_id = project_id
        self.SENDGRID_API_KEY = SENDGRID_API_KEY
        self.formato_ = tabla_de_interes[0].split('.')[-1].upper()
        self.table_name = tabla_de_interes[0].split('/')[-1].upper().split('.')[0]  
        self.cuenta  = service_account
        self.Dataset = Dataset
        self.Correos = correos
        self.SENDGRID_API_KEY = SENDGRID_API_KEY
 

    def econde_tabla(self): 
        if (self.formato_.upper() == 'CSV') or (self.formato_.upper() == 'TXT'):        
            import chardet

            with open(self.tabla, 'rb') as rawdata:
                result = chardet.detect(rawdata.read(10000))

            return result['encoding']
        else:
            pass

    def delimitador(self):
        if (self.formato_.upper() == 'CSV') or (self.formato_.upper() == 'TXT'):
            self.encode = self.econde_tabla( )
            if pd.read_csv(self.tabla, sep=',', encoding= encode , nrows = 1).shape[1]>4:
                sep_ = ','
            elif pd.read_csv(self.tabla, sep=';', encoding= encode, nrows = 1).shape[1]>4:
                sep_ = ';'
            elif pd.read_csv(self.tabla, sep='|', encoding= encode, nrows = 1).shape[1]>4:
                sep_ = '|'
            elif pd.read_csv(self.tabla, sep=':', encoding= encode, nrows = 1).shape[1]>4:
                sep_ = ':'  
            else:
                pass  
            return sep_          
        else:
            pass
        #def reading_format

    def types_dict(self):
        if self.formato_.upper() == 'CSV'  or self.formato_.upper() == 'TXT' :
            col_names = pd.read_csv(self.tabla,sep= self.delimitador(), nrows=0, encoding =  self.encode ).columns
            types_dict_ = { }
            types_dict_.update({col: str for col in col_names  })
            return types_dict_
        elif self.formato_.upper() == 'XLS' or self.formato_.upper() == 'XLSX':
            col_names = pd.read_excel(self.tabla, nrows = 1 )
            types_dict_ = { }
            types_dict_.update({col: str for col in col_names  })
            return types_dict_
        elif self.formato_.upper() == 'XLSB' :
            col_names = pd.read_excel(self.tabla, nrows = 1 )
            types_dict_ = { }
            types_dict_.update({col: str for col in col_names  })
            return types_dict_
        else:
            pass
        
        return  print('reading parameters have been set');


    def unziping_(self ):
        if (self.formato_.upper() == 'ZIP'):
            zipfile.ZipFile(self.tabla , 'r').extractall(os.getcwd()   + '/' + self.tabla.upper().replace('.ZIP','')   )
            return os.listdir(os.getcwd() + '/' + self.tabla.upper().replace('.ZIP','')  )
            "listing a table of files to load into bq"
            "and then read each one for loading"
        elif (self.formato_.upper() == 'RAR'):
            patoolib.extract_archive(self.tabla , outdir= os.getcwd()  + '/' + self.tabla.upper().replace('.RAR','')   )
            return os.listdir(os.getcwd() + '/' + self.tabla.upper().replace('.RAR','')  )
        else:
            pass
 
    def read_tables(self):
        self.start = time.time()
        if (self.formato_.upper() == 'ZIP'  or self.formato_.upper() == 'RAR'):
            pass
            "i need to get the table and then read, normalize colnames and then sent to bq"
        
        elif (self.formato_.upper() == 'CSV'  or self.formato_.upper() == 'TXT' ) :
            Data  =  pd.read_csv(self.file_, sep=self.delimitador(), encoding= self.econde_tabla() )
            
            return Data
        elif (self.formato_.upper() == 'XLS' or self.formato_.upper() == 'XLSX' ):    
            
            Data  = pd.read_excel(self.tabla, converters = self.types_dict())
            
            return Data
        else:
            pass
    def read_table(self):
        start = time.time()
        bq_client = bigquery.Client(self.cuenta)

        def normalize_(s):
                replacements = (
                ("á", "a"),
                ("é", "e"),
                ("í", "i"),('ï»¿',''),
                ("ó", "o"),
                ("ú", "u"), ("ñ", "n"), ("*",""),
                (".", "_"), ("/","_"),("\\", "_"),
                ("1_", ""),("2_", ""),("3_", ""), ("4_", ""), ("5_", ""),("6_", ""),("7_", ""),
                ("8_", ""),("9_", ""),("¿", ""),("?", ""),(" ", "_"),(",", "_"),("__", "_"),("-", ""))
                for a, b in replacements:
                  s = s.replace(a, b).replace(a.upper(), b.upper()).strip()
                return s        
        
        nombre_columnas = []
        Data = self.read_tables()
        Data['AUDITORIA_GCP'] = self.auditoria
        for i in range(len(Data.columns)):
            lista  = normalize_(pd.DataFrame(list(Data.columns), columns =['columnas']).columnas[i].upper() )
            nombre_columnas.append(lista)
        
        Data.columns = nombre_columnas
        schema_ = []
        for i in Data.columns:
            schema_line   =   bigquery.SchemaField("""{}""".format(i), 'STRING' , mode = 'NULLABLE')
            schema_.append(schema_line)
        print('esquema creado correctamente')
        
        return [Data, schema_, nombre_columnas]
        
        ##############################################
        #### Loading table on BQ

    def loading_in_bq(self):      
        bq_client = bigquery.Client() 
        ### JAIME MK TU YO DEL PASADO TE RECUERDA QUE NO PUEDES OLVIDAR EL PUTO GUION BAJO
        ##### NO OLVIDAR
        ### Tengo que cambiar el guin abajo #############################################################################
        table_in_bq = self.tabla.split('/')[-1].split('.')[0]+'_'
        temp_dataset = 'STAGING_SECRETARIA_SALUD'
        #self.Dataset 
        Table_read = self.read_table()
        Data = Table_read[0].astype(str)
        Schema = Table_read[1]

        self.table_id = "{}.{}.{}".format( self.project_id, temp_dataset, table_in_bq )
        job_config = bigquery.LoadJobConfig( schema= Schema, write_disposition='WRITE_TRUNCATE' )
        print("El esquema se utilizó correctamente" )
        try:
            df= Data.astype(str)
            job = bq_client.load_table_from_dataframe(Data, self.table_id , job_config=job_config )
            RESULTADO = job.result()  # Wait for the job to complete. 
        except Exception as e:
            RESULTADO =  e
            

        



        #return RESULTADO # [df,  schema_, df.columns ]

    
    
        print('se cargo la tabla a BQ adecuadamente')
        table_dest_bq = "{}.{}.{}".format(self.project_id, self.Dataset , table_in_bq  )

            ####PARAMETROS PARA EL MENSAJE
        sql = """
            CREATE OR REPLACE TABLE  {}
            OPTIONS () AS 
            SELECT * FROM {}.STAGING_DATA_WRANGLING.dw_{} 
            """.format(table_dest_bq, self.project_id  ,table_in_bq)
        

        job_config = bigquery.QueryJobConfig()
        job_config.allow_large_results = True
        query_job = bq_client.query( sql,  location='US',  job_config=job_config)  # API request - starts the query
        query_job.result()  # Wait for the job to complete.
        print('se leyo el primer query donde se crea la base ')
 
        table = bq_client.get_table(table_dest_bq)  # Make an API request.
    
    
        message = Mail(
            to_emails=self.Correos,
            from_email=Email('observatoriodesalud@saludcapital.gov.co', "SDS, Observatorio de Salud de Bogota"),
            subject="Carga exitosa del archivo " +str(self.tabla ),
            html_content="Buen día" + "<br>" +  "Espero este mensaje les encuentre bien" + "<br>" +   "<br>" +
            "EL archivo se ha cargado correctamente en BigQuery." + "<br>" + 
            "El archivo referente a la fehca {}: ".format(self.auditoria)+  "<br>" +
            "Esta base comprende de: " + str(Data.shape[0]) + " filas y de "+str(Data.shape[1]) + " columnas"+"<br>"+
            
            "El archivo consolidado  contiene {} filas y {} columnas to {}".format(table.num_rows, len(table.schema), table_dest_bq)+ "<br>" +
            
            "Saludos"+"<br>" +
            "Equipo del observatorio"+"<br>" +
            "Sistema Intgrado de la Secretaria de Salud "    )
        
        try:
            sg = SendGridAPIClient(self.SENDGRID_API_KEY)
            response = sg.send(message)
        
        except Exception as e:
            message = Mail(
                to_emails=self.Correos,
                from_email=Email('observatoriodesalud@saludcapital.gov.co', "SDS, Observatorio de Salud de Bogota"),
                subject="Falla al cargar el archivo " +str(self.tabla ),
                html_content="Buen día" + "<br>" +  "Espero este mensaje les encuentre bien" + "<br>" +   "<br>" +
            "EL archivo se no ha cargado correctamente en BigQuery." + "<br>" + 
            "El archivo referente a la fehca {}: ".format(self.auditoria)+  "<br>" +
            "Esta base comprende de: " + str(Data.shape[0]) + " filas y de "+str(Data.shape[1]) + " columnas"+"<br>"+
             
            "Saludos"+"<br>" +
            "Equipo del observatorio"+"<br>" +
            "Sistema Intgrado de la Secretaria de Salud "  
                    ) 
            sg = SendGridAPIClient(self.SENDGRID_API_KEY)
            response = sg.send(message)    
#############################################################################
        end = time.time()
        total= end - self.start
        return print('timepo total de cargue '+str( round(total/60, 0) ) + ' minutos')
                    
