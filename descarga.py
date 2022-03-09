#!/usr/bin/env python
## FUNCTION CLASS DESTINATED TO DOWNLOAD TABLES FROM GS, SHAREPOINT, FTP ORMONGO  
# Created 12/02/2022 by Jaime Polanco
# Last modified  08/03/2022 by Jaime Polanco
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
            
            
            
