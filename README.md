# GCP_data_eng_functions
All this funciton are used for download files from differents souces and import that to bigquery, 
This funciton has 2 main classes.
The fist one called descarga and the second one is caleed read_load_class.

descarga requiere this parameters. (tabla = "",bucket="" ,project_id="" , service_account = """""""", source = "",                 Usuario="",Contrasena="",Servidor="",Puerto="",Archivos=[""] )

read_load_class requiere this parameters (tabla_de_interes = [] ,Dataset="",correos=[""],project_id="",SENDGRID_API_KEY="" , service_account = """"""""):  
# For instance:        
# class_descarga = descarga(tabla = "tablax",
#          bucket="bucket-gcp" ,
#          project_id="proyecto-gcp" ,
#              service_account = """
#                               """
#          , source = "gcp")
#  file_ = class_descarga.download_rep_blob()

# Requirements are the next:
chardet==4.0.0
office365==0.3.15
Office365_REST_Python_Client==2.2.1
openpyxl==3.0.9
pandas==1.3.1
protobuf==3.19.4
pymongo==4.0.1
pytz==2021.1
requests==2.25.1
sendgrid==6.9.6
tqdm==4.61.2
