# GCP_data_eng_functions
All this funciton are used for download files from differents souces and import that to bigquery, 
This funciton has 2 main classes.
The fist one called descarga and the second one is caleed read_load_class.
# descarga requiere this parameters. (tabla = "",bucket="" ,project_id="" , service_account = """""""", source = "",                 Usuario="",Contrasena="",Servidor="",Puerto="",Archivos=[""] )
# read_load_class requiere this parameters (tabla_de_interes = [] ,Dataset="",correos=[""],project_id="",SENDGRID_API_KEY="" , service_account = """"""""):  
#### For instance:        
class_descarga = descarga(tabla = "tablax",
         bucket="bucket-gcp" ,
         project_id="proyecto-gcp" ,
             service_account = """
                              """
         , source = "gcp")
file_ = class_descarga.download_rep_blob()
