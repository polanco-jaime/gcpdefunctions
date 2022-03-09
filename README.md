# GCP_data_eng_functions
All this funciton are used for download files from differents souces and import that to bigquery, 
This funciton has 2 main funciton.
The fist one called 
class_descarga = descarga(tabla = "Saludata",
         bucket="bucket-gcp" ,
         project_id="proyecto-gcp" ,
             service_account = """
                              """
         , source = "gcp")
file_ = class_descarga.download_rep_blob()
