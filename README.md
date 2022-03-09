# GCP_data_eng_functions

class_descarga = descarga(tabla = "Saludata",
         bucket="bucket-gcp" ,
         project_id="proyecto-gcp" ,
             service_account = """
                              """
         , source = "gcp")
file_ = class_descarga.download_rep_blob()
