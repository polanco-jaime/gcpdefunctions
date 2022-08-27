def normalize_(s):
                replacements = (
                ("á", "a"),
                ("é", "e"),
                ("í", "i"),('ï»¿',''),
                ("ó", "o"),
                ("ú", "u"), ("ñ", "n"), ("*",""),
                (".", "_"), ("/","_"),("\\", "_"),
                ("1_", ""),("2_", ""),("3_", ""), ("4_", ""), ("5_", ""),("6_", ""),("7_", ""),   
                ('├í','a'),  ("├®","e"), ('├¡','i'), ('├│','o'),
                ('├║','u'),                ('├▒','ni'),
                ('├ü','A'),('├Ç','A'),('├ç','A'),
                ('├ë','E'),("├ë","E"),("├ê","E"),("╔","E"),
                ('├ì','I'),("├î","I"),('├û','I'),('├Å','I'),
                ('├ô','O'),('├ï','O'),('├è','O'),
                ('├Ö','U'),
                ('├æ','Ñ'),('├É','ni'),('├æ','Ñ'),('Ð','ni'),
                ('├Ü','U'),
                ('├û','Í'  )  ,  ("8_", ""),("9_", "")    ,
                ("¿", ""),("?", ""),(" ", "_"),(",", "_"),("__", "_"),("-", "")
                )
                for a, b in replacements:
                  s = s.replace(a, b).replace(a.upper(), b.upper()).strip()
                return s    

class load_simple_file_class:
    import chardet
    import pandas as pd
    import pandas_gbq
    import pprint
    import time
    import re
    from google.cloud import bigquery
    # Método del codo para averiguar el número óptimo de clusters
    import warnings
    def __init__(self,
                 path = "" , 
                 sheet_number = 1,
                 table_name_bq = '',
                 dataset = '',
                 if_exists_table = 'replace'
                 ):  
        
        # Instance Variable 
        self.tabla =path
        self.formato_ = path.split('.')[-1].upper()
        self.table_name_bq = table_name_bq
        self.sheet_number = sheet_number
        self.dataset = dataset
        self.if_exists_table = if_exists_table

        
    def econde_tabla(self): 
        if (self.formato_.upper() == 'CSV') or (self.formato_.upper() == 'TXT'):        
            import chardet

            with open(self.tabla, 'rb') as rawdata:
                result = chardet.detect(rawdata.read(10000))
            if result['encoding'] == 'ascii':
              return 'ISO-8859-1'
            else:
              return result['encoding']
        else:
            pass        

    def delimitador(self):
        if (self.formato_.upper() == 'CSV') or (self.formato_.upper() == 'TXT'):
            self.encode = self.econde_tabla( )
            if pd.read_csv(self.tabla, sep=',', encoding= self.encode , nrows = 1).shape[1]>4:
                return ','
            elif pd.read_csv(self.tabla, sep=';', encoding= self.encode, nrows = 1).shape[1]>4:
                return ';'
            elif pd.read_csv(self.tabla, sep='|', encoding= self.encode, nrows = 1).shape[1]>4:
                return '|'
            elif pd.read_csv(self.tabla, sep=':', encoding= self.encode, nrows = 1).shape[1]>4:
                return ':'  
            else:
                pass  
                      
        else:
            pass
        #def reading_format

    def read_tables(self):
        self.start = time.time()
        if  (self.formato_.upper() == 'CSV'  or self.formato_.upper() == 'TXT' ) :
            Data  =  pd.read_csv(self.tabla, sep=self.delimitador(), encoding= self.econde_tabla() )
            
            return Data
       
        elif (self.formato_.upper() == 'XLS' or self.formato_.upper() == 'XLSX' ):     

          if self.sheet_number != 1:
            Data  = pd.read_excel(self.tabla, converters = self.types_dict(), sheet_name=self.sheet_number)
          else:
            Data  = pd.read_excel(self.tabla, converters = self.types_dict(), sheet_name= 1 )

          return Data
        else:
            pass
    def normalize_table (self):
      
      nombre_columnas = []
      Data = self.read_tables()
      
      for i in range(len(Data.columns)):
          lista  = normalize_(pd.DataFrame(list(Data.columns), columns =['columnas']).columnas[i].upper() )
          nombre_columnas.append(lista)
      
      Data.columns = nombre_columnas
      return Data


    def load_in_bq(self):
      destination_table =  self.dataset +'.'+ self.table_name_bq
      pandas_gbq.to_gbq(self.normalize_table() ,destination_table ,project_id=os.environ['PROJECT'],
                        if_exists = self.if_exists_table  )
      
      print('the file has been loadded in {}'.format(os.environ['PROJECT']+'.'+destination_table))
