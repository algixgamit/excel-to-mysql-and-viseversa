import pandas as pd
import json
import sys
from openpyxl.utils.dataframe import dataframe_to_rows
import mysql_pkg as mysql_data_source
#dictionaire=[]


def read_param_json(path:''):
    with open(path,'r',encoding='utf-8') as f:
             data = json.load(f)
             return data

def cheking_intigrite_data(record ,i = int) :      #print('(((((((((((((((((((((    ',len(record))
     #record[0] = i    
     print('@@@@@@@@@    ',record[0])
     return record



     #for i,x in enumerate(record) :
        # print(i,'((((((',x ,' : ',type(x))
        # if i==0:
      
       #  if not isinstance(x, str):
       #    record[i] = 'None'
       #    print('unkown type  ',type(x))
       #  print('manque data ',len(record))  
       #  if not len(record) == 38:
       #     print('manque data ',len(record))
        #    return False
         

mysql_dict=[]

row_list_query_preparation = []
#table_server_sql={ "host":"0.0.0.0:3306","user":"yourusername","password":"yourpassword",
  #"database":"mydatabase"}
#with open('mysql_dict.json','w') as ff:
  #  json.dump(table_server_sql, ff)


table_server_sql={"L": 5, "H": 7, "champ_class_in": [{"N": 0, "Structure": 1, "Photo": 2, "TYPE": 3, "Identifiant": 4,
 "Matricule": 5, "Nom": 6, "Prenom": 7, "Nom_Arabe": 8, "Prenom_Arabe": 9, "Nom_de jeune_fille": 10,
 "Nom_de_jeune_fille_arabe": 11, "Civilite": 12, "Presume": 13, "Date_de_naissance": 14, "lieu_Naissance": 15,
 "Lieu_de_naissance_arabe": 16, "Situation_familiale": 17, "Nationalite": 18, "Service_national": 19,
 "Groupe_Sanguin": 20, "Prenom_du_pere": 21, "Prenom_du_pere_arabe": 22, "Nom_de_la_mere": 23,
 "Nom_de_la_mere_arabe": 24, "Prenom_de_la_mere": 25, "Prenom_de_la_mere_arabe": 26, "Date_recrutement": 27,
 "Date_installation": 28, "N_securite_sociale": 29, "Date_affiliation": 30, "Type_Compte": 31, "N_compte": 32,
 "Grade": 33, "Date_Effet": 34, "Echelon": 35, "Categorie": 36}],"create": "CREATE TABLE names (id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,name VARCHAR(30),info TEXT,age TINYINT UNSIGNED DEFAULT '30',PRIMARY KEY (id))","insert":"INSERT INTO names (name, info, age) VALUES (MERZ, UIU, 12)"}
#with open('table.json','w') as ff:
 #json.dump(table_server_sql, ff)


#stmt_insert = "INSERT INTO names (name, info, age) VALUES (%s, %s, %s)"
dictionaire =  read_param_json('table.json')




mysql_dict =  read_param_json('mysql_dict.json')
#print('rrrrrrrrrrrrrrrrrrrr ',mysql_dict['create'])
#with open('table.json','w') as ff:
# json.dump(table_server_sql, ff)



def filtre_data(path:''):#df : pd.core.frame.DataFrame,   read from excel
        print('read from excel to mysql')
        print('ttttttt ',dictionaire['H'],' ======= ',dictionaire['L'])
        H=dictionaire['H']
        L=dictionaire['L']
        table_name = "test"
        file_name ="test.xlsx"
        skiprows = 10#nombre des lignes avant premiere ligne des donnees compter du zero
        choice_of_cel = 'C,E,H,I,N,P,Q'

        
        df = pd.read_excel(file_name,usecols='C,E,H,I,N,P,Q',skiprows=skiprows,keep_default_na=False,na_values=['nan'])#numero l   2       data  (1, None) 
        i =0
        mysql_data_source.creat_table4(table_name+"test",create_core)#create table 
        mysql_data_source.creat_table2(table_name)#create table     
        for r in dataframe_to_rows(df,index=True,header=True):
           print('_____lines________    ',r)
          # print('-----@@@@@@@ type reccord @@@@@@------   ',type(r))
        #insert tabl
           cheking_intigrite_data(r ,i)
           #  mysql_data_source.add_data2(r, table_name)       
           mysql_data_source.add_data4(r, table_name+"test",insert_core)
           i =i +1




### filtre_data(path = "test.xlsx")



        #else:
         #  print('raccord invalide to insert in data base')

        #  for x in r:
         #  print('_____lines________    ',x)  









