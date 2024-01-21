# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 15:33:09 2017

@author: Thanakrit.B
"""
import convert_to_mysql
import pyodbc
import pandas as pd
import mysql_pkg as mysql_data_source
import mysql_pkg
from openpyxl.utils.dataframe import dataframe_to_rows
import logging
S_LOAD_EROR_ = 0
S_LOAD_OK_   = 0

# """ create_core = "Structure TEXT(255),TYPE TEXT(255),Nom TEXT(255),Prenom TEXT(255),Civilite TEXT(255),Date_naissance TEXT(255),lieu_Naissance TEXT(255)"
# create_core2 ="N°Ins TEXT(255) NOT NULL,Nom_Etudiant TEXT(255),Prénom_Etudiant TEXT(255),Moy1Sem TEXT(255),Crédit1Sem TEXT(255),Moy2Sem TEXT(255),Crédit2Sem TEXT(255),Moy3Sem TEXT(255),Crédit3Sem TEXT(255),Moy4Sem TEXT(255),Crédit4Sem TEXT(255),Moy5Sem TEXT(255),Crédit5Sem TEXT(255),Moy6Sem TEXT(255),Crédit6Sem TEXT(255),PRIMARY KEY (N°Ins(20))"
# create_core3 =""
# create_core4 =""
# insert_core = "Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
# insert_core2 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,Moy1Sem ,Crédit1Sem ,Moy2Sem ,Crédit2Sem ,Moy3Sem ,Crédit3Sem ,Moy4Sem ,Crédit4Sem,Moy5Sem ,Crédit5Sem ,Moy6Sem ,Crédit6Sem ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# select_core11 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,TD ,examen ,rattrapage ,Réf_Group ,Cours ,Fillière"
# select_core1_2 = "N°Ins,Nom_Etudiant,Prénom_Etudiant,Moy1Sem,Crédit1Sem,Moy2Sem,Crédit2Sem,Moy3Sem,Crédit3Sem,Moy4Sem,Crédit4Sem,Moy5Sem,Crédit5Sem,Moy6Sem,Crédit6Sem"

select_core_main = " N°Ins,Nom_Etudiant,Prénom_Etudiant,Sexe,Redoublant,Année_Bac,Note_Bac,Réf_Group,Réf_Section,Observation"
insert_core_main ="N°Ins,Nom_Etudiant,Prénom_Etudiant,Sexe,Redoublant,Année_Bac,Note_Bac,Réf_Group,Réf_Section,Observation)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" #"N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,TD ,examen ,rattrapage ,Réf_Group ,Cours ,Fillière ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
create_core_main ="N°Ins TEXT(255) NOT NULL,Nom_Etudiant TEXT(255),Prénom_Etudiant TEXT(255),Sexe TEXT(255),Redoublant TEXT(255),Année_Bac TEXT(255),Note_Bac TEXT(255),Réf_Group TEXT(255),Réf_Section TEXT(255),Observation TEXT(255)" #"N°Ins TEXT(255) NOT NULL,Nom_Etudiant TEXT(255),Prénom_Etudiant TEXT(255),TD TEXT(255),examen TEXT(255),rattrapage TEXT(255),Réf_Group TEXT(255),Cours TEXT(255),Fillière TEXT(255)"

 ### insert_core2 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,Moy1Sem ,Crédit1Sem ,Moy2Sem ,Crédit2Sem ,Moy3Sem ,Crédit3Sem ,Moy4Sem ,Moy5Sem ,Crédit5Sem ,Moy6Sem ,Crédit6Sem ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
# insert_core3 =""
# insert_core4 =""
 
def chek_table(mysql_name):                                            # test existing table mysql 
    return  mysql_pkg.chek_table(mysql_name)                       
def drop_table(mysql_name):                                         # delte table mysql
    return  mysql_pkg.drop_table(mysql_name)

def read_data_dataframe_mysql(data_frame,table_name,insert_c):#df 
  #print('read from data frame to mysql')
  global S_LOAD_OK_,S_LOAD_EROR_
  i = 0
  print('PROGRESSING ')
 
  for r in dataframe_to_rows(data_frame,index=False,header=True):        #load from excel to mysql
    
    if i % 500 == 0 :
        print('_',i,'_')
    i += 1
    if mysql_data_source.add_data4(r, table_name,insert_c):
       S_LOAD_OK_ +=  1
       logging.info('Adding rcord number %s',str(S_LOAD_OK_))
    else:
       S_LOAD_EROR_ +=  1
       logging.info(f'eror rcord number %s',str(S_LOAD_EROR_))

       

def load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1):          #load from acces to excel
    logging.info('load_table_afichage load from %s to mysql server %s',load_from_name,mysql_name) 
    df = mysql_data_source.select_table(select_core1,load_from_name)
    if chek_table(mysql_name) : 
     drop_table(mysql_name)
     print('drop rows table before loading')  
    else:
     mysql_data_source.creat_table4(mysql_name,create_core1)#create table 
    
    read_data_dataframe_mysql(df,mysql_name,insert_core1)#df 

def load_mysql_table(create_core,select_core1,insert_c,table_name):
    rs =mysql_data_source.select_table(select_core1,table_name)
    print('rrrrrr    ',chek_table(table_name))
    if  chek_table(table_name):
       drop_table(table_name) 
    mysql_data_source.creat_table4(table_name,create_core_main)
   
    for r in dataframe_to_rows(rs,index=False,header=False) :
      print('rrrrrr    ',r)
      mysql_data_source.add_data5( r ,table_name,insert_c)
      


if __name__ == "__main__":
    ## START (client side job)

 logging.basicConfig(filename='logserveur.log',filemode='w', level=logging.INFO)
 logging.info(f'Start loading operation') 

 # Check if MS Access driver availabel
 [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]
 # check table name exisiting

 load_mysql_table(create_core_main,select_core_main,insert_core_main,"Etudiants_L2")
 load_mysql_table(create_core_main,select_core_main,insert_core_main,"Etudiants_M2")
 load_mysql_table(create_core_main,select_core_main,insert_core_main,"Etudiants_L1")
 load_mysql_table(create_core_main,select_core_main,insert_core_main,"Etudiants_L3")
 load_mysql_table(create_core_main,select_core_main,insert_core_main,"Etudiants_M1")

 #chek existing to droping table rows
 """ mysql_name ="Affichage_note_L1"
 load_from_name ="0_L1"
 load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1)
 if not chek_table("L1"):
    drop_table("L1") 
 
 creat_table4("L1",create_core)


 mysql_name ="Affichage_note_L2"
 load_from_name ="0_L2"
 load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1)
 creat_table4(table_name,create_core)



 mysql_name ="Affichage_note_L3"
 load_from_name ="0_L3"
 load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1)
 creat_table4(table_name,create_core)



 mysql_name ="Affichage_note_M1"
 load_from_name ="0_M1"
 load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1)
 creat_table4(table_name,create_core)



 mysql_name ="Affichage_note_M2"
 load_from_name ="0_M2"
 load_table_afichage(select_core1,load_from_name,mysql_name,insert_core1)
 creat_table4(table_name,create_core)




 print('statistick correction OK RECORD =',S_LOAD_OK_,' EROR RECORD= ',S_LOAD_EROR_) """