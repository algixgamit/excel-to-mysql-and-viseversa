# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 15:33:09 2017

@author: Thanakrit.B
"""
import convert_to_mysql
import pyodbc
from openpyxl.utils.dataframe import dataframe_to_rows
create_core = "Structure TEXT(255),TYPE TEXT(255),Nom TEXT(255),Prenom TEXT(255),Civilite TEXT(255),Date_de_naissance TEXT(255),lieu_Naissance TEXT(255)"
create_core1 ="N°Ins TEXT(255) NOT NULL,Nom_Etudiant TEXT(255),Prénom_Etudiant TEXT(255),TD TEXT(255),examen TEXT(255),rattrapage TEXT(255),Réf_Group TEXT(255),Cours TEXT(255),Fillière TEXT(255),PRIMARY KEY (N°Ins(20))"
create_core2 ="N°Ins TEXT(255) NOT NULL,Nom_Etudiant TEXT(255),Prénom_Etudiant TEXT(255),Moy1Sem TEXT(255),Crédit1Sem TEXT(255),Moy2Sem TEXT(255),Crédit2Sem TEXT(255),Moy3Sem TEXT(255),Crédit3Sem TEXT(255),Moy4Sem TEXT(255),Moy5Sem TEXT(255),Crédit5Sem TEXT(255),Moy6Sem TEXT(255),Crédit6Sem TEXT(255),PRIMARY KEY (N°Ins)"
create_core3 =""
create_core4 =""
insert_core = "Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
insert_core1 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,TD ,examen ,rattrapage ,Réf_Group ,Cours ,Fillière ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
select_core1 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,TD ,examen ,rattrapage ,Réf_Group ,Cours ,Fillière"
select_core1_2 = "N°Ins,Nom_Etudiant,Prénom_Etudiant,Moy1Sem,Crédit1Sem,Moy2Sem,Crédit2Sem,Moy3Sem,Crédit3Sem,Moy4Sem,Crédit4Sem,Moy5Sem,Crédit5Sem,Moy6Sem,Crédit6Sem"

insert_core2 = "N°Ins ,Nom_Etudiant ,Prénom_Etudiant ,Moy1Sem ,Crédit1Sem ,Moy2Sem ,Crédit2Sem ,Moy3Sem ,Crédit3Sem ,Moy4Sem ,Moy5Sem ,Crédit5Sem ,Moy6Sem ,Crédit6Sem ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
insert_core3 =""
insert_core4 =""
# Check if MS Access driver availabel
[x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]

# Connection String
#cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
con_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=D:/fdsp/SJA-DPDR-2021.mdb;"
   
    )
#conn_str = (
 #   r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
  #  r'DBQ=D:\\Fdsp\\SJA-DPDR-2021.accdb;'
   # )

# Initiate connection
cnxn = pyodbc.connect(con_str)
print('connection succesd')
# Initiate cursor
crsr = cnxn.cursor()

# Print all tables in database
###  for table_info in crsr.tables(tableType='TABLE'):
###    print(table_info.table_name)

# Print all view in database
### for view_info in crsr.tables(tableType='VIEW'):
###    print(view_info.table_name)

# Show column name
### for row in crsr.columns(table = 'DataAllTypeTeam_201601'):
###    print(row.column_name)

# Use cursor for sql excution
###crsr.execute("""
   ###           SELECT L1.N°Ins, L1.Nom_Etudiant, 
      ###        L1.Prénom_Etudiant, L1.Moy1Sem, L1.Crédit1Sem, L1.Moy2Sem, L1.Crédit2Sem, L1.Moy3Sem, L1.Crédit3Sem, L1.Moy4Sem, L1.Crédit4Sem, L1.Moy5Sem, L1.Crédit5Sem, L1.Moy6Sem, L1.Crédit6Sem
         ###     FROM L1;
###
   ###          """)
###row = crsr.fetchall()
#query  = "SELECT L1.N°Ins, L1.Nom_Etudiant,L1.Prénom_Etudiant, L1.Moy1Sem, L1.Crédit1Sem, L1.Moy2Sem, L1.Crédit2Sem, L1.Moy3Sem, L1.Crédit3Sem, L1.Moy4Sem, L1.Crédit4Sem, L1.Moy5Sem, L1.Crédit5Sem, L1.Moy6Sem, L1.Crédit6Sem  FROM L1;"
###print(row)
query = "SELECT "+ select_core1 +" FROM 0_L1;"
query1 = "SELECT "+ select_core1_2 +" FROM L1;"


# Import to pandas dataframe

import pandas as pd

#query = "select * from DataAllTypeTeam_201601 where status = 'N' and source_code like 'O%'"
df = pd.read_sql(query, cnxn)
for r in dataframe_to_rows(df,index=False,header=True):
           print('_____lines________    ',r)

df = pd.read_sql(query1, cnxn)
for r in dataframe_to_rows(df,index=False,header=True):
           print('_____lines________    ',r)

cnxn.close()
#convert_to_mysql.read_data_dataframe_mysql(df,"requete0",create_core1,insert_core1)#df 
