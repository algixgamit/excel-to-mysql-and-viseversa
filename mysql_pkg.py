import os
import mysql.connector as database 
import pyodbc
import pandas as pd

import data_acces_collect




def drop_table(mysql_name):
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
         statement ="DROP TABLE "+ mysql_name+" ;"
         cursor.execute(statement)
         print("DELETE FROM ", mysql_name)
    except database.Error as e: 
        print(f"Error  since deleting table database: {e}") 
    connection.close()

def chek_table(mysql_name):
    chek_rows_count = 0
    

    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
         statement ="SHOW TABLES from fdsp LIKE '"+mysql_name+"%';"
         cursor.execute(statement)
         chek_rows_count = cursor.fetchall()
         print("chek name table database =",chek_rows_count)
    except database.Error as e: 
        print(f"Error  chek name table database: {e}") 
    connection.close()
    if len(chek_rows_count) > 0:
        print("table existe========================== "+mysql_name)
        return True
    else: return False
    

def add_data3(mat,Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance,table_name): 
    
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
        statement = "INSERT INTO "+table_name+" (mat,Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" 
        data = (mat,Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance) 
        print('rrrrrrrrr       ',data)
        cursor.execute(statement, data)
        connection.commit() 
        print("Successfully added entry to database ")
        OK =True
    except database.Error as e: 
        print(f"Error adding entry to database: {e} ") 
        OK = False
    connection.close()
    return OK


def add_data( first_name , last_name ): 
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
        statement = "INSERT INTO employees (first_name,last_name) VALUES (%s, %s)" 
        data = (first_name, last_name) 
        print('rrrrrrrrr       ',data)
        cursor.execute(statement, data)
        connection.commit() 
        print("Successfully added entry to database")
    except database.Error as e: 
        print(f"Error adding entry to database: {e}") 
    connection.close()

def add_data2( data2 ,table_name): 
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
        statement = "INSERT INTO "+table_name+" (mat,Structure,TYPE,Nom,Prenom,Civilite,Date_de_naissance,lieu_Naissance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" 
        #data = tuple(reccord)
       # T1="er" T2="er"  T3="er"  T4="er" T5="er" T6="er"   T7="er"  T8="er"  data = (T1, T2, T3, T4, T5, T6, T7, T8)
       # print('(((((( ',tuple(data2),'  ))))((((  ')
        cursor.execute(statement, tuple(data2))
        connection.commit() 
        print("Successfully added entry to database")
    except database.Error as e: 
        print(f"Error adding entry to database: {e}") 
    connection.close()  

def add_data4( data2 ,table_name,insert_c): 
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
        statement = "INSERT INTO "+table_name+" ( "+insert_c
        print('(((((( ',data2,'  ))))((((  ')
        print('(((((( ',tuple(data2),'  ))))((((  ')
        print('(((((( ',statement,'  ))))((((  ')
        cursor.execute(statement, tuple(data2))
        connection.commit() 
        data_acces_collect.logging.info("Successfully added entry to database %s",table_name) 
        OK = True
    except database.Error as e: 
        data_acces_collect.logging.info(f"Error adding entry to database: %s ",e) 
  
        OK = False

    connection.close()  
    return OK


def add_data5( data2 ,table_name,insert_c): 
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try:
        statement = "INSERT INTO "+table_name+" ( "+insert_c
        print('(((((( ',data2,'  ))))((((  ')
        print('(((((( ',tuple(data2),'  ))))((((  ')
        print('(((((( ',statement,'  ))))((((  ')
        cursor.execute(statement, data2)
        connection.commit() 
        data_acces_collect.logging.info("Successfully added entry to database %s",table_name) 
        OK = True
    except database.Error as e: 
        data_acces_collect.logging.info(f"Error adding entry to database: %s ",e) 
        print(f"Error adding entry to database: %s ",e)
  
        OK = False

    connection.close()  
    return OK

def get_data(last_name):
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try: 
      statement = "SELECT first_name, last_name FROM employees WHERE last_name=%s"
      data = (last_name,)
      cursor.execute(statement, data) 
      for (first_name, last_name) in cursor:
        print(f"Successfully retrieved {first_name}, {last_name}") 
    except database.Error as e:
      print(f"Error retrieving entry from database: {e}") 
    connection.close()

def creat_table(table_name):
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try: 
      statement = "CREATE TABLE progress2(mat TINYTEXT NOT NULL,Structure TEXT(255),TYPE TEXT(255),Nom TEXT(255),Prenom TEXT(255),Civilite TEXT(255),Date_de_naissance TEXT(255),lieu_Naissance TEXT(255),PRIMARY KEY (mat(5)))"
      cursor.execute(statement) 
    except database.Error as e:
      print(f"Error retrieving entry from database: {e}") 
    connection.close() 

def creat_table2(table_name):
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try: 
      statement = "CREATE TABLE "+table_name+" (mat TINYTEXT NOT NULL,Structure TEXT(255),TYPE TEXT(255),Nom TEXT(255),Prenom TEXT(255),Civilite TEXT(255),Date_de_naissance TEXT(255),lieu_Naissance TEXT(255),PRIMARY KEY (mat(5)))"
      
      cursor.execute(statement) 
    except database.Error as e:
      print(f"Error retrieving entry from database: {e}") 
    connection.close()


def select_table(select_core1,table_name):
    con_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=D:/fdsp.accdb;"
   
    )
    cnxn = pyodbc.connect(con_str)
    print('connection succesd')
    crsr = cnxn.cursor()
    query = "SELECT "+ select_core1 +" FROM "+table_name+";"
    print('connection succesd',query)
    df = pd.read_sql(query, cnxn)
    cnxn.close()
    return df

def creat_table4(table_name,create_core):
    connection = database.connect(
    user= "root" , 
    password= "cncalgix" ,
    host="127.0.0.1", 
    database=" fdsp ") 
    cursor = connection.cursor() 
    try: 
      statement = "CREATE TABLE "+table_name+" ( "+create_core+" )"
      
      cursor.execute(statement) 
    except database.Error as e:
      print(f"Error retrieving entry from database: {e}") 
    connection.close()
#creat_table2("progress3")
T=['hjf','hg','gg','gg','gg','gg','gg','gg']
print('ffffffffffffff    ',type(T))
print('ffffffffffffff    ',(T))
#add_data2(T,'progress1')
#add_data3("mat1","Structure","TYPE","Nom","Prenom","Civilite","Date_de_naissance","lieu_Naissance","progress3") 
#add_data("Kofi", "Doe") 
#get_data("Doe") 
#add_data( "first_name" , "last_name" )
#add_data( "first_name" , "last_name" )