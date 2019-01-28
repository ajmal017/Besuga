import tkinter as tk
from tkinter import messagebox as msgbox

import mysql.connector
import pandas as pd
from numpy import sign
from ib_insync import *
from mysql.connector import errorcode


#   funció per fer error handling
def error_handling (e, initial_text='Exception'):
    #mostra un pop-up d'error
    root = tk.Tk()
    root.withdraw()
    text=initial_text + 'Error Number({0}): {1}'.format(e.errno, e.strerror)
    msgbox.showerror("Error", text)


#   grava un DataFrame a excel
def save_to_excel(data_frame, out_path = 'C;/TEST.xlsx', sheet_name='Sheet 1'):
    # crea o sobreescriu un excel amb la informació del dataframe data_frame
    # out_path nome del fitxer (amb path: C:/xxx/yyy.xlsx)
    # sheeet_name nom del sheet
    try:
        writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
        data_frame.to_excel(writer, sheet_name)
        writer.save()
    except IOError as e:
        error_handling(e,'I/O Error: ')
        raise
    except Exception as e:
        error_handling(e)
        raise


#   Obre un formulari per fer inputs (camps de l'input com a paràmetre)
def makeform(root, fields):
    entries = []
    for field in fields:
        row = Frame(root)
        lab = Label(row, width=15, text=field[0], anchor='w')
        v = StringVar(root, value=field[1])
        ent = Entry(row, textvariable=v)
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries.append((field, ent.get()))
        field[1] = ent.get()
    return fields

    
# calcula dies distància entre dues dates
def diffdaysfromtoday(date1):
    #entrem la data en format 20181026
    #calculem la distància en dies respecte a avui de ña data emtrada em termes absoluts
    from datetime import date, datetime
    t1 = str(date.today())
    t2 = date1
    tf1= str(t1)[0:4] + ","+ str(t1)[5:7] + "," + str(t1)[8:10]
    tf2 = str(t2)[0:4] + "," + str(t2)[4:6] + "," + str(t2)[6:8]
    tf1 = datetime.strptime(tf1, "%Y,%m,%d")
    tf2 = datetime.strptime(tf2, "%Y,%m,%d")
    delta = tf2 - tf1
    return abs(delta.days)


# calcula dies de distància des de datetoday
def diffdays(date1,date2):
    #entrem les dates en format 20181026, l'ordre de les datas és indiferent
    #caluulem la distància en dies entre les dues dates en termes absoluts
    from datetime import datetime
    if date1 <= date2:
        t1 = date1
        t2 = date2
    else:
        t1 = date2
        t2 = date1
    tf1 = str(t1)[0:4] + "," + str(t1)[4:6] + "," + str(t1)[6:8]
    tf2 = str(t2)[0:4] + "," + str(t2)[4:6] + "," + str(t2)[6:8]
    tf1 = datetime.strptime(tf1, "%Y,%m,%d")
    tf2 = datetime.strptime(tf2, "%Y,%m,%d")
    delta = tf2 - tf1
    # print(abs(delta.days))
    return abs(delta.days)

def dbconnect(hname, dbname, uname, pwd):
    try:
        cnx = mysql.connector.connect(
            host = hname,
            database = dbname,
            user = uname,
            passwd = pwd
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        print(err)
        raise
    return cnx


def dbdisconnect(connection):
    connection.disconnect


def dbcommit(connection):
    connection.commit()


def execute_query(mydb, query, values=None, commit=True):
    try:
        mycursor = mydb.cursor()
        mycursor.execute(query, values)
        if (query.startswith('SELECT')):
            return mycursor.fetchall()
        elif (query.startswith('INSERT')):
            if commit: mydb.commit()
            return mycursor.lastrowid
        elif (query.startswith('UPDATE') or query.startswith('DELETE')):
            if commit: mydb.commit()
            return mycursor.rowcount
    except Exception as err:
        raise
