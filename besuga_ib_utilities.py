import tkinter as tk
from tkinter import messagebox as msgbox
import openpyxl as oppy
import numpy as np
import pandas as pd


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
    print('fields1: 1: ', fields)
    for field in fields:
        row = Frame(root)
        lab = Label(row, width=15, text=field[0], anchor='w')
        v = StringVar(root, value=field[1])
        ent = Entry(row, textvariable=v)
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries.append((field, ent.get()))
        print('field: ', field)
        field[1] = ent.get()
    print('fields: ', fields)
    return fields


if __name__ == '__main__':

    print('hola')