# Standard library imports
import sys

# Third party imports
import ib_insync as ibsync

# Local application imports
import  besuga_ib_close_positions as ibclose
import besuga_ib_open_positions as ibopen
import  besuga_ib_manage_db as ibmanagedb
import besuga_ib_utilities as ibutil
import besuga_ib_config as cf


if __name__ == '__main__':
    try:
        myib = ibsync.IB()
        mydb = ibutil.dbconnect(cf.dbhost, cf.dbname, cf.dbuser, cf.dbpassword)
        rslt = []
        q = input("Triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2' - exit to quit ")
        while q != "exit":
            sql = "SELECT connHost, connPort, connAccId, connClientId FROM connections WHERE "
            if q.lower() == "besugapaper":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'besugapaper7498'")
                break
            elif q.lower() == "xavpaper":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'xavpaper7497'")
                break
            elif q.lower() == "mavpaper1":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'mavpaper1'")
                break
            elif q.lower() == "mavpaper2":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'mavpaper2'")
                break
            elif q.lower() == "exit":
                sys.exit("Exit requested! ")
            else:
                q = input ("Unknown account! ")

        myib.connect(rslt[0][0], rslt[0][1], rslt[0][3])
        myaccId = rslt[0][2]
        myib.reqMarketDataType(4)
        myorderdict = {}
        q = input(" 1 - Manage DB Positions \n 2 - Open Positions \n 3 - Close Positions \n 4 - Fill Earnings Date \n"
                  " 5 - Fill account history \n 6 - Fill Greeks \n All - Manage-Open-Close \n")
        while q != "exit":
            if q == "1":
                ibmanagedb.manage_positions(myib, mydb, myaccId)
                break
            elif q == "2":
                ibopen.openpositions(myib, mydb, myaccId)
                break
            elif q == "3":
                ibclose.processopenpositions(myib, mydb)
                break
            elif q == "4":
                ibmanagedb.dbfill_earningsdate(mydb)
                break
            elif q == "5":
                ibmanagedb.dbfill_accounthistory(myib, mydb, myaccId)
                break
            elif q == "6":
                ibmanagedb.dbfillall_greeks(myib, mydb, myaccId)
                break
            elif q.upper() == "ALL":
                ibmanagedb.manage_positions(myib, mydb, myaccId)
                ibopen.openpositions(myib, mydb, myaccId)
                myib.sleep(10)
                ibclose.processopenpositions(myib, mydb)
                break
            elif q.lower() == "exit":
                sys.exit("Exit requested! ")
            else:
                q = input("Unknown option! ")

        ibutil.dbcommit(mydb)
        ibutil.dbdisconnect(mydb)
        myib.disconnect()
    except ConnectionRefusedError as cre:
        ibutil.error_handling(cre, "I cannot connect to Interactive Brokers: ")
    except Exception as e:
        ibutil.error_handling(e)


#def portfolio_to_dict(ib_):
#    try:
#        pfl = ib_.portfolio()
#        # dictionary de lists que contindrà les dades que volem recuperar de l'objecte Contract per cada PortfolioItem del Portfolio
#        d_contr = {'secType': [], 'conId': [], 'symbol': [], 'exchange': [], 'primaryExchange': [], 'currency': [], 'localSymbol': []}
#        # OrderedDict de lists que contindrà les dades que volem recuperar de la namedtupla PortfolioItem (excepte els detalls del Contract) per cada PortfolioItem del Portfolio del Portfolio
#        d_pfl = {'position': [], 'marketPrice': [], 'marketValue': [],'averageCost': [], 'unrealizedPNL': [], 'realizedPNL': [], 'account': []}
#        # recorrem tots els PortfoioItema Portfolio
#        for i in range(len(pfl)):
#            ib_.qualifyContracts(pfl[i].contract)
#            for k in d_contr.keys():
#                # afegim els valors (cada value de (key,value) és una llista) de cada atribut que recuperem de l'objecte Contract d'aquest PortfolioItem.
#                d_contr[k].append(getattr(pfl[i].contract, k))
#            for k in d_pfl.keys():
#                # afegim els valors (cada value de (key,value) és una llista) de cada valor que m'interessa de Portfolio Item ( a part dels detalls del contracte, recuperats abans)
#                d_pfl[k].append(getattr(pfl[i], k))
#        # posem tota la informació al dictionary pfl_values
#        d_pfl.update(d_contr)
#        # ordenem i retornem un OrderedDict
#        my_order=['conId', 'symbol', 'localSymbol', 'currency', 'secType', 'position', 'averageCost', 'marketPrice', 'marketValue', 'unrealizedPNL', 'realizedPNL']
#        od_pfl = OrderedDict((k, d_pfl[k]) for k in my_order)
#        return od_pfl
#    except Exception as e:
#        msg='Exception in function portfolio_to_dict \n'
#        error_handling(e, msg)
#        raise


#def open_workbook(filepath, sheetname):
#    wb = oppy.load_workbook(filepath, read_only=False, keep_vba=True)  # to open the excel sheet and if it has macros
#    sheet = wb.get_sheet_by_name(sheetname)
#    print(sheet['D2'].value)
#    sheet.cell(row=2, column=4).value = 'sample'
#    print(sheet['D2'].value)
#    wb.save(filepath)


#def makeform(root, fields):
#    entries = []
#    print('fields1: 1: ', fields)
#    for field in fields:
#        row = Frame(root)
#        lab = Label(row, width=15, text=field[0], anchor='w')
#        v = StringVar(root, value=field[1])
#        ent = Entry(row,textvariable=v)
#        row.pack(side=TOP, fill=X, padx=5, pady=5)
#        lab.pack(side=LEFT)
#        ent.pack(side=RIGHT, expand=YES, fill=X)
#        entries.append((field, ent.get()))
#        print ('field: ', field)
#        field[1] = ent.get()
#    print('fields: ', fields)
#    return fields


