# Standard library imports
import sys

# Third party imports
import ib_insync as ibsync

# Local application imports
import  besuga_ib_close_positions as ibclose
import besuga_ib_open_positions as ibopen
import  besuga_ib_manage_db_positions as ibmanagedb
import besuga_ib_utilities as ibutil
import ib_config as ibconfig


if __name__ == '__main__':
    try:
        myib = ibsync.IB()
        mydb = ibutil.dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")
        acc = ""
        rslt = []
        while acc != "exit":
            acc = input("triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2'")
            if acc == "besugapaper":
                rslt = ibutil.execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
                break
            elif acc == "xavpaper":
                rslt = ibutil.execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
                break
            elif acc == "mavpaper1":
                rslt = ibutil.execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper1'")
                break
            elif acc == "mavpaper2":
                rslt = ibutil.execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper2'")
                break
            elif acc == "exit":
                sys.exit("Exit requested!")
            else:
                print("Unknown account!")
        myib.connect(rslt[0][0], rslt[0][1], 1)
        myib.reqMarketDataType(4)
        myaccId = rslt[0][2]
        myorderdict = {}

        ibmanagedb.manage_positions(myib, mydb, myaccId)
        for i in range(len(ibconfig.myscancodelist)):
            myscan = ibsync.ScannerSubscription(instrument='STK', locationCode='STK.US.MAJOR', scanCode=ibconfig.myscancodelist[i],
                                            aboveVolume=200000, marketCapAbove=10000000000, averageOptionVolumeAbove=10000)
            myorderdict[ibconfig.myscancodelist[i]] = ibopen.openpositions(myib, mydb, myaccId, myscan, ibconfig.mymaxstocks)
        myib.sleep(100)
        ibmanagedb.manage_positions(myib, mydb, myaccId)
        ibclose.processopenpositions(myib, mydb,  myaccId)
        ibmanagedb.manage_positions(myib, mydb, myaccId)

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


