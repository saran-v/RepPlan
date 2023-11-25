import pandas as pd
import csv
from zipfile import ZipFile
import os
import shutil
import subprocess
import time
from datetime import datetime
import pyodbc
import sys
import sqlalchemy

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SQLSPC19-1;'
                      'Database=SupplyChain;'
                      'Trusted_Connection=yes;')

server = 'SQLSPC19-1'
db = 'SupplyChain'
driver = 'ODBC Driver 17 for SQL Server'

def createDir(vendorId, timeStr):
    os.chdir(r"D:\Scripts\OPT\RepModel\Opt")

    if not os.path.exists(vendorId):
        try:
            os.mkdir(vendorId)
            print("Directory ", vendorId, " Created ")
        except FileExistsError:
            print("Directory ", vendorId, " already exists")
    else:
        print("Directory already exists")

    # r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs"
    # os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs")
    os.chdir(r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs")


    if not os.path.exists(timeStr):
        try:
            os.mkdir(timeStr)
            os.mkdir(timeStr + '_aux')
            print("Directory ", timeStr, " Created ")
        except FileExistsError:
            print("Directory ", timeStr, " already exists")
    else:
        print("Directory already exists")


def prepareItemDC(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_ARTICLE_DC] where [Vendor ID] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    df['Vendor ID'] = df['Vendor ID'].fillna(0)
    df['Vendor ID'] = df['Vendor ID'].astype(int)
    df['Vendor ID'] = df['Vendor ID'].astype(str)

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code
    df = df[(df['Forecast Flag'] == 'Y')]  # filter by forecast flag
    df = df[(df['Replenish Flag'] == 'Y')]  # filter by replenish flag
    df = df[(df['Global Drop Status'] == 'N')]  # filter by global drop status

    df = df[['Article', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'site','Family_Code_Desc','Weeks Of Supply For SS','Unrestricted Stock','STO Inbound Qty','confirmedQty',
             'STO Outbound Qty','Volume','DSX LeadTime','Avg Fcst Units','Avg Fcst Cost','MaxCubesPerContainer','Incoterm Group','Minor_Code','Minor_Code_Description','Origin Country']]

    df['Unrestricted Stock'] = df['Unrestricted Stock'].fillna(0)
    df['STO Inbound Qty'] = df['STO Inbound Qty'].fillna(0)
    df['STO Outbound Qty'] = df['STO Outbound Qty'].fillna(0)
    df['confirmedQty'] = df['confirmedQty'].fillna(0)

    df['Avg Fcst Units'] = df['Avg Fcst Units'].fillna(0)
    df['Avg Fcst Cost'] = df['Avg Fcst Cost'].fillna(0)
    df = df.replace({'': 0, ' ': 0, float('NaN'): 0})

    df['on_hand'] = (df['Unrestricted Stock'] + df['STO Inbound Qty'] ) - (df['confirmedQty'] + df['STO Outbound Qty'])
    df['sales'] = df['confirmedQty'] + df['Avg Fcst Units']
    df['cost'] = df['Avg Fcst Cost']/df["Avg Fcst Units"]
    df['cost'] = df['cost'].fillna(0)

    final_df = df[['Article', 'site', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'Family_Code_Desc','on_hand','Weeks Of Supply For SS','sales','Volume','DSX LeadTime','cost','MaxCubesPerContainer','Incoterm Group','Minor_Code','Minor_Code_Description','Origin Country']]
    final_df.rename(columns={'Article': 'item', 'site': 'location', 'Article Description' : 'item_desc', 'Vendor ID': 'vendor_id',
                             'Vendor Name':'vendor_name', 'Family Code - Key': 'family_code','Family_Code_Desc': 'family_group', 'on_hand': 'on_hand', 'Weeks Of Supply For SS': 'safety_stock', 'Volume':'volume', 'DSX LeadTime':'leadtime',
                             'MaxCubesPerContainer':'MaxCubes','Incoterm Group':'incoterm_group','Minor_Code':'minor_code','Minor_Code_Description':'minor_code_desc','Origin Country':'origin_country'}, inplace=True)

    final_df.to_csv(r"D:/Scripts/OPT/RepModel/Opt/item_master.csv", index=False)


def prepareForecast(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_FORECAST_1YEAR] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    # vid = list(map(int, configDict['vendor_id']))
    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code

    final_df = df[['Article', 'DC','FiscalWeek','FiscalYear','Wkly Forecast in Unit']]
    final_df.rename(columns={'Article': 'item', 'DC': 'location', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Wkly Forecast in Unit': 'units'}, inplace=True)

    final_df.to_csv(r"D:/Scripts/OPT/RepModel/Opt/forecast.csv", index=False)


def preparePO(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_OPEN_PO] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)
    df = df[df['Family Code'].notnull()] # skip if null family code

    final_df = df[['PO Doc Nbr', 'PO Item Nbr', 'Article', 'Site','FiscalWeek','FiscalYear','Open PO Qty', 'PO Create Date']]
    final_df.rename(columns={'PO Doc Nbr': 'po_number', 'PO Item Nbr':'PO_Item_Nbr','Article':'Article', 'Site': 'Site', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Open PO Qty': 'units', 'PO Create Date': 'createDate'}, inplace=True)
    final_df.to_csv(r"D:/Scripts/OPT/RepModel/Opt/open_po.csv", index=False)


def  JavaScipRun(vendorId, timeStr):

    os.chdir(r"D:\Scripts\OPT\RepModel\Opt")

    if os.path.isfile("result_java.csv"):
        os.remove("result_java.csv")

    _base_cmd = ['D:\\amazon-corretto-8.392.08.1-windows-x64-jdk\jdk1.8.0_392\\bin\java', '-classpath',
                 'RepPlan.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar;mssql-jdbc-12.2.0.jre8.jar;.;',
                 '-Djava.library.path=D:\Scripts\OPT\RepModel\Opt',
                 'com.optimizer.Optimizer']  # works

    subprocess.check_call(_base_cmd)

    # java is still running
    while not os.path.exists('result_java.csv'):
        time.sleep(1)

    # r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs"
    shutil.copyfile("PO_Data_Summary_Out.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "\\PO_Data_Summary_Out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("PO_Data_Summary_Out_Master_DB.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "\\PO_Data_Summary_Out_Master_DB_" +  timeStr + vendorId +".csv")

    shutil.copyfile("Plot_Data_out.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\Plot_Data_out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("Po_Performance_out.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\Po_Performance_out_" + timeStr + vendorId + ".csv")
    shutil.copyfile("StockOut_Summary_out.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\StockOut_Summary_out_" + timeStr + vendorId + ".csv")
    shutil.copyfile("PO_Exception_Out.csv", r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\PO_Exception_Out_" + timeStr + vendorId + ".csv")

    os.chdir(r"D:\Scripts\OPT\RepModel\Opt")

    shutil.copyfile("PO_Data_Summary_Out.csv", vendorId + "\PO_Data_Summary_Out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Plot_Data_out.csv", vendorId + "\Plot_Data_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("PO_Data_Summary_Out_Master_DB.csv", vendorId + "\PO_Data_Summary_Out_Master_DB_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Po_Performance_out.csv", vendorId + "\Po_Performance_out_" +  timeStr + vendorId +".csv")

def readVendorList():
    vendorList = []
    os.chdir(r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Inputs")
    #os.chdir(r"S:\ReplenishOpt\Inputs")
    shutil.copyfile('config.csv', r"D:\Scripts\OPT\RepModel\Opt\config.csv")
    shutil.copyfile('vendorList.csv', r"D:\Scripts\OPT\RepModel\Opt\vendorList.csv")

    os.chdir(r"D:\Scripts\OPT\RepModel\Opt")

    with open('vendorList.csv', 'r') as fd:
        reader = csv.reader(fd)
        for row in reader:
            vendorList.append(row[0])

    return vendorList

def writePlotDataOut(vendorId,timeStr):
    try:
        data = pd.read_csv(r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + '_aux\\Plot_Data_out_' +  timeStr + vendorId + '.csv')
        df = pd.DataFrame(data)
        df = df.fillna(value=0)

        cursor = conn.cursor()
        for row in df.itertuples():
            # print(str(row.ItemGroup) + ":" + str(row.Item) + ":" + str(row.Site) + ":" + str(row.Item_location) + ":" + str(row.Week) + ":" + str(row.Year) + ":" + str(row.Week_Year) + ":" + str(row.Demand) + ":" + str(row.Prod_Start) + ":" + str(row.POs) + ":" +
            #       str(row.R_Create_Date) + ":" + str(row.Receipts) + ":" + str(row.Inventory_PO) + ":" + str(row.Inventory_R) + ":" + str(row.Demand_I) + ":" + str(row.Demand_R) + ":" + str(row.Inventory_D) + ":" +
            #       str(row.Inventory) + ":" + str(row.SS_Weeks) + ":" + str(row.WOS) + ":" + str(row.VendorId) + ":" + str(row.Week_Date))
            cursor.execute('''
                        INSERT INTO opt.Plot_Data_out_stage ([ItemGroup],[Item],[Site] ,[Item_location],[Week],[Year],[Week_Year],[Demand],[Prod_Start],
                        [POs],[R_Create_Date],[Receipts],[Inventory_PO],[Inventory_R],[Demand_I],[Demand_R],[Inventory_D],[Inventory],[SS_Weeks],[WOS],[Vendor_Id], 
                        [Week_Date]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                           row.ItemGroup, row.Item, row.Site, row.Item_location, row.Week, row.Year, row.Week_Year, row.Demand, row.Prod_Start, row.POs, row.R_Create_Date, row.Receipts,
                           row.Inventory_PO, row.Inventory_R, row.Demand_I, row.Demand_R, row.Inventory_D, row.Inventory, row.SS_Weeks, row.WOS, row.VendorId, row.Week_Date)

        conn.commit()
    except Exception as e:
        print('Error in WritePlotDataOut:' + str(e))
        sys.exit("Error message")


def createExcepTable(vendorId):

    dropStmt = "IF OBJECT_ID('opt.PO_Exception_out_stage_" + vendorId + "', 'U') IS NOT NULL " \
                                    "DROP TABLE opt.PO_Exception_out_stage_" + vendorId + ";"

    cursor = conn.cursor()
    cursor.execute(dropStmt)
    conn.commit()

def writeExcepOutData(vendorId,timeStr):
    data = pd.read_csv(
        r"\\File-Share\Bobs_ShareMerchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\PO_Exception_Out_" + timeStr + vendorId + ".csv")
    df = pd.DataFrame(data)
    df = df.fillna(value=0)

    db_conn = f'mssql://@{server}/{db}?driver={driver}'

    # engine = sqlalchemy.create_engine('Driver={SQL Server};Server=ENG0000003235;Database=Bobs;Trusted_Connection=yes;')
    engine = sqlalchemy.create_engine(db_conn)
    # con = engine.connect()

    df.to_sql('PO_Exception_out_stage_' + vendorId, engine, schema="opt", if_exists="append", index=False)


def writeStockOutData(vendorId,timeStr):
    try:
        data = pd.read_csv(r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "_aux\\StockOut_Summary_out_" + timeStr + vendorId + ".csv")
        df = pd.DataFrame(data)
        df = df.fillna(value=0)

        # db_conn = f'mssql://@{server}/{db}?driver={driver}'
        # engine = sqlalchemy.create_engine(db_conn)
        #
        # df.to_sql('StockOut_Summary_out_stage', engine, schema="opt", if_exists="append", index=False)

        cursor = conn.cursor()
        for row in df.itertuples():
            # print(row)
            cursor.execute('''
                        INSERT INTO opt.StockOut_Summary_out_stage ([ItemGroup],[Item],[Site],[Item_location],[Week],[Year],[Week_Year],[Demand],[POs],[R_Create_Date],[Receipts],[Inventory_PO],[Inventory_R]
                        ,[Demand_I],[Demand_R] ,[Inventory_D],[Inventory],[SS_Weeks],[WOS],[Reason],[Value],[Vendor_Name],[Vendor_Id],[Family_Code]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                           row.ItemGroup, row.Item, row.Site, row.Item_location, row.Week, row.Year, row.Week_Year, row.Demand, row.POs, row.R_Create_Date, row.Receipts, row.Inventory_PO, row.Inventory_R,
                          row.Demand_I, row.Demand_R, row.Inventory_D, row.Inventory, row.SS_Weeks, row.WOS, row.Reason, row.Value, row.Vendor_Name, row.Vendor_Id, row.Family_Code)
        conn.commit()
    except Exception as e:
        print('Error in writeStockOutData:' + str(e))
        sys.exit("Error message")


def writePODataOutSummaryMasterDB(vendorId,timeStr):
    try:
        data = pd.read_csv(r"\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\\" + timeStr + "\\PO_Data_Summary_Out_Master_DB_" + timeStr + vendorId + ".csv")
        df = pd.DataFrame(data)

        cursor = conn.cursor()
        for row in df.itertuples():
            cursor.execute('''
                        INSERT INTO opt.Po_data_summary_out_stage ([RunDate],[Planner] ,[ItemGroup],[Family_Code],[Item],[Site],[Article_Desc],[Vendor_Id],[Vendor_Name],[PO_Index],[PO_Week_Date]
                        ,[PO_Week],[PO_Year],[POs],[Item_Volume],[Volume],[Wos],[PO_Volume],[SS_Weeks],[WOS_SS_Ratio],[WK_AFTER_LT]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
                        ''',
                           row.RunDate, row.Planner, row.ItemGroup, row.Family_Code, row.Item, row.Site, row.Article_Desc, row.Vendor_Id, row.Vendor_Name, row.PO_Index, row.PO_Week_Date,
                           row.PO_Week, row.PO_Year, row.POs, row.Item_Volume, row.Volume, row.Wos, row.PO_Volume, row.SS_Weeks, row.WOS_SS_Ratio, row.WeeksAfterLT)

        conn.commit()
    except Exception as e:
        print('Error in writePODataOutSummaryMasterDB' + str(e))
        sys.exit("Error message")


def writePODataOutSummaryMasterDBT():
    try:
        data = pd.read_csv(r'\\File-Share\Bobs_Share\Merchandising_Shared\Supply Chain Automation\ReplenishOpt\Outputs\PO_Data_Summary_Out_Master_DB_2023_09_18_15_36_20006201.csv')
        df = pd.DataFrame(data)

        cursor = conn.cursor()
        for row in df.itertuples():
            cursor.execute('''
                        INSERT INTO opt.Po_data_summary_out_stage ([RunDate],[Planner] ,[ItemGroup],[Family_Code],[Item],[Site],[Article_Desc],[Vendor_Id],[Vendor_Name],[PO_Index],[PO_Week_Date]
                        ,[PO_Week],[PO_Year],[POs],[Item_Volume],[Volume],[Wos],[PO_Volume],[SS_Weeks],[WOS_SS_Ratio]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
                        ''',
                           row.RunDate, row.Planner, row.ItemGroup, row.Family_Code, row.Item, row.Site, row.Article_Desc, row.Vendor_Id, row.Vendor_Name, row.PO_Index, row.PO_Week_Date,
                           row.PO_Week, row.PO_Year, row.POs, row.Item_Volume, row.Volume, row.Wos, row.PO_Volume, row.SS_Weeks, row.WOS_SS_Ratio)

        conn.commit()
    except Exception as e:
        print('Error in writePODataOutSummaryMasterDBT' + str(e))
        sys.exit("Error message")


def clearAndWriteItemMaster():
    try:
        data = pd.read_csv(r'D:\Scripts\OPT\RepModel\Opt\item_master.csv')
        df = pd.DataFrame(data)

        cursor = conn.cursor()
        for row in df.itertuples():
            cursor.execute('''
                        DELETE FROM opt.Item_Master WHERE item = ? and site = ?
                        ''',
                           row.item, row.location)
        conn.commit()

        for row in df.itertuples():
            cursor.execute('''
                        INSERT INTO opt.Item_Master ([item],[site],[item_desc],[vendor_id],[vendor_name],[family_code],[family_group],[on_hand],[safety_stock],[sales]
                        ,[volume],[leadtime],[cost],[MaxCubes], [incoterm_group], [minor_code], [minor_code_desc], [origin_country]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                           row.item, row.location, row.item_desc, row.vendor_id, row.vendor_name, row.family_code, row.family_group, row.on_hand, row.safety_stock, row.sales,
                           row.volume, row.leadtime, row.cost, row.MaxCubes, row.incoterm_group, row.minor_code, row.minor_code_desc, row.origin_country)
        conn.commit()

    except Exception as e:
        print('Error in clearAndWriteItemMaster' + str(e))
        sys.exit("Error message")


def clearTables(vendorId):
    try:
        cursor = conn.cursor()
        sql_Delete_query = "DELETE FROM opt.Po_data_summary_out_stage WHERE Vendor_Id = ?"
        cursor.execute(sql_Delete_query, vendorId)
        conn.commit()

        sql_Delete_query = "DELETE FROM opt.Plot_Data_out_stage WHERE Vendor_Id = ?"
        cursor.execute(sql_Delete_query, vendorId)
        conn.commit()

        sql_Delete_query = "DELETE FROM opt.StockOut_Summary_out_stage WHERE Vendor_Id = ?"
        cursor.execute(sql_Delete_query, vendorId)
        conn.commit()

        sql_Delete_query = "DELETE FROM opt.Po_Performance_out_stage WHERE Vendor_Id = ?"
        cursor.execute(sql_Delete_query, vendorId)
        conn.commit()

    except Exception as e:
        print('Error in Deletion' + str(e))
        sys.exit("Error message")

def writeTables(vendorList):

    db_conn = f'mssql://@{server}/{db}?driver={driver}'
    engine = sqlalchemy.create_engine(db_conn)
    # cnx = engine.connect()

    df = pd.read_sql_query('''SELECT *  FROM [SupplyChain].[opt].[Item_Master]''', con=engine)
    df.to_csv('Item_Master.csv', index=False)

    df = pd.read_sql_query('''SELECT *  FROM [SupplyChain].[opt].[Plot_Data_out_stage]''', con=engine)
    df.to_csv('Plot_Data_out_stage.csv', index=False)

    df = pd.read_sql_query('''SELECT *  FROM [SupplyChain].[opt].[Po_data_summary_out_stage]''', con=engine)
    df.to_csv('Po_data_summary_out_stage.csv', index=False)

    for vendorId in vendorList:
        df = pd.read_sql_query('SELECT *  FROM [SupplyChain].[opt].[PO_Exception_out_stage_' + vendorId + ']', con=engine)
        df.to_csv('PO_Exception_out_stage_' + vendorId + '.csv', index=False)

    df = pd.read_sql_query('''SELECT *  FROM [SupplyChain].[opt].[Po_Performance_Out_stage]''', con=engine)
    df.to_csv('Po_Performance_Out_stage.csv', index=False)

    df = pd.read_sql_query('''SELECT *  FROM [SupplyChain].[opt].[StockOut_Summary_out_stage]''', con=engine)
    df.to_csv('StockOut_Summary_out_stage.csv', index=False)

def temp():
    vendorId = "20013685"
    timeStr = "2023_11_18_18_37_"

    data = pd.read_csv(r'D:\Scripts\OPT\RepModel\Opt\item_master.csv')
    df = pd.DataFrame(data)
    df = df.replace({'': 0, ' ': 0, float('NaN'): 0})
    df.to_csv(r"D:/Scripts/OPT/RepModel/Opt/item_master.csv", index=False)

    clearTables(vendorId)
    clearAndWriteItemMaster()
    writePlotDataOut(vendorId, timeStr)
    writePODataOutSummaryMasterDB(vendorId, timeStr)
    # writePoPerformanceOut(vendorId, timeStr)
    writeStockOutData(vendorId, timeStr)

    createExcepTable(vendorId)
    writeExcepOutData(vendorId, timeStr)

    # data = pd.read_csv(r'D:\Scripts\OPT\RepModel\Opt\item_master.csv')
    # df = pd.DataFrame(data)
    # df = df.replace({'': 0, ' ': 0, float('NaN'): 0})
    #
    # df.to_csv(r"D:/Scripts/OPT/RepModel/Opt/item_master_m.csv", index=False)

if __name__ == '__main__':

    # temp()
    # sys.exit()

    # writeTables(vendorList)
    # sys.exit()


    # vendorList = readVendorList()
    # writeTables(vendorList)
    # sys.exit()

    # writePODataOutSummaryMasterDB('20006201', '2023_10_30_09_18_')
    # sys.exit()

    # writePODataOutSummaryMasterDBT()
    # sys.exit()

    now = datetime.now()
    timeStr = now.strftime("%Y") + '_' + now.strftime("%m") + '_' + now.strftime("%d") + '_' + now.strftime("%H_%M_")
    # timeStr = "2023_11_20_22_35_"

    vendorList = readVendorList()

    for vendorId in vendorList:
        createDir(vendorId, timeStr)
        prepareItemDC(vendorId)
        prepareForecast(vendorId)
        preparePO(vendorId)
        JavaScipRun(vendorId, timeStr)

        clearTables(vendorId)
        clearAndWriteItemMaster()
        writePlotDataOut(vendorId, timeStr)
        writePODataOutSummaryMasterDB(vendorId, timeStr)
        # writePoPerformanceOut(vendorId, timeStr)
        writeStockOutData(vendorId,timeStr)

        createExcepTable(vendorId)
        writeExcepOutData(vendorId,timeStr)

    writeTables(vendorList)
