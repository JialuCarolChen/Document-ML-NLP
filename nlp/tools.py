import os
import json
import csv
import pandas as pd
from pymongo import MongoClient
from gridfs import GridFS
import sys

##############################################
#   import data into Mongo DB collections    #
##############################################

def import_csv(filepath, collection_name, db):
    #mng_db = mng_client[db]  # Replace mongo db

    db_cm = db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(filepath)
    # data = pd.read_csv(filepath, delimiter=',', dtype=
    #                    {"Keywords": str,
    #                     "Paper Fares": bool,
    #                     "Filed Fares": bool,
    #                     "Commission": bool,
    #                     "Country": str,
    #                     "Reference Filename": str
    #                    }, low_memory=False)
    data2 = data.fillna(False)
    data_json = json.loads(data2.to_json(orient='records'))
    db_cm.drop()
    db_cm.insert_many(data_json)
    print(filepath, ' imported to', collection_name)

##############################################
#   export data from Mongo DB collections    #
##############################################
# can export a document collection as a CSV file.
# It is meant for collections that are already structured as a table eg export from NLP work to SQL
# To use:
# in terminal command line:  python exportCSV.py "test"  "raxb" "/Users/gt/rassure/Working/CX/"
# This will export the collection "test" to "test.csv" in the specified directory
# collection=str(sys.argv[1])
# mongodb= str(sys.argv[2])
# path=str(sys.argv[3])

# client=MongoClient('mongodb://raxPaper:vN9k32dXma@cluster0-shard-00-00-yvbwp.mongodb.net:27017,cluster0-shard-00-01-yvbwp.mongodb.net:27017,cluster0-shard-00-02-yvbwp.mongodb.net:27017/admin?replicaSet=Cluster0-shard-0&ssl=true')
# export_path=path+collection+'.csv'
# db=client[mongodb]
# commission_data=db[collection]
# try:
# 	df = pd.DataFrame(list(commission_data.find()))
# 	df.to_csv(export_path, header=True,index=True)
# except:
# 	print(collection,'does not exist')

# print ("This is the name of the script: ", sys.argv[0])
# print ("Number of arguments: ", len(sys.argv))
# print ("The arguments are: " , str(sys.argv))
# print('Collection for export',str(sys.argv[1]))


##############################################
#        get HTML string from Mongo DB       #
##############################################
class TransferMongo(object):
    def __init__(self):
        """
        A class to export html files from mongodb
        """
        # self.client = MongoClient(host, port)
        self.client = MongoClient('mongodb://raxPaper:vN9k32dXma@cluster0-shard-00-00-yvbwp.mongodb.net:27017,cluster0-shard-00-01-yvbwp.mongodb.net:27017,cluster0-shard-00-02-yvbwp.mongodb.net:27017/admin?replicaSet=Cluster0-shard-0&ssl=true')

        # self.client = MongoClient('localhost:27017')

    def fetch_GridFS(self, db,fn,cntry):
        """
        GridFS collection for large file
        :param db: database name
        :param fn: file name
        :param cntry: country name
        :return doc: html content in json str format
        """
        fs = GridFS(db)
        if fs.find({"file_name":fn,"cntry":cntry}).count() > 0:
            for fs_doc in fs.find({"file_name":fn,"cntry":cntry}):
                doc = fs_doc.read()
                doc = doc.decode("utf-8")
        else:
            doc = None
        return doc

    def get_html(self, dbname, colname, file_name, cntry, filepath = None):
        """
        A function to export html files from mongo database collection
        :param dbname: database name
        :param colname: collection name
        :param file_name: file name to be searched
        :param cntry: country name to be searched
        :param filepath(optional): file path
        :return htmls: a list of inserted html string
        """
        db = self.client[dbname]
        collection = db[colname]
        collection_child = db[colname + "_child"]
        htmls = []
        for doc in collection.find({"file_name":file_name,"cntry":cntry}):
            # extract html string from mongodb
            file_name = doc['file_name']
            # for single file:
            if doc['html_type'] == "single":
                html = doc['html_content']
                htmls.append(html)
            # for multi files:
            if doc['html_type'] == "multi":
                for sheet in collection_child.find({"file_name": file_name,"cntry":cntry}):
                    html = sheet["html_content"]
                    htmls.append(html)
        large_html = self.fetch_GridFS(db,file_name,cntry)
        htmls.append(large_html)
        # if large_html:
        #     print("Is a GridFS file!")
        # if GridFS return None, remove None from the list
        if None in htmls:
            htmls.remove(None)
        return htmls

def fetch_htmls(client, db_name, collection_name, file_name, cntry):
    """a function to fetch html strings using Transfer Mongo

    """
    tm = TransferMongo()
    tm.client = client
    docs = tm.get_html(db_name, collection_name, file_name, cntry)
    return docs
    # end = datetime.now()
    # duration = (end-now).total_seconds()
    # print(duration)

##############################################
#        normalise_tables                    #
##############################################

class Element(object):
    def __init__(self, row, col, text, rowspan=1, colspan=1):
        self.row = row
        self.col = col
        self.text = text
        self.rowspan = rowspan
        self.colspan = colspan

    def __repr__(self):
        return f'''{{"row": {self.row}, "col":  {self.col}, "text": {self.text}, "rowspan": {self.rowspan}, "colspan": {self.colspan}}}'''

    def isRowspan(self):
        return self.rowspan > 1

    def isColspan(self):
        return self.colspan > 1


def parse(h) -> [[]]:
    # doc = BeautifulSoup(h, 'html.parser')
    doc = h

    trs = doc.select('tr')

    m = []

    for row, tr in enumerate(trs):  # collect Node, rowspan node, colspan node
        it = []
        ts = tr.find_all(['th', 'td'])
        for col, tx in enumerate(ts):
            element = Element(row, col, tx.text.strip())
            if tx.has_attr('rowspan'):
                element.rowspan = int(tx['rowspan'])
            if tx.has_attr('colspan'):
                element.colspan = int(tx['colspan'])
            it.append(element)
        m.append(it)

    def solveColspan(ele):
        row, col, text, rowspan, colspan = ele.row, ele.col, ele.text, ele.rowspan, ele.colspan
        m[row].insert(col + 1, Element(row, col, text, rowspan, colspan - 1))
        for column in range(col + 1, len(m[row])):
            m[row][column].col += 1

    def solveRowspan(ele):
        row, col, text, rowspan, colspan = ele.row, ele.col, ele.text, ele.rowspan, ele.colspan
        offset = row + 1
        m[offset].insert(col, Element(offset, col, text, rowspan - 1, 1))
        for column in range(col + 1, len(m[offset])):
            m[offset][column].col += 1

    for row in m:
        for ele in row:
            if ele.isColspan():
                solveColspan(ele)
            if ele.isRowspan():
                solveRowspan(ele)
    return m

def prettyPrint(m):
    for i in m:
        it = [f'{len(i)}']
        for index, j in enumerate(i):
            # if j.text != '':
            it.append(f'{index:2} {j.text[:4]:4}')
        print(' --- '.join(it))

##############################################
#        work with google speadsheets        #
##############################################
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# use creds to create a client to interact with the Google Drive API
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
# creds = ServiceAccountCredentials.from_json_keyfile_name('./raxutil/nlp/client_secret.json', scope)
# client = gspread.authorize(creds)

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
# sheet = client.open("00 Fares Keywords List").sheet1

# Extract and print all of the values
# list_of_hashes = sheet.get_all_records()
# print(list_of_hashes[0])
# print(list_of_hashes)

##############################################
#  reading, writing and exporting files      #
##############################################

def write_dict_file(filename,my_dict):
    with open(filename, 'w') as f:
        w = csv.DictWriter(f, my_dict.keys())
        w.writeheader()
        w.writerow(my_dict)


