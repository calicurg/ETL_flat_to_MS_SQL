import pyodbc
import pickle as PI
import LightLinter as LL
from tkinter import filedialog as TF

TK   = LL.TK
TKDI = LL.TKDI

ConnLI = ['DRIVER',
          'SERVER',
          'DATABASE',
          'UID',
          'PWD'
          ]

                 
ConnDI = {'DRIVER':'{SQL Server}',
          'SERVER':"WIN-DPISK0DDVO5\SQLEXPRESS",
          'DATABASE':'accs00',
          'UID':'ilya',
          'PWD':'0000'
          }


AtomTypesLI = ['INT IDENTITY PRIMARY KEY',
               'VARCHAR (50)',
               'INT',
               'MONEY',
               'TEXT',
               'CHAR',
               'DATETIME',
               'INT IDENTITY'                 
               ]


Accs = {0:[],
        1:[],
        2:[]
        }
    
    
CurrDI = {'con':'',
          'cursor':'',
          'si':'',
          'table':'receipt_heders',
          'db':'accs00',
          'schema':'dbo',
 #         'lr':'' ## linear (string) row (comma-separated)
           'lr':'00БП-000005	28.02.2019 13:09:29	499 317	БП-000014	ЭНЕРДЖИ 3Д ООО',      
           'sep': '\t', ## input file separator 
           'insert_head':'',
           'create':''
           }


StructureDI = {'receipt_heders':
                    {'attrs':[
                             'id',
                             'code1c',
                             'date',                             
                             'summa',
                             'ref_contr',
                             'name_contr'
                             ],
                    'state':
                            {
                            'id'         :  'int IDENTITY PRIMARY KEY',
                            'code1c'     :  'varchar (11)  NOT NULL',
                            'date'       :  'datetime',
                            'ref_contr'  :  'varchar (10)  NOT NULL',
                            'summa'      :  'MONEY',
                            'name_contr' :  'varchar (150)'	
                             },
                    'types':['char',
                            'datetime',                            
                            'int',
                            'char',
                            'char'
                            ]
                    
                    },
                    'income_goods':
                    {'attrs':[
                             'id',
                             'code1c',
                             'date',
                             'ref_contr',
                             'summa',
                             'name_contr'
                             ],
                    'state':
                            {
                            'id'         :  'int IDENTITY PRIMARY KEY',
                            'code1c'     :  'varchar (11)  NOT NULL',
                            'date'       :  'datetime',
                            'ref_contr'  :  'varchar (10)  NOT NULL',
                            'summa'      :  'MONEY',
                            'name_contr' :  'varchar (150)'	
                             },
                     
                     'types':[]
              
                    }
             }

##############################################################
#TransDI = {'char':prepare_char,
#            'int':prepare_int,
#            'datetime':prepare_date_from_1c,
#            'float':prepare_float
#            }

def ShowSource():

    #Accs[0] = rl
    
    
    LL.TKDI['tx']['attrs'].delete('1.0', TK.END)
    
    for ls in Accs[0]:
        LL.TKDI['tx']['attrs'].insert(TK.END, ls)
    

def detect_atom_type(sql_type):
    
    sql_type = sql_type.upper()
    atom_type = 0
    
    if 'VARCHAR' in sql_type or sql_type == 'TEXT':
        atom_type = 'char'
        
    elif 'INT' in sql_type:
        atom_type = 'int'
        
    elif sql_type == 'DATETIME':
        atom_type = 'datetime'
        
    elif sql_type == 'MONEY':
        atom_type = 'float'
    
#    else:
#        atom_type = 'char'
    
    return atom_type
    
      
  

def SetColumnTypes():
    
    cur_table = CurrDI['table']
    attrs_li = StructureDI[cur_table]['attrs']
    
    state_di = StructureDI[cur_table]['state'] 
    
    TypesLI = []
    
    for attr in attrs_li:
        sql_type  = state_di[attr]
        atom_type = detect_atom_type(sql_type)
        TypesLI.append(atom_type)
        
    StructureDI[cur_table]['types'] = TypesLI
    
    print('SetColumnTypes : done')
    

def Suggest__table__structure():

    if len(Accs[0]) == 0:
        print('please, read the source file')
        return 0
    
    new_table = LL.TKDI['en']['tables'].get()
    if new_table not in StructureDI:
        LL.TKDI['lx']['tables'].insert(0, new_table)    
#        print('table with rhis name already exists!')
#        return 0
        
    headers_line = Accs[0][0]
    headers_line = headers_line.strip()
    sep = CurrDI['sep']
    sl = headers_line.split(sep)
    
    StructureDI[new_table] = {'attrs':sl,
                              'state':{},
                              'types':[]
                              }
    sl.insert(0, 'id')
    di = {'id':'INT IDENTITY PRIMARY KEY'}
    for attrinx in range(1, len(sl)):
        attr = sl[attrinx]
        di[attr] = 'VARCHAR (50)'
    
    StructureDI[new_table]['state'] = di    
    
    CurrDI['table'] = new_table
    SetColumnTypes()
    
    #LL.TKDI['en']['tables'].put(new_table)        
    
    
    refresh__attrs()
    
    return 1

def open_file():
    
    fn = TF.askopenfilename()
    LL.TKDI['en']['source'].put(fn)


def CopyStructure():

    curr_table = LL.TKDI['en']['tables'].get()
    v = StructureDI[curr_table]
    new_v = v.copy()
    
    new_table = LL.TKDI['en']['attrs'].get()
    StructureDI[new_table] = new_v
    LL.TKDI['lx']['tables'].insert(TK.END, new_table)
    LL.TKDI['en']['tables'].put(new_table)
    
    refresh__attrs()
    
    

def LoadStructureDI():

    fi = open('Structure.li', 'rb')
    lo = PI.load(fi)
    fi.close()
    
    for k, v in lo.items():
        StructureDI[k] = v
    
    array = list(StructureDI.keys())
    array.sort()
    LL.Fill__lx(array, 'tables')
    
    LL.TKDI['en']['tables'].put(array[0])
    
    refresh__attrs()
    
    print('LoadStructureDI : done')
    

def DumpStructureDI():

    fi = open('Structure.li', 'wb')
    PI.dump(StructureDI, fi)
    fi.close()
    print('DumpStructureDI : done')
    

def prepare_char():
    
    single     = CurrDI['si']
    
    outchar    = "'"+single+"'"
    return outchar
    
    
def prepare_int():
    
    single     = CurrDI['si']
    if ' ' in single:
        single = single.replace(' ', '')
    
    return single
    
    
def prepare_date_from_1c():

    single     = CurrDI['si']
    dt = Convert_dateime(single)
    
    return dt
    
    
def prepare_float():

    single   = CurrDI['si']  
    if ' ' in single:
        single = single.replace(' ', '')
        
    if ',' in single:
     
        single = single.replace(',', '.')
        
    return single


TransDI = {'char':prepare_char,
            'int':prepare_int,
            'datetime':prepare_date_from_1c,
            'float':prepare_float
            }

def Prepare__Insert__head():

    head_line = ''
    
    TableParamAttrsLI = ['db', 'schema', 'table']
    TableParamsLI     = []
    
    for attr in TableParamAttrsLI:
        value = CurrDI[attr]
        TableParamsLI.append(value)
 
##   'INSERT INTO 
##        [accs00].dbo.goods
 
    line = 'INSERT INTO ' \
           + '.'.join(TableParamsLI)
           
    curr_table = CurrDI['table']    
    TaAttrsLI = StructureDI[curr_table]['attrs']
    TaAttrsLI = TaAttrsLI[1:]
    
    ColsLI = []
    for attr in TaAttrsLI:
        attr = '['+attr+']'
        ColsLI.append(attr)
        
    columns_line = ' ('+ ', '.join(ColsLI) + ') '
    line += columns_line
    CurrDI['insert_head'] = line
    
    return line


def PrintoutColumnTypes():

    table_name = CurrDI['table']
    ColumnTypesLI = StructureDI[table_name]['types']

    print('ColumnTypesLI :')    
    for ctinx in range(len(ColumnTypesLI)):
        ty = str(ctinx) +' : '+ColumnTypesLI[ctinx]
        print(ty)


def PrepareValline():

    ValsLI = []

    lr  = CurrDI['lr']
    #sep = CurrDI['sep']
    sep = '\t'
    sl  = lr.split(sep)
    
    ## cinx = column index
    table_name = CurrDI['table']
    ColumnTypesLI = StructureDI[table_name]['types']

#    print('ColumnTypesLI :')    
#    for ctinx in range(len(ColumnTypesLI)):
#        ty = str(ctinx) +' : '+ColumnTypesLI[ctinx]
#        print(ty)
    
    for cinx in range(len(sl)):
        single = sl[cinx]
        CurrDI['si'] = single
#        fr = str(cinx) +'__'+single
#        print(fr)
        
        column_type = ColumnTypesLI[cinx+1]
       ##TransDI = {'char':prepare_char,
        value = TransDI[column_type]()## = {'char':prepare_char,       
        ValsLI.append(value)
    
    valline = ', '.join(ValsLI)
    valline = ' VALUES (' + valline +');'
    
    statement = CurrDI['insert_head'] + valline
    Accs[1].append(statement)


def PrepareShowINSERT():

    Accs[1] = []
    
    Prepare__Insert__head()
    
    LL.TKDI['tx']['data'].delete('1.0', TK.END)
    

    for rinx in range(1, len(Accs[0])):
        #print('rinx :'+str(rinx ))
    
        line = Accs[0][rinx]
        line = line.strip()
        if '"' in line:
            line = line.replace('"', '')
        
        if "'" in line:
            line = line.replace("'", "")

        CurrDI['lr'] = line    
        PrepareValline()
        ls = Accs[1][-1]
        ls += '\n\n'
        LL.TKDI['tx']['data'].insert(TK.END, ls)
    
    
def PrepareINSERT():
    
    Accs[1] = []
    
    Prepare__Insert__head()
    
    for line in Accs[0]:
        line = line.strip()
        if '"' in line:
            line = line.replace('"', '')
        
        if "'" in line:
            line = line.replace("'", "")

        CurrDI['lr'] = line    
        PrepareValline()
        ls = Accs[1][-1]
        print(ls)  
        
    
def ExecuteINSERT():
    
    cnxn   = CurrDI['con']
    cursor = CurrDI['cursor']
    
   # PrepareINSERT()
    if len(Accs[1]) == 0:
       print('Prepare INSERT statements!')
       return 0
    
    for y in range(len(Accs[1])):
        statement = Accs[1][y] 
        try:        
            cursor.execute(statement)
            cnxn.commit()
            print(y)
        except:
            el = 'not done : '+str(y)
            print(el)
            
    return 1

def INSERT():
    
    cnxn   = CurrDI['con']
    cursor = CurrDI['cursor']
    
    PrepareINSERT()
    
    for y in range(len(Accs[1])):
        statement = Accs[1][y] 
        try:        
            cursor.execute(statement)
            cnxn.commit()
            print(y)
        except:
            el = 'not done : '+str(y)
            print(el)
    
    
def Prepare__create():

    table_name = LL.TKDI['en']['tables'].get()
    
    DI = StructureDI[table_name]
    AttrsLI  = DI['attrs']
    StateDI  = DI['state']

    head = 'CREATE TABLE [accs00].dbo.' + table_name +'\n'
    #head += '[id] INT IDENTITY PRIMARY KEY\n'
    SL = []    
    for attr in AttrsLI:
        col_type = StateDI[attr]
        st_line  = '['+attr +'] '+col_type
        SL.append(st_line)
    
    valline = ' (' \
              +',\n'.join(SL) \
              + ')'
    
    
    statement = head + valline
    CurrDI['create'] = statement
    
    LL.TKDI['tx']['data'].delete('1.0', TK.END)
    LL.TKDI['tx']['data'].insert('1.0', statement)
 
    
    print(statement) 
    
    ##return statement

def PrepareCreateStatement(table_name):

    DI = StructureDI['receipt_heders']
    AttrsLI  = DI['attrs']
    StateDI  = DI['state']

    head = 'CREATE TABLE [accs00].dbo.' + table_name +'\n'
    SL = []    
    for attr in AttrsLI:
        col_type = StateDI[attr]
        st_line  = '['+attr +'] '+col_type
        SL.append(st_line)
    
    valline = ' (' \
              +',\n'.join(SL) \
              + ')'
    
    
    statement = head + valline
    print('PrepareCreateStatement : done') 
    
    return statement
    
    
def CREATE_TABLE(table_name): 

    if table_name in StructureDI:
        statement = PrepareCreateStatement(table_name)
        print(statement)
        Execute(statement)
    else:
        print('table description was mnot found!')
        

def Execute__statement():

    statement = LL.TKDI['tx']['data'].get('1.0', TK.END)
    cursor = CurrDI['cursor']
    cnxn   = CurrDI['con']
    #try:
    cursor.execute(statement)
    cnxn.commit()
    print('Execute__statement : done')    


            

def Execute(statement):

    cursor = CurrDI['cursor']
    cnxn   = CurrDI['con']
    #try:
    cursor.execute(statement)
    cnxn.commit()
    print('Execute : done')
    

def Create_receipt_heders():

    statement = '''CREATE TABLE [accs00].dbo.receipt_heders
        (
        [id] 	int IDENTITY PRIMARY KEY,
        [code1c] varchar (11)  NOT NULL,
        [date] datetime,
        ref_contr varchar (10)  NOT NULL,
        summa MONEY,
        name_contr varchar (150)	
        );'''
    cursor = CurrDI['cursor']
    cnxn   = CurrDI['con']
    #try:
    cursor.execute(statement)
    cnxn.commit()
    #print('CreateTable: done')
    #except:
    #    print('not done')
    
    
def Get_conn_line():

    conn_line = ''
    LI = []
    for attr in ConnLI:
        value = ConnDI[attr]
        fr = attr+'='+value
        LI.append(fr)

    conn_line = ';'.join(LI)
    return conn_line


def Add__flankers(valline):

    valline  = " ('" + valline + "');"
    return valline

  
##INSERT INTO [accs00].[dbo].[payment_orders]
##(ref_contr, [code1c], [doc_date], [time])
##VALUES
##(3, '00БП-000001', '2019-11-03T11:24:05', '11:24:05');  

def Convert_dateime(date_time):
    
 #   print(date_time)
    sqldt = ''
    ##13.03.2019 14:46:47
    sl = date_time.split()
    if len(sl) < 2:
        print(sl)
        return 'null'
        
    lindate = sl[0]
    lintime = sl[1]
    
    ss = lindate.split('.')
    sqlli = [ ss[2],
              ss[1],
              ss[0]
            ]
            
    sql_date = '-'.join(sqlli)
    sqldt = "'"+sql_date+'T'+lintime+"'"
    
    return sqldt
    
    
def Prepare__receipt_heders():

    Accs[1] = []


    statement = '''INSERT INTO 
            [accs00].dbo.goods
    ( 
    [code1c], 
    [сname]
    )
    VALUES '''

#'БП-00000471'	'Bluetooth-адаптер ASUS USB BT 400 usb'
#*/
    #statement = ''.join(StateLI)		
    for y in range(len(Accs[0])):
        ls = Accs[0][y]
        ls = ls.strip()
        ls = ls.replace('"', '')
        sl = ls.split('\t')
        if len(sl) < 2:
            continue
    
        #date_time = Convert_dateime(sl[1])
        
        valline = sl[0] \
                + "', '" \
                + sl[1]  
         
           
        fl_vll = Add__flankers(valline)
        query = statement + fl_vll

        if "'<NULL>'" in query:
            query = query.replace("'<NULL>'", "NULL")
            
        Accs[1].append(query)


 
def Prepare__goods():

    Accs[1] = []


    statement = '''INSERT INTO 
            [accs00].dbo.goods
    ( 
    [code1c], 
    [сname]
    )
    VALUES '''

#'БП-00000471'	'Bluetooth-адаптер ASUS USB BT 400 usb'
#*/
    #statement = ''.join(StateLI)		
    for y in range(len(Accs[0])):
        ls = Accs[0][y]
        ls = ls.strip()
        ls = ls.replace('"', '')
        sl = ls.split('\t')
        if len(sl) < 2:
            continue
    
        #date_time = Convert_dateime(sl[1])
        
        valline = sl[0] \
                + "', '" \
                + sl[1]  
         
           
        fl_vll = Add__flankers(valline)
        query = statement + fl_vll

        if "'<NULL>'" in query:
            query = query.replace("'<NULL>'", "NULL")
            
        Accs[1].append(query)



def Prepare__paym_orders():

    Accs[1] = []


    statement = '''INSERT INTO \
            [accs00].dbo.payment_orders\
    ( 
    [code1c], 
    [doc_date],
    [ref_contr],
    [summa],
    [purpose]
    )
    VALUES '''

#('0БП-000001',
#'2019-11-03T11:24:05',
#'БП-000011',
#1700,
#'Оплата по счету №5 от 13.02.2019'
#);
#*/
    #statement = ''.join(StateLI)		
    for y in range(len(Accs[0])):
        ls = Accs[0][y]
        ls = ls.strip()
        ls = ls.replace('"', '')
        sl = ls.split('\t')
        if len(sl) < 5:
            continue
    
        date_time = Convert_dateime(sl[1])
        
        valline = sl[0] \
                + "', '" \
                + date_time \
                + "', '" \
                + sl[2]  \
                + "', " \
                + sl[3].replace(' ', '')\
                + ", '" \
                + sl[4]
         
           
        fl_vll = Add__flankers(valline)
        query = statement + fl_vll

        if "'<NULL>'" in query:
            query = query.replace("'<NULL>'", "NULL")
            
        Accs[1].append(query)

 

def Prepare__queries():

    Accs[1] = []

    StateLI = ["INSERT INTO  [accs00].[dbo].[contrs] ",
#    StateLI = ["INSERT INTO  [accs00].[dbo].[contagents] ",
		"(code1c, cname, inn, full_name) ",
		" VALUES "]

    statement = ''.join(StateLI)		
    for y in range(len(Accs[0])):
        ls = Accs[0][y]
        ls = ls.strip()
        ls = ls.replace('"', '')
        sl = ls.split('\t')
        if len(sl) < 4:
            continue
    
        ### all varchars
        valline = "', '".join(sl)
        fl_vll = Add__flankers(valline)
        query = statement + fl_vll

        if "'<NULL>'" in query:
            query = query.replace("'<NULL>'", "NULL")
            
        Accs[1].append(query)


def read_source():
    
    fna = LL.TKDI['en']['source'].get()
    
    Accs[0] = []
       
    fi = open(fna, 'r')
    rl = fi.readlines()
    fi.close()
    Accs[0] = rl
    print('read__source: done')
    
    LL.TKDI['tx']['attrs'].delete('1.0', TK.END)
    
    for ls in rl:
        LL.TKDI['tx']['attrs'].insert(TK.END, ls)


def Read__source():

    Accs[0] = []
    
   # fna = 'D:\\___________ETL\\AccsEL\\Contagents.txt'
   # fna = 'D:\\___________ETL\\AllContrs.txt'
   # fna = 'D:\\___________ETL\\PaymantOrders.txt'
    fna = 'D:\\___________ETL\\all_goods.txt'
    fna = 'D:\\___________ETL\\receipt_goods_headers.txt'
   
    fi = open(fna, 'r')
    rl = fi.readlines()
    fi.close()
    Accs[0] = rl
    print('Read__source: done')
    
    
def InsertValues():

    
    ValLI = ["INSERT INTO  [accs00].[dbo].[contrs] ",
		"(cname, inn, full_name) ",
		" VALUES ",	
                "('",
                "Красное знамя",
                     "', '",
                     "4821001604",
                     "', '",
                     "Финансовый комитет(Редакция газеты Красное знамя л/с 30607000003');"
             ]
    sql_query = ''.join(ValLI)
    print(sql_query) 
    conn_line = Get_conn_line()    
    cnxn = pyodbc.connect(conn_line) 

    cursor = cnxn.cursor()
    cursor.execute(sql_query)
    cnxn.commit()

    print("InsertValues: done")

    
def ConnectMSSQL():
    
    conn_line = Get_conn_line()
    #cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.1.1;DATABASE=BaseName;UID=User;PWD=Password')
    cnxn = pyodbc.connect(conn_line) #'DRIVER={SQL Server};SERVER=192.168.1.1;DATABASE=BaseName;UID=User;PWD=Password')

    cursor = cnxn.cursor()

    sql_query = "SELECT TOP (20)\
		 [CustomerID]  \
		,[FirstName]   \
		,[LastName]     \
		 FROM [SalesDB].[dbo].[Customers]\
		 ORDER BY LastName;";

    cursor.execute(sql_query) ##"select id, name from test")
    rows = cursor.fetchall()
    for row in rows:
        print(row.CustomerID)
        print(row.FirstName)
        print(row.LastName)
        print('\n==================\n')



def Select__agents():
    
    conn_line = Get_conn_line()
    #cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.1.1;DATABASE=BaseName;UID=User;PWD=Password')
    cnxn = pyodbc.connect(conn_line) #'DRIVER={SQL Server};SERVER=192.168.1.1;DATABASE=BaseName;UID=User;PWD=Password')

    cursor = cnxn.cursor()

    sql_query = "SELECT TOP (30)\
                [id] \
                ,[cname] \
                ,[inn]   \
                 ,[full_name]\
                  FROM [accs00].[dbo].[contagents];"

##    "SELECT TOP (20)\
##		 [CustomerID]  \
##		,[FirstName]   \
##		,[LastName]     \
##		 FROM [SalesDB].[dbo].[Customers]\
##		 ORDER BY LastName;";

    cursor.execute(sql_query) ##"select id, name from test")
    rows = cursor.fetchall()
    for row in rows:
        print(str(row.id))
        print(row.inn)
        print(row.cname)
        print('\n==================\n')
        

def Connect_mssql():

    conn_line = Get_conn_line()
    cnxn = pyodbc.connect(conn_line) 
    cursor = cnxn.cursor()  
    CurrDI['con'] = cnxn
    CurrDI['cursor'] = cursor    
    
    print('Connect_mssql: done')


def Insert__all():

    conn_line = Get_conn_line()
    cnxn = pyodbc.connect(conn_line) 
    cursor = cnxn.cursor()    

    for qinx in range(len(Accs[1])):
        sql_query = Accs[1][qinx]
        try:
            cursor.execute(sql_query)
            cnxn.commit()
            print(qinx)
        except:
            print('not done')
 #       print(sql_query)
        #print('=============\n')
        

def Insert__data():

    Read__source()
   # Prepare__queries()
   # Prepare__paym_orders()
   # Prepare__goods()
    Prepare__receipt_heders()
    Insert__all()


def reflect__attr(event):

    pair  = LL.reflect__lx__in__entry('attrs')
    table = LL.TKDI['en']['tables'].get()
    attr  = LL.TKDI['en']['attrs'].get()
    col_type = StructureDI[table]['state'][attr]
    
    LL.TKDI['en']['col_type'].put(col_type)
      

def reflect__table(event):

    pair = LL.reflect__lx__in__entry('tables')
    CurrDI['table'] = pair[1]
    refresh__attrs()    

    
def StartingFills():

    array = list(StructureDI.keys())
    array.sort()
    LL.Fill__lx(array, 'tables')

    LL.Fill__lx(AtomTypesLI,'col_type')
    
    
def Create__forms():

    LL.Create__root('===== MS SQL data explorer =====')
    LL.Add__one__frame('zero', 'root', 0, 1)
    LL.Add__one__frame('0.5',  'root', 1, 1)
    LL.Add__one__frame('one',  'root', 2, 1)
    LL.Add__one__frame('two', 'root', 3, 1)

    LL.Add__one__frame(0, 'zero', 0, 1)
    
    LL.Add__lx('tables', 0, 1, 1, 17, 7, 'Arial 14')
    LL.Add__lx('attrs',  0, 1, 2, 17, 7, 'Courier 14')
    LL.Add__tx('attrs',  0, 3, 4, 65, 7, 'white', 'blue', 'Courier 14')

    LL.TKDI['lx']['tables'].bind('<KeyRelease>',    reflect__table)
    LL.TKDI['lx']['tables'].bind('<ButtonRelease>', reflect__table)

    LL.TKDI['lx']['attrs'].bind('<KeyRelease>',    reflect__attr)
    LL.TKDI['lx']['attrs'].bind('<ButtonRelease>', reflect__attr)

###########################  frame 0.5 ######################

    LL.Add__entry('source',     '0.5', 1, 1, 85, 'Arial 12')
   ## fn = "D:\\___________ETL\\All_rec_goods_details.txt"
    fn = "All_rec_goods_details.txt"
    LL.TKDI['en']['source'].put(fn)
    
    LL.Add__button('open_file', '0.5', 1, 4, 20, 'open file')
    LL.TKDI['bu']['open_file']['command'] = open_file

    LL.Add__button('read_source', '0.5', 1, 5, 20, 'read source')
    LL.TKDI['bu']['read_source']['command'] = read_source
    
###########################  frame 1 ######################

    LL.Add__one__frame(1, 'one', 0, 1)
##        'id'         :  'int IDENTITY PRIMARY KEY',
##        'code1c'     :  'varchar (11)  NOT NULL',
##        'date'       :  'datetime',
##        'ref_contr'  :  'varchar (10)  NOT NULL',
##        'summa'      :  'MONEY',
##        'name_contr' :  'varchar (150)'	

    ##            column types
    LL.Add__one__frame('cty', 1, 0, 1)

    ##             actions
    LL.Add__one__frame('act', 1, 0, 2)
    
    LL.Add__lx('col_type', 'cty', 1, 1, 30, 10, 'Arial 12')
    LL.Add__button('add_column', 'act', 1, 1, 15, 'add_column')
    LL.TKDI['bu']['add_column']['command'] = Add_column

    LL.Add__button('accept_column', 'act', 2, 1, 15, 'accept_column')
    LL.TKDI['bu']['accept_column']['command'] = Accept_column

    LL.Add__one__frame('data', 1, 0, 3)
    LL.Add__tx('data', 'data', 1, 1, 70, 10, 'white', 'black', 'Courier 14')


def Create__menu():

    LL.Create__menu()

##    TKDI['me'][0] = TK.Menu(TKDI['fr']['root'])
##    TKDI['me'][1] = TK.Menu(TKDI['me'][0])
##    TKDI['me'][1].add_command(label = 'Print_blocks', command = Print_blocks)
    TKDI['me'][1].add_command(label = 'Dump structure', command = DumpStructureDI)
    TKDI['me'][1].add_command(label = 'Load structure', command = LoadStructureDI)
    TKDI['me'][1].add_command(label = 'CopyStructure',  command = CopyStructure)



    TKDI['me'][2] = TK.Menu(TKDI['me'][0])
    TKDI['me'][2].add_command(label = 'Prepare__create',   command = Prepare__create)
    TKDI['me'][2].add_command(label = 'PrepareShowINSERT', command = PrepareShowINSERT)
    TKDI['me'][2].add_command(label = 'Suggest__table__structure', command = Suggest__table__structure)
    TKDI['me'][2].add_command(label = 'SetColumnTypes', command = SetColumnTypes)
    TKDI['me'][2].add_command(label = 'PrintoutColumnTypes', command = PrintoutColumnTypes)
    TKDI['me'][2].add_command(label = 'ShowSource', command = ShowSource)




    TKDI['me'][3] = TK.Menu(TKDI['me'][0])
    TKDI['me'][3].add_command(label = 'Execute', command = Execute__statement)
    TKDI['me'][3].add_command(label = 'ExecuteINSERT', command = ExecuteINSERT)



    TKDI['me'][0].add_cascade(label = 'Prepare', menu = TKDI['me'][2])
    TKDI['me'][0].add_cascade(label = 'Execute', menu = TKDI['me'][3])
##   TKDI['fr']['root'].config(menu = TKDI['me'][0])
    


def refresh__attrs():

    table = LL.TKDI['en']['tables'].get()
    AttrsLI = StructureDI[table]['attrs']

    LL.Fill__lx(AttrsLI, 'attrs')
    

    StateDI = StructureDI[table]['state']

    OutLI = []
    for attr in AttrsLI:
        column_type = StateDI[attr]
        outline = attr +'\t: '+column_type
        OutLI.append(outline)

    outline = '\n'.join(OutLI)
    LL.TKDI['tx']['attrs'].delete('1.0', TK.END)
    LL.TKDI['tx']['attrs'].insert('1.0', outline)
    

def Accept_column():

    table = LL.TKDI['en']['tables'].get()
    attr  = LL.TKDI['en']['attrs'].get()
    col_type = LL.TKDI['en']['col_type'].get()

    if col_type == '':
        print('type column name! -> not done')
        return 0

    if table not in StructureDI:
        StructureDI[table] = {'attrs':[],
                              'state':{},
                              'types':[]
                              }    
        
    #StructureDI[table]['attrs'].append(attr)
    StructureDI[table]['state'][attr] = col_type

    SetColumnTypes()
    
    refresh__attrs()
    #StructureDI[table]['types'].append(attr)
    
    return 1

    
def Add_column():

    table = LL.TKDI['en']['tables'].get()
    attr  = LL.TKDI['en']['attrs'].get()
    col_type = LL.TKDI['en']['col_type'].get()

    if col_type == '':
        print('type column name! -> not done')
        return 0

    if table not in StructureDI:
        StructureDI[table] = {'attrs':[],
                              'state':{},
                              'types':[]
                              }
        
    if attr in StructureDI[table]['attrs']:
        print('column exists! -> not done')
        return 0

        
    StructureDI[table]['attrs'].append(attr)
    StructureDI[table]['state'][attr] = col_type

    refresh__attrs()
    #StructureDI[table]['types'].append(attr)
    
    return 1
    

    
def Update_column():

    table = LL.TKDI['en']['tables'].get()
    attr  = LL.TKDI['en']['attrs'].get()
    col_type = LL.TKDI['en']['col_type'].get()

    if col_type == '':
        print('type column name! -> not done')
        return 0

    if table not in StructureDI:
        StructureDI[table] = {'attrs':[],
                              'state':{},
                              'types':[]
                              }
        
    if attr in StructureDI[table]['attrs']:
        print('column exists! -> not done')
        return 0

        
    StructureDI[table]['attrs'].append(attr)
    StructureDI[table]['state'][attr] = col_type

    refresh__attrs()
    #StructureDI[table]['types'].append(attr)
    
    return 1
    

    
def Start():

   # ConnectMSSQL()
   #InsertValues()
 #   Insert__data()
#    Select__agents()
#    Read__source()
    Connect_mssql()
#    Create_receipt_heders()
    #CreateTable()
##    CREATE_TABLE('income_goods')
##    INSERT()
    Create__forms()
    StartingFills()
    Create__menu()

    LoadStructureDI()
    
    LL.TKDI['fr']['root'].mainloop()

Start()


