from pymongo import TEXT
from collections import namedtuple
from nlp.spellchecker import *
from nlp.tools import *
from nlp.cat16utilities import *
from nlp.chunker import *
from nlp.faresheet import *
from collections import Counter, defaultdict, deque


# Commission = namedtuple('Commission', ['value', 'unit'])
TaggedWord = namedtuple('TaggedWord', ['token', 'tag'])
TOURCODE_REGEX = r"[\d+|\w+]+FF\d+"
TC_fmt = re.compile(TOURCODE_REGEX)


client=MongoClient('localhost:27017')
db = client.raxdb

db=client.raxdb
filename="00 Fares Keywords List"
# train_dirname='/Users/gt/rassure/Working/CX/Faresheets'
faresheets='CXfaresheets_new'
tourcodes='CXtourcodes'
faresheet_tourcodes='CXfaresheet_tourcodes'
TOPWORDS=''
group='training'
NWORDS=defaultdict(lambda: 1)
#TOURCODE_REGEX=r"[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+|[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+"
TOURCODE_REGEX=r"[\d+|\w+]+FF\d+"
tourcodes_master=db['CXtourcodes_master']
tkt_tourcodes=db['CXfaresheet_tourcodes']

########################################################
#   Populate faresheet collection (creating fields)    #
########################################################

def update_one_fare(cnt, fs, fare, run_id):
    """
    A function to update one fare file
    :param cnt: the number of fare file updated so far
    :param fs: db[collection]
    :param fare: fare file
    :param run_id: run_id to keep track of the running batch
    :return:
    """
    # file to update:
    filename = fare['filename']
    country = fare['country']
    print('Updating:', cnt + 1, fare['_id'], filename, country)
    # get the html string
    ####################################
    farefile = FareFile(filename, country)
    # get clean text with colour tag
    farefile.s1s2 = farefile.get_s1s2()
    values = farefile.__dict__
    values['run_id'] = run_id
    #####################################
    del values['soup']  # can't store soup object
    del values['trs']
    del values['html_string']  # can bust Mongo limits, keep in the file
    # del values['_id']

    word_tokens_list, wordlist = create_wordlist(db, farefile.teststring)
    farefile.word_tokens_list = word_tokens_list  # include these in FareFile class
    farefile.wordlist = wordlist

    try:
        fs.update_one({'_id': fare['_id']}, {'$set': values})
    except  pymongo.errors.DocumentTooLarge:
        print('unable to update', fare["_id"], filename, country)
        with open("unable_to_update_list.txt", "w") as f:
            f.write(str(fare["_id"])+" "+filename+" "+country+" /n")


def update_CXfaresheets_new(db,collection, start_id=-1, run_id=1):
    """
    :param db: database
    :param collection: the CXfaresheets_new collection
    :param run_id: run_id to keep track of the running batch
    :param start_id: the _id of the document to start from, if start_id==-1, run from the beginning
    :return: the number of updated files
    """
    fs=db[collection]
    # start timing
    t1=datetime.now()

    DEBUG=False
    ################
    # u_cnt=0
    # u_list=[]
    cnt=0
    total = fs.find({}).count()
    # a variable to control when to start running the update
    start_update = False
    ###########

    # test_fn = '7CTU004FF706_files'
    # test_cn = 'CN'
    # test_doc=fs.find_one({'filename':test_fn, 'country':test_cn})
    # test_fare = []
    # test_fare.append(test_doc)
    # for fare in test_fare:
    for fare in fs.find({}).sort("_id", 1):

        if str(fare['_id']) == start_id or start_id == -1:
            start_update = True

        if start_update == True:
            update_one_fare(cnt, fs, fare, run_id)

        cnt+=1
        if (cnt % 100) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)

    # print(cnt, ' files imported')
    # print(u_cnt, ' missed')
    # print('MISSED:')
    # print(u_list)

    ##############
    t8=datetime.now()
    bench=(t8-t1).total_seconds()/60
    msg=f'TOTAL processing time: {bench:.1f} minutes'
    print(msg)
    return cnt

def create_text_index(db,faresheets):
    fs=db[faresheets]
    fs.create_index([('teststring', TEXT)], default_language='english' )

########################################################
#          Finding tourcodes                           #
########################################################

def find_all_tourcodes(db, faresheets, faresheet_tourcodes, select_dict={}):
    DEBUG = False
    t1 = datetime.now()

    if 'fat_run_id' in select_dict.keys():
        run_id = select_dict['fat_run_id']
        select_dict['fat_run_id'] = {'$ne': run_id}
    else:
        run_id = t1.strftime('%Y%m%H%M')

    run_id = int(run_id)
    fs = db[faresheets]
    ft = db[faresheet_tourcodes]

    total = fs.find(select_dict).count()
    print('Processing with fat_run_id', run_id, 'Total tourcodes to be processed:', total)
    cnt = 0
    TC_fmt1 = re.compile(TOURCODE_REGEX)
    # TC_fmt1 = re.compile(r"[\d|\w]\w+\d+'*,*\s*'*FF\d+")
    TC_fmt2a = re.compile(r'[\d|\w]\w+\d+$')
    TC_fmt2b = re.compile(r'^FF\d+')
    TC_fmt3b = re.compile(r'^FF$')
    TC_fmt3c = re.compile(r'^\d+')
    adhoc_fmt4 = re.compile(r'ADHOC')
    tc_range = r'[\d+|\w+]+FF\d+[\/]\d+'  # eg TYO4102FF800/701
    tc_range_compiled = re.compile(tc_range)
    tourcode_counter = Counter()
    # fs.update_many(select_dict,{'$unset':{'tourcodes':0 ,'adhoc_tourcodes': 0}})
    t2 = datetime.now()
    print('clearing tourcodes took', (t2 - t1).total_seconds())
    # for fare in fs.find({'filename'  : '7HAN088.html'},{'wordlist': 1}):
    # for fare in fs.find({'teststring':{'$regex':TOURCODE_REGEX}},{'wordlist': 1, 'filename':1}):
    for fare in fs.find(select_dict, {'wordlist': 1, 'filename': 1}).batch_size(1000):
        # tourcodes = [TC_fmt1.match(word).group(0)   for word in fare['wordlist'] if TC_fmt1.match(word) ]
        tc_counter_list = []
        tourcodes = []
        adhoc = Counter()
        word_buffer = deque(maxlen=5)
        tourcode_counter = Counter()
        cnt += 1
        print(cnt, 'Processing', fare['filename'], "{0:.1f}".format(cnt * 100 / total), "% complete")
        if (cnt % 1000) == 0:
            t3 = datetime.now()
            cum = (t3 - t2).total_seconds()
            rem = ((total - cnt) * cum / cnt) / 60
            etf = (t3 + timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat = f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)
        if TC_fmt1.search(fare['filename']):
            # tourcodes += [TC_fmt1.search(fare['filename']).group(0)]
            tourcode_counter[TC_fmt1.search(fare['filename']).group(0)] += 1
            if DEBUG:
                print('From filename:', tourcode_counter.keys())
        if adhoc_fmt4.search(fare['filename']):  # adhoc file
            fare['faretype']: 'paper'

        for word in fare['wordlist']:
            word_buffer.append(word.upper())
            if DEBUG:
                print('tourcodes:', list(tourcode_counter.keys()))
            if TC_fmt1.search(word):
                tourcodes += [TC_fmt1.search(word).group(0)]
                tourcode_counter[TC_fmt1.search(word).group(0)] += 1
                if DEBUG:
                    print('Found:', TC_fmt1.search(word).group(0))
                if bool(['ADHOC' in w for w in word_buffer if
                         'ADHOC' in w]):  # check if 'ADHOC' is within 4 words of Tourcode
                    adhoc[tourcodes[-1]] += 1
                    if DEBUG:
                        print('From adhoc:', tourcodes)
                continue
            # two way split
            if len(word_buffer) > 1:
                if TC_fmt2a.search(word_buffer[-2]) and TC_fmt2b.search(word_buffer[-1]):
                    w1 = TC_fmt2a.search(word_buffer[-2]).group(0)
                    w2 = TC_fmt2b.search(word_buffer[-1]).group(0)
                    tourcodes += [w1 + w2]
                    tourcode_counter[w1 + w2] += 1
                    if DEBUG:
                        print('Found 2 way split tourcode:', w1, w2)
                    if bool(['ADHOC' in w for w in word_buffer if
                             'ADHOC' in w]):  # check if 'ADHOC' is within 4 words of Tourcode
                        adhoc[tourcodes[-1]] += 1
                    if DEBUG:
                        print('Tourcodes (2 way split):', tourcodes)
                    continue
            # threeway split
            if len(word_buffer) > 2:
                if TC_fmt2a.search(word_buffer[-3]) and TC_fmt3b.search(word_buffer[-2]) and TC_fmt3c.search(
                        word_buffer[-1]):
                    w1 = TC_fmt2a.search(word_buffer[-3]).group(0)
                    w2 = TC_fmt3b.search(word_buffer[-2]).group(0)
                    w3 = TC_fmt3c.search(word_buffer[-1]).group(0)
                    tourcodes += [w1 + w2 + w3]
                    tourcode_counter[w1 + w2 + w3] += 1
                    if DEBUG:
                        print('Found 3 way split tourcode:', w1, w2, w3)
                    if bool(['ADHOC' in w for w in word_buffer if
                             'ADHOC' in w]):  # check if 'ADHOC' is within 4 words of Tourcode
                        adhoc[tourcodes[-1]] += 1
                        # print(tourcodes)
                    continue

                    # tc=list(set(tourcodes))

        # tc=[x for x in tc if '为' not in x]  # exclude tourcodes like Plan为FF701
        # print('keys',tourcode_counter.keys())

        tc_counter_list = [x for x in tourcode_counter.keys() if '为' not in x]
        adhoc_counter_list = [x for x in adhoc if '为' not in x]
        # print('tourcode_counter 1',tc_counter_list )

        adhoc_counter_list += [code for code in tc_counter_list if code[-1] == '6']  # add any ending in 6
        adhoc_tc = list(set(adhoc_counter_list))
        # adhoc_tc=[x for x in tc if '为' not in x]  # exclude tourcodes like Plan为FF701

        if DEBUG:
            print('tourcodes:', tc_counter_list)
        # fs.update_one(fare, { "$addToSet": { "tourcodes": tourcodes }})
        # fs.update_one({'_id':fare['_id']}, { "$unset": { "tourcodes": 0,
        #                                                 'adhoc_tourcodes': 0}})

        fs.update_one({'_id': fare['_id']}, {"$set": {"tourcodes": tc_counter_list,
                                                      'fat_run_id': run_id,
                                                      'adhoc_tourcodes': adhoc_tc}})
        for tc in tc_counter_list:
            #     fs.update_one({'_id':fare['_id']}, { "$addToSet": { "tourcodes": tc}})
            #     fs.update_one({'_id':fare['_id']}, { "$set": { 'fat_run_id':run_id}})

            try:
                tourcodes_master.update_one({'_id': fare['_id']},
                                            {'$set': {'tour_cd': tc,
                                                      'source': 'faresheet',
                                                      'imported': run_id
                                                      }},
                                            upsert=True)
            except pymongo.errors.DuplicateKeyError:
                pass
        TOURCODES.update(tc_counter_list)
        # adhoc_tc=list(set(adhoc))
        # adhoc_tc=[x for x in tc if '为' not in x]  # exclude tourcodes like Plan为FF701
        # fs.update_one({'_id':fare['_id']}, { "$set": { 'adhoc_tourcodes':adhoc_tc  }})

    select_dict['teststring'] = {'$regex': tc_range}
    # for fare in  fs.find({'teststring':{'$regex':tc_range}},{
    for fare in fs.find(select_dict, {
        'filename': 1,
        'teststring': 1,
        "tourcodes": 1}).batch_size(1000):  # checking for ranges abt 2% of faresheets have this

        print('Processing:', fare['filename'])
        tc_counter_list = []
        tourcodes = fare['tourcodes']
        adhoc = Counter()
        tourcode_counter = Counter()
        tc_ranges_list = re.findall(tc_range_compiled, fare['teststring'])
        for t in tc_ranges_list:  # eg ['OKA4601FF801/701', 'OKA4601FF800/700']
            # print(t)
            components = t.split('/')
            print(components)
            # TC1=TC_fmt1.search(t).group(0)  # 'OKA4601FF801'
            tourcodes += [components[0]]
            tourcode_counter[components[0]] += 1
            TC_stock = components[0][:-3]  # 'OKA4601FF'
            for tail in components[1:]:
                # TC_tail=t[-3:]                  # '701'
                newTC = TC_stock + tail  # 'OKA4601FF701'
                tourcodes += [newTC]
                tourcode_counter[newTC] += 1
        # print('keys 2',tourcode_counter.keys())

        tc_counter_list = [x for x in tourcode_counter.keys() if '为' not in x]
        if DEBUG:
            print('Updated tourcodes', tourcodes)
            print('tourcode_counter', tc_counter_list)
        fs.update_one({'_id': fare['_id']}, {"$set": {"tourcodes": tc_counter_list,
                                                      'fat_run_id': run_id,
                                                      'adhoc_tourcodes': adhoc_tc}})
        # for tc in tc_counter_list:
        #     fs.update_one({'_id':fare['_id']}, { "$addToSet": { "tourcodes": tc}})
        try:
            tourcodes_master.update_one({'_id': fare['_id']},
                                        {'$set': {'tour_cd': tc,
                                                  '           source': 'faresheet',
                                                  'imported': run_id
                                                  }},
                                        upsert=True)
        except pymongo.errors.DuplicateKeyError:
            pass

        TOURCODES.update(tc_counter_list)
        # fs.update_one({'_id':fare['_id']}, { "$set": { "tourcodes": tc_counter_list }})
    # print('tourcode_counter 2',tc_counter_list )
    # {teststring:RegExp('ad hoc|Ad hoc|ADHOC|adhoc')}   7,264 count

    t5 = datetime.now()
    bench2 = (t5 - t1).total_seconds() / 60
    msg = f'TOTAL tourcodes setup time: {bench2:.1f} minutes'
    print(msg)


def setup_search_for_tourcodes(db,tourcodes='CXtourcodes', master='CXtourcodes_master'):
    t1= datetime.now()
    dt=db[tourcodes]
    db_master=db[master]
    bcode=re.compile(r'^BCODE')
    it=re.compile(r'^IT')
    for tc in dt.find():
        tour_cd=tc['tour_cd']
        #eliminate spaces
        clean_tc= tour_cd.replace(' ','')
        if db_master.find({'tour_cd':clean_tc}):   #if there is a legitimate ITxxxx code
            dt.update_one({'_id': tc['_id']}, {'$set': {'search_tour_cd':clean_tc}})
            continue
        if it.search(clean_tc):
            clean_tc=clean_tc[2:]
        if bcode.search(clean_tc):
            clean_tc=clean_tc[5:]
        if clean_tc!=tour_cd:
            print('orig:',tour_cd  ,'search:',clean_tc)
        dt.update_one({'_id': tc['_id']}, {'$set': {'search_tour_cd':clean_tc}})
    t3=  datetime.now()
    bench2=(t3-t1).total_seconds()
    msg=f'TOTAL tourcodes search setup time: {bench2:.1f} seconds'
    print(msg)

def update_tourcodesmaster_from_faresheets(run_id=0, search_dict={}):
    t1= datetime.now()
    if run_id==0:
        run_id=int(t1.strftime('%Y%m%H%M'))
    search_dict['tourcodes']={'$exists':True} #only work with faresheets with tourcodes
    search_dict['run_id']={'$ne':run_id}
    found=fs.find(search_dict,{'tourcodes':1})
    total=found.count()
    print('Processing',total, 'faresheets using run_id',run_id)
    for cnt,fare in enumerate(found):
        for tc in fare['tourcodes']:
            try:
                tourcodes_master.update({'tour_cd':tc},{'$set':{
                                                            'source':'faresheet',
                                                            'imported':run_id
                                                            }},upsert=True)
                tourcodes_master.update_one({'tour_cd':tc},{'$inc': {'faresheet_count':1}} ) #count of the number of faresheets with this tourcode
            except pymongo.errors.DuplicateKeyError:
                pass
        if (cnt % 100) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)


    t3=  datetime.now()
    bench2=(t3-t1).total_seconds()/60
    msg=f'TOTAL tourcodes master update from faresheets time: {bench2:.1f} minutes'
    print(msg)




##############################################
#         FARE TABLE Processing              #
##############################################

def get_col_text(df_col, col):
    (x, y) = df_col.shape
    txt = ''
    for i, el in df_col[col][-x + 1:].items():
        txt += ' ' + el.text
    return txt.strip()


def flow_col_text_up(df_col, col):
    (x, y) = df_col.shape


# def get_col(hdr_chunks_list,tag):
#     check_list=list(map(lambda x:find_subtrees(x, tag) ,hdr_chunks_list ))
#     col=[cnt for cnt, x in enumerate(check_list) if x != []]
#     if len(col)>1:
#         colnums=[c+1 for c in col]
#         print('COLUMNS MAPPING TO DUPLICATES!', colnums)
#         raise ValueError('col number should be unique')
#     if col:
#         [col]=col
#     return col
def get_col(hdr_chunks_list, tag):
    check_list = list(map(lambda x: find_subtrees(x, tag), hdr_chunks_list))
    col = [cnt for cnt, x in enumerate(check_list) if x != []]
    # if len(col)>1:
    #     colnums=[c+1 for c in col]
    #     print('COLUMNS MAPPING TO DUPLICATES!', colnums)
    #     raise ValueError('col number should be unique')
    # if col:
    #     [col]=col
    return col


def get_hdr_dict(hdr_txt, norm_hdr_list, DEBUG=False):
    hdr_chunks_list = list(map(myTagger, hdr_txt))
    tag_col_number = {}
    for tag in norm_hdr_list:

        col_list = get_col(hdr_chunks_list, tag)
        if DEBUG:
            print('looking for', tag, ' found ', col_list)
        # check for duplicate cols, exclude what has been detected earlier
        if col_list == []:
            tag_col_number[tag] = []

            continue
        for col in col_list:
            if DEBUG:
                print('before', tag_col_number)
            if col not in tag_col_number.values():  # column has  been allocated
                tag_col_number[tag] = col
            if DEBUG:
                print('after', tag_col_number)

    return tag_col_number


def correct_hdr(texts, NWORDS):
    translation_table = dict.fromkeys(map(ord, r'()!@#$"“”````'), None)
    translation_table2 = dict.fromkeys(map(ord, "。:.,'"), ' ')
    translation_table3 = dict.fromkeys(map(ord, '/'), ' / ')
    translation_table4 = dict.fromkeys(map(ord, '-'), ' - ')
    texts = [word.translate(translation_table) for word in texts]  # drop parens and other odd characters
    texts = [word.translate(translation_table2) for word in
             texts]  # ensure sentence and word boundary typos are fixed along the way
    texts = [word.translate(translation_table3) for word in
             texts]  # insert spaces between / so routes can be recognised
    texts = [word.translate(translation_table4) for word in texts]
    texts = [x.split(' ') for x in texts]
    out = []
    # hdr_list=texts.split(' ')
    for sent in texts:
        for word in sent:
            if isalpha(word):
                out += [correct(word, NWORDS) for x in texts]
    return out

def process_fare_table(run_id,
                       filename,
                       country,
                       html_table,
                       originating,
                       tourcodes,
                       flights,
                       sales_period,
                       tkt_dis_date,
                       trv_dis_date,
                       rtw_fare_comm,
                       ct_fare_comm,
                       pos):
    # to add a new column, add to input arguments above **add_column**
    comm_collection = db['CXcommissions_' + run_id]
    # 'CORPORATE_DISCOUNT' must be processed before 'AGENT_DISCOUNT' as 'discount' is used for both
    hdr_list = ['RBD', 'FBC', 'CORPORATE_DISCOUNT', 'AGENT_DISCOUNT', 'ROUTE']
    row_dict = {}

    rows = parse(html_table)
    # Process headers
    hdr_txt = [x.text for x in rows[0]]
    (hdr_col_number, tag_hdr_dict) = get_header_dicts(hdr_txt, hdr_list, filename)
    if hdr_col_number['AGENT_DISCOUNT'] == []:  # no commission information in this table
        return []

    imp = pd.DataFrame(data=rows)

    # Fill down table
    (x, y) = imp.shape
    fares = pd.DataFrame(np.nan, index=imp.index, columns=imp.columns)
    blanks = Counter()
    for col in range(y):
        above = ""
        for i in range(x):
            txt = imp.iloc[i, col].text.upper()
            if txt == '':
                blanks[col] += 1
            if i > 1 and txt == '':
                fares.iloc[i, col] = above
            else:
                above = txt
                fares.iloc[i, col] = txt
    # re-process columns with mostly blanks
    for col in range(y):
        if (blanks[col] / (x + 1)) > 0.5:
            col_text = get_col_text(imp, col)
            for i in range(1, x):  # fill column with this text
                fares.iloc[i, col] = col_text

    fares.columns = fares.iloc[0]  # define column headers

    # hdr_txt=list(fares)
    # hdr_chunks_list=list(map(FareTagger,hdr_txt))
    # rename_dict={}
    # for col_hdr in hdr_txt:
    #     rename_dict[col_hdr]=get_col(hdr_chunks_list,tag)

    # turn row data into objects in preparation to expand RBDs to individual rows
    rbd_col = hdr_col_number['RBD']
    fares.iloc[0, rbd_col] = 'RBD'

    disc_col = hdr_col_number['AGENT_DISCOUNT']
    fares.iloc[0, disc_col] = 'AGENT_DISCOUNT'

    disc_col = hdr_col_number['CORPORATE_DISCOUNT']
    fares.iloc[0, disc_col] = 'CORPORATE_DISCOUNT'

    route_col = hdr_col_number['ROUTE']
    fares.iloc[0, route_col] = 'ROUTE'

    fbc_col = hdr_col_number['FBC']
    fares.iloc[0, fbc_col] = 'FBC'

    # tag_hdr_dict['ORIGINATING']='ORIGINATING'
    # tag_hdr_dict['EXCEPT']='EXCEPT'
    # print('old HEADERS',fares.columns)
    # print('new HEADERS',tag_hdr_dict)
    norm_fares = fares.rename(columns=tag_hdr_dict)
    norm_fares.columns = norm_fares.iloc[0]
    fare_rows = []
    # print(norm_fares)
    for index, row in norm_fares.iterrows():
        if 'AGENT_DISCOUNT' not in row.keys():  # not a commission table
            return []

        if 'ROUTE' in row.keys():
            route = check_route(row['ROUTE'])
        else:
            route = ''

        if 'RBD' in row.keys():
            rbds = row['RBD']
        else:
            rbds = ''

        if 'AGENT_DISCOUNT' in row.keys():
            discount = row['AGENT_DISCOUNT']

        if 'CORPORATE_DISCOUNT' in row.keys():
            corporate_discount = row['CORPORATE_DISCOUNT']
        else:
            corporate_discount = '0%'
        if 'FBC' in row.keys():
            fbc = row['FBC']
        else:
            fbc = ''
        # print(index,originating,excluded,route,rbds,discount)
        # to add a new column, modify statement below **add_column**
        fare_rows += [Fare_Row(tourcodes,
                               originating,
                               route,
                               rbds,
                               fbc,
                               discount,
                               corporate_discount,
                               flights,
                               sales_period,
                               tkt_dis_date,
                               trv_dis_date,
                               rtw_fare_comm,
                               ct_fare_comm,
                               pos)]  # data row

    # expand rows to individual RBDs
    row_dict['ORIGINATING'] = ['ORIGINATING']
    # row_dict['EXCEPT']=['EXCEPT']
    row_dict['ROUTE'] = ['ROUTE']
    row_dict['RBD'] = ['RBD']
    row_dict['FBC'] = ['FBC']
    row_dict['AGENT_DISCOUNT'] = ['AGENT_DISCOUNT']
    row_dict['CORPORATE_DISCOUNT'] = ['CORPORATE_DISCOUNT']
    ndf = pd.DataFrame()
    index = 0

    for row in fare_rows[1:]:  # exclude old row headers
        # print(row.fare_basis)
        row_dict = dict()
        for rbd in row.rbd:
            row_dict['TOURCODES'] = [row.tourcodes]  # to add a new column, add statement here **add_column**
            row_dict['ORIGINATING'] = [row.originating]
            row_dict['FLIGHTS'] = [row.flights]
            # row_dict['EXCEPT']=[row.excluded]
            row_dict['ROUTE'] = [row.route]
            row_dict['RBD'] = [rbd]
            row_dict['FBC'] = row.fbc
            row_dict['AGENT_DISCOUNT'] = [row.discount]
            row_dict['DISCOUNT_UNIT'] = [row.discount_unit]
            row_dict['CORPORATE_DISCOUNT'] = [row.corporate_discount]
            row_dict['SALES_PERIOD_EFF_DATE'] = row.sales_period_from
            row_dict['SALES_PERIOD_DIS_DATE'] = row.sales_period_to
            row_dict['TKT_DIS_DATE'] = row.tkt_dis_date
            row_dict['TRV_DIS_DATE'] = row.trv_dis_date
            row_dict['RTW_FARE_COMM'] = row.rtw_fare_comm
            row_dict['CT_FARE_COMM'] = row.ct_fare_comm
            row_dict['POS'] = row.pos
            row_dict['COUNTRY'] = country
            row_dict['FILENAME'] = filename

            rw = pd.DataFrame(row_dict)

            ndf = pd.concat([ndf, rw], ignore_index=True)  # add normalised RBD row
            # print(list(ndf))
            # print((index,row.originating,row.route,rbd,row.discount))
            index += 1
    ###### POST PROCESSING for ROUTES - fill blank routes with the most common
    rt = Counter()
    for row in ndf['ROUTE']:
        rt[row] += 1
        # print(row)
    (most_common, count) = rt.most_common()[0]
    # print('Most Common',most_common)

    for index, row in enumerate(ndf['ROUTE']):
        if not row:
            ndf.ix[index, 'ROUTE'] = most_common
    # decode from and to locations in ROUTE
    ndf['FROM_LOCATION'] = (ndf.ROUTE.apply(get_from_location))
    ndf['TO_LOCATION'] = (ndf.ROUTE.apply(get_to_location))

    # delete route rows that have non-route info in them
    remove = set(['FIRST', 'BUSINESS', 'ECONOMY', 'PREMIUM', 'CLASS'])
    drop_rows=[]

    for index, row in enumerate(ndf['ROUTE']):
        test = set(flattern((nltk.word_tokenize(row))))
        if test.intersection(remove):
            drop_rows+=[index]
    ndf_clean=ndf.drop(drop_rows)
    comm_collection.insert_many(ndf_clean.to_dict('records'))  # push to RAXDB
    print(filename, 'saved')
    return ndf_clean


def get_header_dicts_old(hdr_txt, norm_hdr_list, filename):
    # generate two dicts linking standard headers and column numbers in both directions
    hdr_chunks_list = list(map(myTagger, hdr_txt))
    # print('2307 hdr_chunks_list',hdr_chunks_list)
    # hdr_chunks_list=list(map(FareTagger,hdr_txt))
    try:
        hdr_col_number = get_hdr_dict(hdr_txt, norm_hdr_list)
    except ValueError:
        db.comm_exceptions.insert_one({'filename': filename})
        print('Stored', filename, 'in exception list')
        return None
    # set up standardized headers
    hdr_tag_dict = {}
    tag_hdr_dict = {}
    # hdr_txt=[x.text for x in rows[0]]

    # print('hdr_txt',hdr_txt)
    # print(hdr_chunks_list)
    try:
        # tags=conll_tag_chunks(hdr_chunks_list)
        # hdr_chunks_list2=[x[0] for x in hdr_chunks_list] #drop one level in Tree
        tags_list = list(map(conll_tag_chunks, hdr_chunks_list))
    except ValueError:
        print('2332 Value error', hdr_chunks_list)
        return []
    # hdrs=[ tag     for sent in tags for x,tag in sent if tag in ['B-RBD','B-FBC','B-DISCOUNT','B-ROUTE']]
    hdrs = [tag for sents in tags_list for sent in sents for x, tag in sent if
            tag in ['B-RBD', 'B-FBC', 'B-DISCOUNT', 'B-ROUTE']]
    # print ('hdr_chunks_list',hdr_chunks_list)
    if len(set(hdrs).intersection(set(['B-RBD', 'B-FBC', 'B-DISCOUNT', 'B-ROUTE']))) < 3:  # not a fare table
        return []
    for col_hdr in hdr_txt:
        chunk = myTagger(col_hdr)
        # print(chunk)
        check_cols = list(map(lambda tag: tag if find_subtrees(chunk, tag) else "", norm_hdr_list))
        hdr = [x for x in check_cols if x]
        if hdr:
            [hdr] = hdr
            hdr_tag_dict[hdr] = col_hdr  # mapping dictionaries
            tag_hdr_dict[col_hdr] = hdr
    return (hdr_tag_dict, tag_hdr_dict)


def get_header_dicts(hdr_txt, norm_hdr_list, filename):
    # generate two dicts linking standard headers and column numbers in both directions
    # hdr_chunks_list=list(map(myTagger,hdr_txt))
    # print('2307 hdr_chunks_list',hdr_chunks_list)
    # hdr_chunks_list=list(map(FareTagger,hdr_txt))
    try:
        hdr_tag_dict = get_hdr_dict(hdr_txt, norm_hdr_list)
    except ValueError:
        db.comm_exceptions.insert_one({'filename': filename})
        print('Stored', filename, 'in exception list')
        return None
    tag_hdr_dict = {v: k for k, v in hdr_tag_dict.items() if v != []}

    return (hdr_tag_dict, tag_hdr_dict)


def add_header_to_table(tab1, tab2):
    # tab1 and tab2 are BS soup text
    # function adds the header from tab1 to tab2
    tab1_hdr = str(tab1[0])  # get header from tab1
    str_tab2_list = [str(tag) for tag in tab2]
    tab3 = [tab1_hdr] + str_tab2_list
    str_tab3 = '<table>' + ' '.join(tab3) + '</table>'
    tab3_soup = BeautifulSoup(str_tab3, 'html.parser')
    return tab3_soup


def get_rtw_fare_comm(chunk):
    ST = find_subtrees(chunk, 'RTW_FARE_NO_COMMISSION')
    if ST:
        return 0
    else:
        return 9999  # need to add code for pulling special commissions


def get_ct_fare_comm(chunk):
    ST = find_subtrees(chunk, 'CIRCLE_TRIP_FARE_NO_COMMISSION')
    if ST:
        return 0
    else:
        return 9999  # need to add code for pulling special commissions


def reconstruct_route(loc_leaves):
    new_route = ''
    for tagged_loc in loc_leaves:
        new_loc = ''
        for loc, tag in tagged_loc:
            new_loc += ' ' + loc
        if new_route:
            new_route += ' - ' + new_loc
        else:
            new_route = new_loc.strip()
    return new_route


from nltk.corpus import names, ieer, gazetteers

LOCATIONS = set(gazetteers.words())


def check_route(route):
    # check to make sure that route are all of the same type eg city, airport, country

    loc_tagged = myTagger(route)
    ST = find_subtrees(loc_tagged, 'LOCATION')
    loc_leaves = [x.leaves() for x in ST]
    new_locs = loc_leaves
    tag_counter = Counter()
    for loc in loc_leaves:
        for word, tag in loc:
            tag_counter[tag] += 1
    common_tags = [tag for tag, cnt in tag_counter.most_common(2)]
    if ('CITY' or 'AIRPORT') in tag_counter and ('CITY' or 'AIRPORT') not in common_tags:  # look for countries instead
        new_locs = []
        for loc in loc_leaves:
            new_loc_leaves = []
            for test_word, tag in loc:
                corrected_leaf = (test_word, tag)
                if ('CITY' or 'AIRPORT') in tag:
                    # corrected =[ word for word in LOCATIONS if (test_word in word) and (test_word!= word)]  # this needs to be a lot smarter!
                    country = db.currency.find_one({'alt_names': test_word})
                    if country:
                        corrected = country['country']
                        corrected_leaf = (corrected, 'corrected')
                    # if corrected:
                    #     corrected_leaf=(corrected[0],'corrected')
                new_loc_leaves += [corrected_leaf]
            new_locs += [new_loc_leaves]
            return reconstruct_route(new_locs)
    else:
        return route

###############################################################
#   Commission Sheets production (extracting information)     #
##############################################################

def parse_CX_commission_sheet(filename, country):
    fare = fs.find_one({'filename': filename, 'country':country})
    return parse_CX_commission_sheet_fare(fare, run_id='_test', DEBUG=True)


def parse_CX_commission_sheet_fare(ff, run_id='', DEBUG=False):

    default_period = [[datetime(2016, 1, 1, 0, 0), datetime(9999, 12, 31, 0, 0)]]

    # ff=fs.find_one({'_id':ObjectId(fareID)})

    country = ff['country']
    filename = ff['filename']
    fare = FareFile(filename, country)
    #fare.tagged_hdrs = myTagger(fare.teststring)
    tourcode = ff['tourcodes']

    allowed_carrier_codes = ['CX', 'KA', 'AA']
    paras = fare.paras()
    souptext = fare.soup
    table_list = souptext.find_all('table')
    origins = {}
    prev_sent = ''
    results = []
    cnt = 1
    paragraph_info = {}
    # **addcolumn: document level
    SALESPERIOD = fare.find_period('SALESPERIOD')
    TKT_DIS_DATE = fare.find_tkt_dis_date_tagged(fare.tagged_hdrs)
    TRV_DIS_DATE = fare.find_trv_dis_date_tagged(fare.tagged_hdrs)
    RTW_FARE_NO_COMMISSION = get_rtw_fare_comm(fare.tagged_hdrs)
    CIRCLE_TRIP_FARE_COMMISSION = get_ct_fare_comm(fare.tagged_hdrs)

    if DEBUG:
        print('TOURCODE:', tourcode)
        print('SALESPERIOD', SALESPERIOD)
        print('TKT_DIS_DATE', TKT_DIS_DATE)
        print('TRV_DIS_DATE', TRV_DIS_DATE)
        print('RTW_FARE_NO_COMMISSION', RTW_FARE_NO_COMMISSION)
        print('CIRCLE_TRIP_FARE_COMMISSION', CIRCLE_TRIP_FARE_COMMISSION)
    # for para in range(0,len(fare.paragraph_data)):
    # pre-process paragraphs
    for cnt, para in fare.paragraph_data.items():
        # para=fare.paragraph_data[cnt]
        # print('processing paragraph',cnt)
        # print(para)
        para_tagged = {}
        para_tagged = myTagger(para)
        # para_tagged=FareTagger(para)
        # print('para_tagged',para_tagged)
        origins = find_origins(para_tagged)
        pos = find_POS(para_tagged)
        if DEBUG:
            print('processing paragraph',cnt)
            #print(para)
            #print('para_tagged',para_tagged)
            print('origins',origins)
            print('POS:',pos)
        paragraph_info[(cnt, 'origins')] = origins
        paragraph_info[(cnt, 'tkt_dis_date')] = TKT_DIS_DATE
        paragraph_info[(cnt, 'pos')] = pos
        sp_list = fare.find_period_detail(para_tagged, 'SALESPERIOD')
        if sp_list != default_period:
            paragraph_info[(cnt, 'sales_period')] = sp_list
        else:
            paragraph_info[(cnt, 'sales_period')] = SALESPERIOD

        flights = fare.find_flights(para_tagged, allowed_carrier_codes)
        paragraph_info[(cnt, 'flights')] = flights
        # print('FLIGHTS',flights)
        cnt += 1
    table_list = souptext.find_all('table')
    origins = {}
    prev_sent = ''
    results = []
    if table_list:  # make sure there are tables in this document
        for sent in paras:
            if '<table ' in sent:
                if DEBUG:
                    print('processing', sent)
                table_index = int(sent[-3:-1]) - 1
                target_table = table_list[table_index]
                if '<table ' in prev_sent:  # back to back tables, check if current table has headers
                    if DEBUG:
                        print('processing back to back')
                    tab1 = table_list[table_index - 1].find_all('tr')
                    tab2 = table_list[table_index].find_all('tr')
                    prev_table_hdr = tab1[0]
                    try:
                        prev_table_hdr_bgcolor = prev_table_hdr.td['bgcolor']
                    except KeyError:
                        prev_table_hdr_bgcolor = ''
                    if DEBUG:
                        print('BGCOLOR', prev_table_hdr_bgcolor)
                    try:
                        curr_table_hdr_bgcolor = tab2[0].td['bgcolor']
                    except KeyError:
                        curr_table_hdr_bgcolor = ''
                    if prev_table_hdr_bgcolor != curr_table_hdr_bgcolor:  # current table has no headers
                        target_table = add_header_to_table(tab1, tab2)
                        # carry paragraph info over from previous paragraph
                        paragraph_info[(table_index + 1, 'origins')] = paragraph_info[(table_index, 'origins')]
                        paragraph_info[(table_index + 1, 'flights')] = paragraph_info[(table_index, 'flights')]
                        paragraph_info[(table_index + 1, 'sales_period')] = paragraph_info[
                            (table_index, 'sales_period')]
                # print(sent, 'origins:', origins)
                # if '<table ' in prev_sent: # table is abutting previous table
                origins = paragraph_info[(table_index + 1, 'origins')]
                flights = paragraph_info[(table_index + 1, 'flights')]
                sales_period = paragraph_info[(table_index + 1, 'sales_period')]
                pos = paragraph_info[(table_index + 1, 'pos')]
                # print('TABLE ',table_index+1, 'pos',pos)
                # **addcolumn
                try:
                    ft = process_fare_table(run_id,
                                            filename,
                                            country,
                                            target_table,
                                            origins,
                                            tourcode,
                                            flights,
                                            sales_period,
                                            TKT_DIS_DATE,
                                            TRV_DIS_DATE,
                                            RTW_FARE_NO_COMMISSION,
                                            CIRCLE_TRIP_FARE_COMMISSION,
                                            pos)
                except IndexError:
                    print('Unable to process table ',table_index)
                # print('col headers',list(ft))
                if DEBUG:
                    print(ft)
                results += [ft]
            prev_sent = sent
        return results


def process_CX_commission_sheets(skip=0):
    t1 = datetime.now()
    found = fs.find({'$text': {'$search': '"INCENTIVE AGREEMENT"'}, 'country': 'US'},
                    {'filename': 1,
                     'country': 1,
                     'tourcodes': 1,
                     'filepath': 1})
    run_id = t1.strftime('%Y%m%d%H%M')
    # list of exception files
    exception = [x['filename'] for x in list(db.comm_exceptions.find({}, {'filename': 1, '_id': 0}))]

    print('Processing', found.count(), 'files using run_id:', run_id)

    for cnt, fare in enumerate(found):
        filename = fare['filename']
        if cnt < skip:
            continue
        if filename in exception:
            print(cnt, filename, ' skipped')
        else:
            print(cnt, 'Processing', filename)
            try:
                results = parse_CX_commission_sheet_fare(fare, run_id=run_id, DEBUG=False)
            except (KeyError,IndexError):
                db.comm_exceptions.insert_one({'filename': filename})
                print('Error processing: ', filename)
                continue

    t5 = datetime.now()
    print('total processing', (t5 - t1).total_seconds(), 'seconds')





#############################################################
#       Extracting commission/extension/effective dates     #
############################################################

DEBUG = False

def extract_commissions(tables, col_hdr, regex):
    adhoc_col = ''
    adhoc_codes = []
    word_buffer = deque(maxlen=5)
    for table in tables:
        for r, row in enumerate(table):
            for cell in row:
                # print('sec3',k, sec3)
                # print('TEXT',sec3.text)
                word_buffer.append(cell.text.upper())
                if cell.col == adhoc_col and col_hdr not in cell.text.upper():
                    if regex.search(cell.text):
                        if bool(['BASE' in w for w in word_buffer if 'BASE' in w]):
                            continue
                        else:
                            adhoc_codes += regex.findall(cell.text)
                            # print(word_buffer)
                            # print(adhoc_col_text, cell.text)

                if col_hdr in cell.text.upper():
                    adhoc_col = cell.col
                    adhoc_col_text = cell.text
                    hdr_row = r
                    print('Target column:', adhoc_col, adhoc_col_text)

    return adhoc_codes

def find_faresheet_extension_dates(filename='', country=''):
    typo_cnt = 0
    translation_table2 = dict.fromkeys(map(ord, "。:.,'"), ' ')
    if filename:
        DEBUG = False
    else:
        DEBUG = False
    cnt = 0
    mandarin = 0
    search_phrases = ['Previous version',
                      'extended for sales till',
                      'validity revised and extended till',
                      '上 一 版 本 ']

    for keyword in search_phrases:
        search_txt = "\"" + keyword + "\""
        pivot = keyword.split()[0]
        search_dict = {"$text": {"$search": search_txt}, 'faretype': 'filed'}
        if filename:
            search_dict = {}
            search_dict['filename'] = filename
            search_dict['country'] = country

        # for fare in fs.find({ "$text": { "$search": search_txt } ,'faretype':'filed'}) :
        if DEBUG:
            print(fs.find(search_dict).count(), 'files found')
            print(search_dict)
        for fare in fs.find(search_dict):
            # if fare['filename']!='SHA9801,9802FF7XX_R1702_NAM_wef 04DEC17-31MAR19.html':
            #     continue
            ext_date = ''
            if True:
                cnt += 1
                print(cnt, 'processing', fare['filename'])
            if keyword != '上 一 版 本 ':

                sel = ngram2(30, pivot, fare['wordlist'])
                sel_sent = ' '.join(sel)
                try:
                    if DEBUG:
                        print('sel_sent', sel_sent)
                        print(find_subtrees(myTagger(sel_sent), 'EXTENSION_DATE'))
                    ext_date = extract_extension_date(sel_sent)

                    if DEBUG:
                        print('EXTENSION_DATE:', ext_date)
                except (IndexError, ValueError):
                    # print('EXTENSION NOT DETECTED', fare['filename'])
                    # print(sel_sent)

                    continue
            else:
                ff = FareFile(fare['filename'], fare['country'])
                mandarin += 1
                # souplist=ff.soup.select('.s1')+ff.soup.select('.s2')
                # cleanText=[ (sent.text.translate(translation_table2)) for sent in souplist ]
                cleanText = ff.get_s1s2()
                print('cleanText:', cleanText)
                ext_date = extract_extension_date(cleanText, DEBUG=False)
                # tagged=Tree('S',[myTagger(sent) for sent in cleanText])
                # ed_chunk=find_subtrees(tagged,'EXTENSION_DATE')
                # if DEBUG:
                #     print()
                #     print('in extract_extension_date' )
                #     print('TAGGED',tagged)
                #     print('ed_chunk',ed_chunk)
                #     sel_sent=tagged
                # ext_date=None
                # if ed_chunk:
                #     ext_date=min([ extractDateXX(x) for x in ed_chunk ])
                if DEBUG:
                    print('Extension date:', ext_date)

            # print( 'sel',sel, 'sel_sent',sel_sent)
            # tagged=myTagger(sel_sent)
            # print(tagged)
            # sp=find_period_detail(tagged, 'TODATE')
            # if sp[0][1].year!=9999:
            if ext_date:
                if DEBUG:
                    print(fare['faretype'], fare['filename'])
                    print(sel_sent)
                # print(tagged)
                # ext_date= sp[0][1].strftime('%Y%m%d')
                # ext_date_pretty=sp[0][1].strftime('%d-%b-%Y')

                # get effective date for this fare
                effective_date = get_effective_date(fare['filename'])
                extension_from_fn = (ext_date - effective_date['from_fn']).days
                extension_from_body = (ext_date - effective_date['from_body']).days
                if DEBUG:
                    print('effective_dates:', effective_date)
                    print('extension_from_fn', extension_from_fn, 'extension_from_body', extension_from_body)
                if 0 < extension_from_body <= 30:
                    res = {'extension_date': int(ext_date.strftime('%Y%m%d')),
                           'effective_date': int(effective_date['from_body'].strftime('%Y%m%d')),
                           'extension': extension_from_body
                           }
                    old_effective_date = effective_date['from_body']
                else:
                    res = {'extension_date': int(ext_date.strftime('%Y%m%d')),
                           'effective_date': int(effective_date['from_fn'].strftime('%Y%m%d')),
                           'extension': extension_from_fn
                           }
                    old_effective_date = effective_date['from_fn']
                ############# temporary patch for broken html which leads to extensions of -358 to -366 by adding a year to extension date and recalculate extension
                if res['extension'] < -350:
                    if ext_date.year == 2016:  # update extension date
                        updated_year = ext_date.year + 1
                        revised_ext_date = ext_date.replace(year=updated_year)
                        revised_extension = (revised_ext_date - old_effective_date).days
                        res['extension_date'] = revised_ext_date
                        res['extension'] = revised_extension
                    else:  # update effective date
                        updated_year = old_effective_date.year - 1
                        revised_effective_date = old_effective_date.replace(year=updated_year)
                        revised_extension = (ext_date - revised_effective_date).days
                        res['effective_date'] = revised_effective_date
                        res['extension'] = revised_extension

                if DEBUG:
                    print('faresheet extended')
                    print('Extension date', res['extension_date'],
                          'effective_date', res['effective_date'],
                          'extension', res['extension'], 'days',
                          fare['filename'])

                ############ end of patch
                # if DEBUG:
                #         print('Fixing typo. extension days:',extension)
                # if not(-100<= extension <=100):#probably a typo on extension date or effective dates
                #     if DEBUG:
                #         print('Fixing typo. extension_date:',extension)
                #     if effective_date.year>= 2017:
                #         #typo is in extension date
                #         # curr_year=ext_date.year
                #         # ext_date=ext_date.replace(year=curr_year+1)
                #         # extension=(ext_date-effective_date).days
                #         # print('{:^20}{:^20}{:^20}{:<200}'.format(ext_date,effective_date,extension,fare['filename']))
                #         #print(typo_cnt, 'Typo extension year',fare['filename'])
                #         typo_cnt+=1
                #     else:
                #         #typo is in effective date
                #         # curr_year=effective_date.year
                #         # effective_date=effective_date.replace(year=curr_year+1)
                #         # extension=(ext_date-effective_date).days

                # if 15<= extension <= 45: # extension date overstated, reduce by a month
                #     # curr_month=ext_date.month
                #     # ext_date=ext_date.replace(month=curr_month-1)
                #     # extension=(ext_date-effective_date).days
                #     #print(typo_cnt, 'Typo extension month overstated',fare['filename'])
                #     print('{:^20}{:^20}{:^20}{:<200}'.format(ext_date,effective_date,extension,fare['filename']))
                #     typo_cnt+=1
                # if -45<= extension <= -15: # extension date understated, reduce by a month
                #     # curr_month=ext_date.month
                #     # ext_date=ext_date.replace(month=curr_month+1)
                #     # extension=(ext_date-effective_date).days
                #     #print(typo_cnt, 'Typo extension month understated',fare['filename'])
                #     print('{:^20}{:^20}{:^20}{:<200}'.format(ext_date,effective_date,extension,fare['filename']))
                #     typo_cnt+=1
                fs.update_one({'_id': fare['_id']}, {'$set': res})
                print('Updated ', fare['filename'])

                # print(sel)
    if not filename:
        print(cnt, 'faresheets had extensions, including', mandarin, 'in mandarin')
    else:
        updt_fare = fs.find_one({'filename': filename})
        return {'extension_date': updt_fare['extension_date'],
                'effective_date': updt_fare['effective_date'],
                'extension': updt_fare['extension']
                }


def get_effective_date(filename):
    from_fn = get_effective_date_from_fn(filename)
    from_body = get_effective_date_from_body(filename)
    return {'from_fn': from_fn, 'from_body': from_body}


def get_effective_date_from_body(filename):
    fare = fs.find_one({'filename': filename})
    ff = FareFile(fare['filename'], fare['country'])
    periods = ff.CX_periods()
    return ff.effective_date


def get_effective_date_from_fn(filename):
    DEBUG = False
    fn_list = filename.split('_')
    fn = ' '.join(fn_list)
    tagged_filename = myTagger(fn)
    if DEBUG:
        print(tagged_filename)
    ed_chunk = Tree('S', find_subtrees(tagged_filename, 'SALESPERIOD'))
    if not ed_chunk:  # Sales period not explicit
        ed_chunk = Tree('S', find_subtrees(tagged_filename, 'RANGE'))
    if DEBUG:
        print('ed_chunk')
        print(ed_chunk)
    from_chunk = Tree('S', find_subtrees(ed_chunk, 'FROMDATE'))
    is_dmdate = find_subtrees(from_chunk, 'DMDATE')
    is_ddate = find_subtrees(from_chunk, 'DDATE')

    if is_dmdate:
        to_chunk = find_subtrees(ed_chunk, 'TODATE')  # may have multiple TODATEs
        to_date = extractDateXX(to_chunk[0])  # pick first one
        to_YEAR = to_date.year
        to_MONTH = to_date.month

    if is_ddate:
        [to_chunk] = find_subtrees(ed_chunk, 'TODATE')
        to_date = extractDateXX(to_chunk)
        to_YEAR = to_date.year
        to_MONTH = to_date.month
        fr_DAY = int(find_leaves(is_ddate[0], 'CD')[0])
        if DEBUG:
            print('IN is_ddate', is_ddate)
            print('YMD', to_YEAR, to_MONTH, fr_DAY)

    if DEBUG:
        print('from_chunk')
        print(from_chunk)
    if from_chunk:
        effective_date = extractDateXX(from_chunk)
        if DEBUG:
            print('from date:', effective_date)
        if is_dmdate:  # default year,month
            from_MONTH = effective_date.month
            if DEBUG:
                print('from_MONTH:', from_MONTH, 'to_MONTH:', to_MONTH, 'to_YEAR', to_YEAR)
            if from_MONTH > to_MONTH:
                effective_date = effective_date.replace(year=to_YEAR - 1)
            else:
                effective_date = effective_date.replace(year=to_YEAR)
            if DEBUG:
                print('corrected from date:', effective_date)
        if is_ddate:  # default year,month
            effective_date = datetime(year=to_YEAR, month=to_MONTH, day=fr_DAY)
            if DEBUG:
                print('corrected from date:', effective_date)
    else:  # no FROM date -> look for TO date

        to_chunk = find_subtrees(ed_chunk, 'TODATE')
        if to_chunk:
            effective_date = extractDateXX(to_chunk[0])
        else:
            effective_date = extractDateXX(ed_chunk)
            # sales_periods=find_period_detail(tagged_filename,'SALESPERIOD')

    if DEBUG:
        # print('sales period', sales_period)
        print('EFFECTIVE DATE', effective_date)
        print('tagged_filename', tagged_filename)
        print('effective_date from filename', effective_date.strftime('%Y%m%d'), filename)

    if effective_date == datetime(9999, 1, 1, 0, 0):  # No sales period info in title
        travel_periods = find_period_detail(tagged_filename, 'TRAVELPERIOD')
        if travel_periods:
            travel_period = travel_periods[0]
            effective_date = travel_period[0] if travel_period[0] is not datetime(2016, 1, 1, 0, 0) else travel_period[
                1]

    return effective_date


def extract_extension_date(sel, DEBUG=False):
    # DEBUG=True
    tagged = Tree('S', [myTagger(sent) for sent in sel])
    # tagged=myTagger(sel)

    ed_chunk = find_subtrees(tagged, 'EXTENSION_DATE')
    if DEBUG:
        print()
        print('in extract_extension_date function')
        print(tagged)
        print(ed_chunk)
    ext_date = None
    if ed_chunk:
        ext_date = min([extractDateXX(x) for x in ed_chunk])
        if DEBUG:
            print('Extension date:', ext_date)
    return ext_date


def ngram2(n, word1, wordlist):
    word = word1.upper()
    l = [i for i, j in zip(count(), wordlist) if j.upper() == word]  # find all occurances of `word`
    feat = {}
    # print(l)
    res = []
    max_len = len(wordlist)
    for j, i in zip(count(), l):
        # print('extract:',j,'is', wordlist[max(0,(i-n)) : min(max_len,(i+n))])
        res += wordlist[max(0, (i - n)): min(max_len, (i + n))]
        # print('res',res)
        # context_list=break_notes(wordlist[(i-n) : (i+n)]) #find the words excluding numbers
        # print(context_list)
        # context_list=[note for note in context_list if word1 in note]
        # print(context_list)
    return res






########################################################
#   Exporting Tourcodes/Extension dates/Faresheets     #
########################################################
def export_tourcodes(run_id=0, select_dict={}):
    t1 = datetime.now()
    tcdb = db['CXtourcodes']
    fs = db['CXfaresheets']
    if run_id:
        select_dict['et_run_id'] = {'$ne': run_id}
        total_codes = tcdb.find(select_dict).count()
    else:
        total_codes = tcdb.find().count()
        run_id = t1.strftime('%Y%m%H%M')

    print('Processing using et_run_id:', run_id, 'total to be processed', total_codes)
    code_cnt = 0
    correction_cnt = 0
    export_id = t1.strftime('%Y%m%H%M')
    NWORDS = Counter()

    for fare in db.CXtourcodes_master.find({}, {'tour_cd': 1, '_id': 0}):
        for code in fare['tour_cd']:
            NWORDS[code] += 1
            # export_dict={}
    # row_dict={}
    filename = 'tourcodes_' + str(run_id) + '_' + export_id + '.csv'
    ext_dates_filename = 'tourcodes_extension_dates_' + str(run_id) + '_' + export_id + '.csv'
    f = open(filename, "w")
    # e=open(ext_dates_filename,"w")
    # with open(filename,"wb") as csv_file:
    # writer = csv.writer(csv_file, delimiter=',')
    line = ','.join(['tour_cd', 'search_tour_cd', 'CAT35_filed', 'faretype', 'total_file_cnt', 'paper', 'filed'])

    f.write(line + '\n')

    for code in tcdb.find(select_dict).batch_size(500):
        code_cnt += 1
        try:
            filed = code['filed']
        except KeyError:
            filed = ''
        tour_cd = code['search_tour_cd']
        print(code_cnt, 'Processing', tour_cd, "{0:.1f}".format(code_cnt * 100 / total_codes), "% complete")
        # export_dict['tour_cd']=code['tour_cd']
        # export_dict['CAT35']=code['filed']

        file_cnt = 0
        paper_cnt = 0
        filed_cnt = 0
        faretype = 'Not found'

        res = get_file_counts(fs, tour_cd)
        if res['faretype'] == 'Not found' and filed == '':
            corrected = list(
                correct_CXtourcode(code['search_tour_cd'], NWORDS))  # a set of possible codes, default:search_tour_cd
            correction_cnt += 1
            if corrected[0] == code and len(corrected) == 1:
                pass
            else:
                print(tour_cd, 'corrected to:', corrected)
            for candidate in corrected:
                res = get_file_counts(fs, candidate)
                line = ','.join([code['tour_cd'], code['search_tour_cd'], filed, res['faretype'],
                                 str(res['file_cnt']), str(res['paper_cnt']), str(res['filed_cnt'])])
                f.write(line + '\n')
                tcdb.update_one({'_id': code['_id']}, {'$addToSet': {'file_counts': res}})
            continue

        if filed != 'Y' and res[
            'faretype'] == 'filed':  # fare is not in CAT35 but is found in faresheets =>must be PAPER
            res['faretype'] = 'paper'
        res['search_tour_cd'] = tour_cd
        tcdb.update_one({'_id': code['_id']}, {'$addToSet': {'file_counts': res}})
        tcdb.update_one({'_id': code['_id']}, {'$set': {'et_run_id': run_id}})

        line = ','.join([code['tour_cd'], tour_cd, filed, res['faretype'],
                         str(res['file_cnt']), str(res['paper_cnt']), str(res['filed_cnt'])])
        f.write(line + '\n')

    export_extension_dates(run_id)
    if correction_cnt > 0:
        print(correction_cnt, 'tourcodes corrected')
    print('Exported to:', filename)


def export_tourcodes_to_db(select_dict={}):
    t1 = datetime.now()
    tcdb = db['CXtourcodes']
    fs = db['CXfaresheets']
    if select_dict:
        total_codes = tcdb.find().count() - tcdb.find(select_dict).count()
    else:
        total_codes = tcdb.find().count()

    if 'et_run_id' in select_dict.keys():
        run_id = select_dict['et_run_id']
        select_dict['et_run_id'] = {'$ne': run_id}
    else:
        run_id = t1.strftime('%Y%m%H%M')

    print('Processing using et_run_id:', run_id, 'total to be processed', total_codes)
    code_cnt = 0
    correction_cnt = 0
    export_id = t1.strftime('%Y%m%H%M')
    NWORDS = Counter()
    for code in db.CXtourcodes_master.find():
        NWORDS[code['tour_cd']] += 1
        # export_dict={}
    # row_dict={}
    filename = 'tourcodes_' + str(run_id) + '_' + export_id
    ext_dates_filename = 'tourcodes_extension_dates_' + str(run_id) + '_' + export_id
    export_tc = db[filename]
    export_ed = db[ext_dates_filename]
    # f=open(filename,"w")
    # e=open(ext_dates_filename,"w")
    # with open(filename,"wb") as csv_file:
    # writer = csv.writer(csv_file, delimiter=',')
    # line= ','.join(['tour_cd','search_tour_cd','CAT35_filed','faretype','total_file_cnt','paper','filed'] )

    # f.write(line+'\n')

    for code in tcdb.find(select_dict).batch_size(500):
        code_cnt += 1
        keys = ['tour_cd', 'search_tour_cd', 'CAT35_filed', 'faretype', 'total_file_cnt', 'paper', 'filed']
        try:
            filed = code['filed']
        except KeyError:
            filed = ''
        tour_cd = code['search_tour_cd']
        print(code_cnt, 'Processing', tour_cd, "{0:.1f}".format(code_cnt * 100 / total_codes), "% complete")
        # export_dict['tour_cd']=code['tour_cd']
        # export_dict['CAT35']=code['filed']

        file_cnt = 0
        paper_cnt = 0
        filed_cnt = 0
        faretype = 'Not found'

        res = get_file_counts(fs, tour_cd)
        if res['faretype'] == 'Not found' and filed == '':
            corrected = list(
                correct_CXtourcode(code['search_tour_cd'], NWORDS))  # a set of possible codes, default:search_tour_cd
            correction_cnt += 1
            if corrected[0] == tour_cd and len(corrected) == 1:
                pass
            else:
                print(tour_cd, 'corrected to:', corrected)

            for candidate in corrected:
                res = get_file_counts(fs, candidate)
                values = [code['tour_cd'], candidate, filed, res['faretype'],
                          (res['file_cnt']), (res['paper_cnt']), (res['filed_cnt'])]
                line = dict(zip(keys, values))
                export_tc.insert_one(line)
                # f.write(line+'\n')
                tcdb.update_one({'_id': code['_id']}, {'$addToSet': {'file_counts': res}})
            continue

        if filed != 'Y' and res[
            'faretype'] == 'filed':  # fare is not in CAT35 but is found in faresheets =>must be PAPER
            res['faretype'] = 'paper'
        res['search_tour_cd'] = tour_cd
        tcdb.update_one({'_id': code['_id']}, {'$addToSet': {'file_counts': res}})
        tcdb.update_one({'_id': code['_id']}, {'$set': {'et_run_id': run_id}})
        values = [code['tour_cd'], code['search_tour_cd'], filed, res['faretype'],
                  str(res['file_cnt']), str(res['paper_cnt']), str(res['filed_cnt'])]
        line = dict(zip(keys, values))
        export_tc.insert_one(line)
        # f.write(line+'\n')

    export_extension_dates_to_db(run_id)
    if correction_cnt > 0:
        print(correction_cnt, 'tourcodes corrected')
    print('Exported to:', filename)
    # write to csv
    export_collection_csv(filename)


def export_extension_dates(run_id):
    t1 = datetime.now()
    tcdb = db['CXtourcodes']
    fs = db['CXfaresheets_new']
    export_id = t1.strftime('%Y%m%H%M')
    ext_dates_filename = 'tourcodes_extension_dates_' + str(run_id) + '_' + export_id + '.csv'
    e = open(ext_dates_filename, "w")

    header2 = ','.join(['tour_cd', 'search_tour_cd', 'extension_date', 'effective_date'])
    e.write(header2 + '\n')
    for code in tcdb.find({'filed': 'Y'}):  # only look at filed fares
        found_fares = fs.find({'tourcodes': code['search_tour_cd'], 'extension_date': {'$exists': True}})
        if found_fares.count():
            print(code['search_tour_cd'], 'found in', found_fares.count(), 'files with extension dates')
        for fare in found_fares:  # only look at sheets with extension dates
            print(fare['extension_date'])
            line = ','.join([code['tour_cd'],
                             code['search_tour_cd'],
                             fare['extension_date'],
                             fare['effective_date']
                             ])
            e.write(line + '\n')
    print('Tourcodes Exported to:', ext_dates_filename)


def export_extension_dates_to_db(run_id):
    t1 = datetime.now()
    tcdb = db['CXtourcodes']
    fs = db['CXfaresheets_new']
    export_id = t1.strftime('%Y%m%H%M')
    ext_dates_filename = 'tourcodes_extension_dates_' + str(run_id) + '_' + export_id
    export_ed = db[ext_dates_filename]
    print('exporting to:', ext_dates_filename)
    # e=open(ext_dates_filename,"w")

    keys = ['tour_cd', 'search_tour_cd', 'extension_date', 'effective_date', 'extension', 'filename']

    for code in tcdb.find({'filed': 'Y'}):  # only look at filed fares
        found_fares = fs.find({'tourcodes': code['search_tour_cd'], 'extension_date': {'$exists': True}})
        if found_fares.count():
            print(code['search_tour_cd'], 'found in', found_fares.count(), 'files with extension dates')
        for fare in found_fares:  # only look at sheets with extension dates
            print(fare['extension_date'])
            values = [code['tour_cd'],
                      code['search_tour_cd'],
                      fare['extension_date'],
                      fare['effective_date'],
                      fare['extension'],
                      fare['filename']
                      ]
            line = dict(zip(keys, values))
            export_ed.insert_one(line)
    print('Extension Dates Exported to:', ext_dates_filename)
    export_collection_csv(ext_dates_filename)


def get_file_counts(collection, tour_cd):
    file_cnt = 0
    paper_cnt = 0
    filed_cnt = 0
    extension_date = {}
    srch = collection.find({'tourcodes': {'$in': [tour_cd]}})
    if srch.count():
        srch_dict = {'tourcodes': {'$in': [tour_cd]}}
    else:
        srch_dict = {"$text": {"$search": tour_cd}}  # to catch BCODES used in Tourcodes
    fare = collection.find_one(srch_dict)
    try:
        faretype = fare['faretype']
        file_cnt = collection.find(srch_dict).count()
        srch_dict['faretype'] = 'paper'
        paper_cnt = collection.find(srch_dict).count()
        srch_dict['faretype'] = 'filed'
        filed_fares = collection.find(srch_dict)
        filed_cnt = filed_fares.count()
        NWORDS[tour_cd] = file_cnt  # rank tourcodes in terms of frequncy of use. can be changed to TKT sales
        # look for faresheet extensions
        if filed_cnt:

            for fare in filed_fares:
                if 'extension_date' in fare.keys():
                    extension_date[str(fare['_id'])] = fare['extension_date']  # record faresheet & extension date


    except KeyError:
        faretype = 'Not found'
    except TypeError:
        faretype = 'Not found'
    return {'faretype': faretype,
            'file_cnt': file_cnt,
            'paper_cnt': paper_cnt,
            'filed_cnt': filed_cnt,
            'extension_date': extension_date
            }


def export_tourcodes_ext_dates(tc_collection, ed_collection):
    tcdb = db[tc_collection]
    found = tcdb.find()
    tcdb_dict = list(found)
    wtr = csv.DictWriter(open(tc_collection + '.csv', 'w'), fieldnames=tcdb_dict[0].keys())
    wtr.writeheader()
    wtr.writerows(tcdb_dict)


# dictwrite
def export_collection_csv(export_collection):
    tcdb = db[export_collection]
    found = tcdb.find()
    tcdb_dict = list(found)
    if found:
        wtr = csv.DictWriter(open(export_collection + '.csv', 'w'), fieldnames=tcdb_dict[0].keys())
        wtr.writeheader()
        wtr.writerows(tcdb_dict)
    else:
        print('EMPTY COLLECTION!', export_collection)


def export_faresheets(run_id):
    t1 = datetime.now()
    # tc = db['CXtourcodes']
    fs = db['CXfaresheets']
    faretype_not_found_cnt = 0
    faretype_found_cnt = 0
    # export_dict={}
    # row_dict={}
    filename = 'faresheets_' + str(run_id) + '.txt'
    total = fs.find().count()
    cnt = 0
    f = open(filename, "w")
    # with open(filename,"wb") as csv_file:
    # writer = csv.writer(csv_file, delimiter=',')
    line = '\t'.join(['filename', 'filepath', 'faretype', 'tourcodes'])
    f.write(line + '\n')
    for fare in fs.find({}, {'filename': 1, 'filepath': 1, 'faretype': 1, 'tourcodes': 1}).limit(10):
        filename = fare['filename']
        print('filename:', filename)
        filepath = fare['filepath']
        faretype = fare['faretype']
        # filed=code['filed']
        tourcodes = fare['tourcodes']

        try:
            faretype = fare['faretype']
            # file_cnt=fs.find({'tourcodes':{'$in': [tour_cd]}} ).count()
            faretype_found_cnt += 1
        except KeyError:
            faretype = 'Not found'
            faretype_not_found_cnt += 1
        except TypeError:
            faretype = 'Not found'
            faretype_not_found_cnt += 1

        # export_dict['faretype']=faretype
        # export_dict['file_cnt']=file_cnt

        # row=f'{tour_cd:<15} {filed:<4} {faretype:<20} {file_cnt:<5} files'
        # line=f'{tour_cd:<15} ,{filed:<4} ,{faretype:<20}, {file_cnt:<5}/n'
        tc_line = [str(tc) for tc in tourcodes]  # convert to string
        print(tc_line)
        print(filename, filepath, faretype, tourcodes)
        row = [filename, filepath, faretype, tourcodes].append(tc_line)
        print(row)
        line = '\t'.join([filename, filepath, faretype])
        line2 = '\t'.join(tc_line)
        print(line + ',' + line2)
        f.write(line + '\t' + line2 + '\n')
        cnt += 1
        if (cnt % 1000) == 0:
            t3 = datetime.now()
            cum = (t3 - t1).total_seconds()
            rem = ((total - cnt) * cum / cnt) / 60
            etf = (t3 + timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat = f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)
        # line=(tour_cd,filed,faretype,file_cnt)
        # writer.writerow(line)
    print(faretype_found_cnt, 'files had faretypes ', faretype_not_found_cnt, 'did not')
    print('Exported to:', filename)


