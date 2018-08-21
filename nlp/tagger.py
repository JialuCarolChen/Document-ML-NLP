from datetime import datetime, date, timedelta
from functools import reduce
import calendar
import re, itertools
from pymongo import MongoClient
from collections import namedtuple
from collections import Counter

import nltk.classify.util
from nltk.corpus import stopwords
import nltk.tag
from nltk.corpus import treebank
from nltk.tree import Tree
from nltk.chunk import RegexpParser
from nltk.tag import UnigramTagger
from nltk.tag import DefaultTagger
from nltk.tag import RegexpTagger

from nlp.chunker import sub_leaves
from nlp.chunker import LocationChunker
from nlp.transforms import tag_startswith, first_chunk_index, tag_equals, flatten_deeptree
#from raxutil.nlp.tools import *
from nlp.spellchecker import *

client=MongoClient('localhost:27017')
db = client.raxdb

Commission = namedtuple('Commission', ['value', 'unit'])
TaggedWord = namedtuple('TaggedWord', ['token', 'tag'])
TOURCODE_REGEX = r"[\d+|\w+]+FF\d+"
TC_fmt = re.compile(TOURCODE_REGEX)

#db.carriers.insert_one({'KA':'CARRIER'})
# get carriers information
carriers = db['carriers']
carrs = carriers.find()
CARRIER_MODEL = {}

for carr in carrs:
    ID, mod = carr
    CARRIER_MODEL[mod] = 'CARRIER'

rsystem=db['route_system']  #dictionary for tracking all system / network destinations by carrier code
# rsystem.insert_one({'carrier':'KA','SYSTEM':'ALL CX AND KA'})
# rsystem.insert_one({'carrier':'CX','SYSTEM':'ALL CX AND KA'})
# rsystem.insert_one({'carrier':'SYSTEM','SYSTEM':'ALL CX AND KA'})
# location_to_clean = {
# 'CX AND KA': 'ALL CX AND KA',
# 'CX AND KA SYSTEM': 'ALL CX AND KA',
# 'SYSTEM': 'ALL CX AND KA',
# 'FROM': None
# }

fs = db['CXfaresheets_new']
# fs=db['CXfaresheets']
# carriers = db.carriers
# carrs = carriers.find(  )
#
# for carr in carrs:
#     ID, mod = carr
#     CARRIER_MODEL[mod] = 'CARRIER'

TOURCODES = []
for x in db.CXtourcodes_master.find():
    TOURCODES += [x['tour_cd']]

TOURCODES = set(TOURCODES)
# DATEDEBUG=True
# DEBUG=True

DATEDEBUG = False
DEBUG = False

#############################################
#           TEXT TAGGING                    #
#############################################


patterns = [(r'^\d+$', 'CD'),
            (r'年', 'YEAR'),
            (r'月', 'MONTH'),
            (r'日', 'DAY'),
            (r'最', '最'),
            (r'后', '后'),
            (r'新', '新'),
            (r'更', '更'),
            (r'销售开票', 'SALES_PERIOD'),
            (r'销', '销'),
            (r'售', '售'),
            (r'开', '开'),
            (r'行程始发', 'TRAVEL_PERIOD'),
            (r'RTW', 'RTW_FARE'),
            (r'CTP', 'CIRCLE_TRIP_FARE'),
            #            (r'ticket|tkt','TICKET'),
            (r'SYSTEM', 'SYSTEM'),
            ((r'UPDATED|ISSUED|ISSUE|issue|ISS|iss|ISSD|DTD|issued|dtd', 'ISSUED')),
            ((r'sales|selling|Sales|期', 'SALES')),
            ((r'ticket|TICKET|tkt|ticketing|Ticketing|TICKETING', 'TICKETING')),
            ((r'TRVL|TRAVL|TRAVEL|travel|dep|票', 'TRAVEL')),
            ((r'EFF|EEF|AFTER|after|Effective|effective|eff|eef|Effectove|Wef|wef|wf', 'EFFECTIVE')),
            ((r'VALID|Valid|VALIDITY|validity|valid|限|开票时限', 'VALIDITY')),
            ((r'DAYS', 'NOTICE')),
            ((r'FROM', 'FROM')),
            ((r'TOUR|Tour|DEAL|Deal|TOURCODE|Tourcode|DEALCODE|Dealcode', 'TOUR')),
            ((r'CODE|Code|code', 'CODE')),
            ((r'&|AND', 'AND')),
            ((r'TO|UNTIL|until|till|–|upto|up to|至|:|/', 'TO')),
            ((r'\"', 'QT')),
            ((r'FOR', 'CCONJ')),
            ((r'EXPIRY|TILL', 'END')),
            ((r'CAT|Cat', 'CAT')),
            ((r'AIRPORT|CITY|COUNTRY|AREA|REGION|MARKET', 'LOCATIONTYPE')),
            ((r'CURRENCY|CURENCY|CURR', 'CURRENCY')),
            ((r'SUPERCEDES|SUPERSEDES|SUPERSEDED', 'NEGATIVE')),
            ((r'DISP', 'DISP')),
            ((r'UFN|ONWARDS|further', 'UFN')),
            ((r'extended|延|宽', 'EXTENDED')),
            ((r'Previous|previous|一', 'PREVIOUS')),
            ((r'version|vers|版|v', 'VERSION')),
            ((r'honored|honoured|honour', 'HONORED')),
            ((r'CATHAY', 'CATHAY')),
            ((r'PACIFIC', 'PACIFIC')),
            ((r'AIRWAYS', 'AIRWAYS')),
            ((r'AGGREEMENT', 'AGGREEMENT')),
            ((r"[\d+|\w+]+FF\d+", 'CX_TOURCODE')),  # formart for CX tourcodes
            ((r"[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+|[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+", 'CX_TOURCODE')),
            ('ROUTE', 'ROUTE'),
            ('CABIN', 'CABIN'),
            ('BOOKING', 'BOOKING'),
            ('CLASS', 'CLASS'),
            (r'Basis|BASIS', 'BASIS'),
            (r'FARE|FARES|Fares|Fare', 'FARE'),
            ('FBC', 'FARE_BASIS'),
            (r'ROUND|Round', 'ROUND'),
            (r'World|WORLD', 'WORLD'),
            (r'Circle|CIRCLE', 'CIRCLE'),
            (r'Trip|TRIP', 'TRIP'),
            ('BASIS', 'BASIS'),
            ('CORPORATE', 'CORPORATE'),
            ('OTHER|other', 'OTHER'),
            ('LAST|last', 'LAST'),
            (r'POS', 'POINT_OF_SALE'),
            (r'AFTERCOMPANY|CORPORATE|COMPANY', 'CORPORATE'),
            (r'DISCOUNT|COMMISSIO|COMMISSIONS|COMISSION|discount|discounts|Commissionable|commissionable', 'DISCOUNT'),
            (r'ASIA|Asia|asia|AISA|亚洲', 'ASIA'),
            (r'NORTH|North|north', 'NORTH'),
            (r'SOUTH|South|south', 'SOUTH')

            #            ('TICKET','VALIDITY')
            ]  # add learning loop here for tags

def_tagger = DefaultTagger('NN')
prelim_def_tagger = DefaultTagger(None)

backoff = RegexpTagger([
    (r'^-?[0-9]+(.[0-9]+)?$', 'CD'),  # cardinal numbers
    (r'(The|the|A|a|An|an)$', 'AT'),  # articles
    (r'.*able$', 'JJ'),  # adjectives
    (r'.*ness$', 'NN'),  # nouns formed from adjectives
    (r'.*ly$', 'RB'),  # adverbs
    (r'.*s$', 'NNS'),  # plural nouns
    (r'.*ing$', 'VBG'),  # gerunds
    (r'.*ed$', 'VBD'),  # past tense verbs
    (r'is|was|are|were', 'VBZ'),  # verb to be
    (r'"', 'QT'),  # quote
    (r'.*', 'NN')  # nouns (default)

], backoff=def_tagger)
cal2={v.upper(): k for k,v in enumerate(calendar.month_abbr)}
cal2.update({v: k for k,v in enumerate(calendar.month_abbr)})
cal2.update({v.upper(): k for k,v in enumerate(calendar.month_name)})
cal2.update({v: k for k,v in enumerate(calendar.month_name)})
del cal2[""] #remove blank string keyupdat
monthModel = {}
monthModel = {k: 'MM' for k, v in cal2.items()}

# monthModel={v.upper(): 'MM' for k,v in enumerate(calendar.month_abbr)}
# monthModel.update({v.title(): 'MM' for k,v in enumerate(calendar.month_abbr)})
# monthModel.update({v.upper(): 'MM' for k,v in enumerate(calendar.month_name)})
# monthModel.update({v: 'MM' for k,v in enumerate(calendar.month_name)})
# monthModel.update({v: 'YYYY' for k,v in enumerate([2016,2017,2018,2019,2020,2021,2022])})

# del monthModel['']

airportcode = {}
# with open ('./raxutil/data/IATA-airport-city-ref.csv') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         airportcode[row['airpt_cd']] ='AIRPORT'
#         airportcode[row['airpt_name']] ='AIRPORT'
#         airportcode[row['city_cd']] ='CITY'


codeshareModel = {}
for item in open('./raxutil/data/codeshare.txt'):
    codeshareModel[item.strip()]='CODESHARE'

currencycode = {}
currencycountry = {}
country = {}
# with open ('./raxutil/data/currency.csv',mode="r", encoding="utf-8") as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         currencycode[row['code']] ='CURRENCYCODE'
#         currencycountry[row['code']]=row['Country']
#         currencycountry[row['Country']]=row['code']
# country[row['Country']]='COUNTRY'
# now coming from raxdb
for row in db.currency.find():
    currencycode[row['currencycode']] = 'CURRENCYCODE'
    currencycountry[row['currencycode']] = row['country']
    currencycountry[row['country']] = row['currencycode']

for row in db.airport_city_codes.find():
    airportcode[row['airpt_cd']] = 'AIRPORT'
    airportcode[row['airpt_name']] = 'AIRPORT'
    airportcode[row['city_cd']] = 'CITY'

for row in db.codeshare_words.find():
    codeshareModel[row['word'].strip()] = 'CODESHARE'

TO_MODEL = {}
TO_MODEL['-'] = 'TO'
TO_MODEL['至'] = 'TO'
# ABOVE THREE CAN BE COLLAPSED INTO A GENERIC DATABASE TABLE FOR UNIGRAM TAGGERS
CURRENCY = set(currencycode.keys())
known_tourcodes = {tc: 'TC' for tc in TOURCODES}  # setup tourcodes model

train_sents = treebank.tagged_sents()[:3000]
unigramtagger = UnigramTagger(train_sents, backoff=backoff)
currencytagger = UnigramTagger(model=currencycode, backoff=unigramtagger)  # tag currency
airporttagger = UnigramTagger(model=airportcode, backoff=currencytagger)  # tag airports
codesharetagger = UnigramTagger(model=codeshareModel, backoff=airporttagger)  # tag codeshare
carriertagger = UnigramTagger(model=CARRIER_MODEL, backoff=codesharetagger)  # tag carriers

datetagger = UnigramTagger(model=monthModel, backoff=carriertagger)  # tag months
rtagger = RegexpTagger(patterns, backoff=datetagger)
known_tourcodes_tagger = UnigramTagger(model=known_tourcodes, backoff=rtagger)

dashtagger = UnigramTagger(model=TO_MODEL, backoff=known_tourcodes_tagger)
#

simpletagger = UnigramTagger(train_sents, backoff=backoff)
currencytagger2 = UnigramTagger(model=currencycode, backoff=simpletagger)
# countrytagger=UnigramTagger(country, backoff= rtagger2 )
rtagger2 = RegexpTagger(patterns, backoff=currencytagger2)

cat_tagger = RegexpTagger(patterns, backoff=def_tagger)

farechunker = RegexpParser(r'''

CARRIER:
{<CODESHARE><CODESHARE><CODESHARE><CODESHARE>}
{<CODESHARE><CODESHARE><CODESHARE>}
{<CODESHARE><CODESHARE>}
{<CODESHARE><NN>}



ROUTE:
{<ROUTE>}

CABIN:
{<CABIN>}

RBD:
{<BOOKING><CLASS>}


CORPORATE_DISCOUNT:
{<CORPORATE><DISCOUNT>}
{<EFFECTIVE><DISCOUNT>}

AGENT_DISCOUNT:
{<DISCOUNT>}



FBC:
{<FBC><VBD><TO><VBP><DISCOUNT>}
{<FARE><BASIS>}


TICKET_VALIDITY:
{<TICKET><VALIDITY>}


LOCATION:
{<LOCATIONTYPE><NN><.*>}

AIRLINE:
{<CAT><PACIFIC><AIRWAYS><CITY>}

CLIENT:
<AIRLINE>{<.*><.*>}<TOURCODE>



''')


mychunker = RegexpParser(r'''

CARRIER:
{<CODESHARE><CODESHARE><CODESHARE><CODESHARE>}
{<CODESHARE><CODESHARE><CODESHARE>}
{<CODESHARE><CODESHARE>}
{<CODESHARE><NN>}

SYSTEM:
{<CARRIER><AND><CARRIER><SYSTEM>}
{<CARRIER><AND><CARRIER>}

FLIGHT:
{<CARRIER><CD>}

DISP:
{<DISP><CD><NN><CD>}
{<DISP><CD>}



TICKET_VALIDITY:
{<TICKET><VALIDITY>}



DMYDATE:
{<CD>< NN ><MM><CD>}
{<CD>< MM ><CD>}
{<CD><NN><CD>}
{<DATE><CD>}
{<MM><CD><CD>}

YMDDATE:
{<CD><TO><CD><TO><CD>}
<TO>{<CD><CD><CD>}
{<CD><YEAR><CD><MONTH><CD><DAY>}

DDATE:
<EFFECTIVE>{<CD>}<TO>

DMDATE:
{<CD><MM>}
{< MM ><CD>}

DATE:
{<DMYDATE>}
{<DMDATE>}
{<YMDDATE>}

ISSUE:
{<最><后><更><新><DAY>}

SALES_PERIOD:
{<销><售><开><TRAVEL>}




FROMDATE:
{<DATE>}<TO><DATE>
{<DATE>}<TO><UFN>
<FROM>{<DATE>}
{<DATE>}<IN>
<EFFECTIVE>{<DATE>}
<EFFECTIVE>{<DDATE>}
<EFFECTIVE><.*>{<DATE>}
<SALES>{<DATE>}
<TRAVEL>{<DATE>}
<TOURCODE>{<DATE>}





TODATE:
<DATE><TO>{<DATE>}
<FROMDATE><TO>{<DATE>}
<TO>{<DATE>}
<SALES><TO>{<DATE>}
<TRAVEL><TO>{<DATE>}
<FROMDATE>{<DATE>}
<LAST><.*>{<DATE>}
<IN>{<DATE>}
<JJS>{<DATE>}
{<UFN>}



LOCATION:
{<LOCATIONTYPE><NN><.*>}
{<CITY>}
{<AIRPORT>}


CURRENCYCHUNK:
{<CURRENCY><CURRENCYCODE>}
{<CURRENCY><.*>*<CURRENCYCODE>}
{<CURRENCY><AIRPORT>}
{<CURRENCY><CITY>}

TOURCODE:
{<TOUR><NN><NN>}

TOURCODE:
{<TOUR><CODE><TC>}
<TOUR><CODE><NN><NN*>{<NN>}
<TOUR><CODE><TO><NN*>{<NN>}
<TOUR><CODE><.*><.*>{<NN>}
<TOUR><CODE>{<NN>}
{<NN>}<CCONJ><LOCATION>
{<TC>}

TOURCODELOCATION:
<TOURCODE><.*>*<AND>{<NN><CCONJ><LOCATION>}
{<TOURCODE><CCONJ><LOCATION>}
{<TOURCODE><LOCATION>}
{<TC><LOCATION>}

CATEGORY:
{<CAT><CD><.*>*}<CAT>
{<CAT><CD><.*>*}

AIRLINE:
{<CAT><PACIFIC><AIRWAYS><CITY>}

CLIENT:
<AIRLINE>{<.*><.*>}<TOURCODE>

RTW_FARE:
{<ROUND><DT><WORLD>}
{<RTW_FARE>}


CIRCLE_TRIP_FARE:
{<CIRCLE><NN>}
{<CIRCLE><TRIP>}
{<CIRCLE_TRIP_FARE>}

CIRCLE_TRIP_FARE_NO_COMMISSION:
{<CIRCLE_TRIP_FARE><.*>*<RB><DISCOUNT>}
{<CIRCLE_TRIP_FARE><.*>*<RB><.*>*<DISCOUNT>}

RTW_FARE_NO_COMMISSION:
{<RTW_FARE><CC><CIRCLE_TRIP_FARE_NO_COMMISSION>}

POS:
{<POINT_OF_SALE><LOCATION><TO><LOCATION><TO><LOCATION>}
{<POINT_OF_SALE><LOCATION><TO><LOCATION>}
{<POINT_OF_SALE><LOCATION>}

''')

rangechunker = RegexpParser(r'''

RANGE:

{<FROMDATE><TO><TODATE>}
{<FROMDATE><TODATE>}
{<FROMDATE><TO><TODATE>}<FROMDATE>
<SALES_PERIOD><.*>*{<FROMDATE><TO><TODATE>}<FROMDATE>
<VALIDITY><.*>*{<FROMDATE><TO><TODATE>}
{<FROMDATE><NN><TODATE>}
{<FROMDATE><IN><TODATE>}
{<FROMDATE><.*>*<TODATE>}
{<FROMDATE><.*><TODATE>}
{<FROMDATE><TODATE>}
{<DATE><TO><DATE>}
{<FROMDATE><TO><DATE>}
{<NN><DATE><UFN>}

DEFAULT_RANGE:
{<IN><NN><EFFECTIVE><RANGE>}


''')

# RANGE:
# {<FROMDATE><TO><TODATE>}
# {<FROMDATE><NN><TODATE>}
# {<FROMDATE><IN><TODATE>}
# {<FROMDATE><.*>*<TODATE>}
# {<FROMDATE><.*><TODATE>}
# {<DATE><TO><DATE>}
# {<FROMDATE><TO><DATE>}
# {<NN><DATE><UFN>}

periodchunker = RegexpParser(r'''





SALESPERIOD:
{<SALES><NN><DATE>}
{<SALES><CD><DATE>}
{<SALES><TO><DATE>}
{<SALES><TO><TODATE>}
{<SALES><RP><TO><TODATE>}
{<SALES><TO><RANGE>}
{<SALES><DATE>}
{<SALES><NN><RANGE>}
{<SALES><RANGE>}
{<SALES><.*>*<DATE>}
{<SALES><.*>*<RANGE>}
{<SALES><TO><TODATE>}
{<SALES><FROMDATE><TO><TODATE>}
{<SALES_PERIOD><FROMDATE><TO><TODATE>}
{<SALES><FROMDATE><END><TODATE>}
{<SALES><END><TODATE>}
{<SALES><FROMDATE>}
{<EFFECTIVE><TO><RANGE>}
{<EFFECTIVE><RANGE>}
{<EFFECTIVE><.*>*<DATE>}
{<EFFECTIVE><.*>*<TODATE>}
{<EFFECTIVE><.*>*<TO><TODATE>}<TRAVEL>
{<RANGE>}<RANGE>
<SALES_PERIOD><TRAVEL_PERIOD><VALIDITY><.*>*<NN>{<RANGE>}<RANGE>
<SALES_PERIOD><TRAVEL_PERIOD><.*>*<VALIDITY><CITY>{<FROMDATE><TO><TODATE>}<FROMDATE><TO><TODATE>
{<SALES_PERIOD><RANGE>}
}<TOURCODE>{


SALESSUPERCEDES:
{<NEGATIVE><.*>*<SALESPERIOD>}

TRAVELPERIOD:
{<TRAVEL><NN><DATE>}
{<TRAVEL><TO><DATE>}
{<TRAVEL><TO><TODATE>}
{<TRAVEL><EFFECTIVE><RANGE>}
{<TRAVEL><EFFECTIVE><DATE>}
{<TRAVEL><EFFECTIVE><FROMDATE>}
{<TRAVEL><DATE>}
{<TRAVEL><NN><RANGE>}
{<TRAVEL><IN><RANGE>}
{<TRAVEL><RANGE>}


{<TRAVEL><FROMDATE><TO><TODATE>}
{<TRAVEL><.*>*<FROMDATE><TO><TODATE>}
{<TRAVEL><.*>*<FROMDATE><TODATE>}
{<TRAVEL><FROMDATE><END><TODATE>}
{<TRAVEL><TO><DATE>}
{<TRAVEL><END><TODATE>}
{<TRAVEL><FROMDATE>}
{<VALIDITY><CONJ><TRAVEL> }
<VALIDITY><.*>*<TRAVEL><.*>*{<RANGE>}
<SALESPERIOD>{<RANGE>}
{<TRAVEL><NN><SALESPERIOD>}
<DATE>{<TRAVEL><TO><DATE>}
<SALES_PERIOD><TRAVEL_PERIOD><VALIDITY><.*>*<SALESPERIOD>{<RANGE>}
}<TOURCODE>{

ISSUE_DATE:
<ISSUE>{<SALESPERIOD>}

DISCONTINUE:
{<LAST>}<.*>*<TODATE>
{<LAST>}<.*>*<DATE>
{<MD><VB><ISSUED><IN>}<TODATE>
{<MD><VB><NN><IN>}<TODATE>

TKT_DIS_DATE:
{<TICKETING><DISCONTINUE><TODATE>}
{<DISCONTINUE><TICKETING><TODATE>}

TRV_DIS_DATE:
{<TRAVEL><DISCONTINUE><TODATE>}
{<DISCONTINUE><TRAVEL><TODATE>}


''')

# {<TRAVEL><.*>*<RANGE>}
# {<TRAVEL><.*>*<DATE>}
# original version
extensionchunker = RegexpParser(r'''
EXTENSION_DATE:

{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}<ISSUE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<DATE>}<DATE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<DATE>}<ISSUE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}<DATE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<ISSUE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<FROMDATE>}<TODATE>
{<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<ISSUE>
{<PREVIOUS><VERSION><NN><.*>*<TRAVEL><.*>*<SALES><VALIDITY><.*>*<DATE>}<ISSUE>

{<PREVIOUS><VERSION><.*>*<TRAVEL><DAY><SALES><.*>*<DATE>}<ISSUE>
{<PREVIOUS><VERSION><.*>*<EXTENDED><VALIDITY><SALES><.*>*<TODATE>}<ISSUE>
{<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<ISSUE>
{<HONORED><.*><TICKET><SALES><.*>*<TODATE>}
{<HONORED><.*>*<.*>*<TODATE>}
{<PREVIOUS><VERSION><.*>*<TICKET><.*>*<JJS><TODATE>}
{<PREVIOUS><VERSION><.*>*<TICKETING><.*>*<JJS><TODATE>}

{<PREVIOUS><VERSION><VALIDITY><IN><SALES><TO><TODATE>}
{<PREVIOUS><VERSION><.*>*<MD><VB><ISSUED><IN><TODATE>}


}<SALES_PERIOD><.*>*{
''')

originchunker = RegexpParser(r'''


FROM_LOCATION:

{<LOCATION>}<LOCATION><TO>
{<LOCATION>}<TO>

{<LOCATION>}<LOCATION><AND>
<AND>{<LOCATION>}<LOCATION>
{<CITY>}<TO>
<VBG><IN>{<CITY>}
{<AIRPORT>}<TO>

FROM_LOCATION:
{<LOCATION>}<FROM_LOCATION>
FROM_LOCATION:
{<LOCATION>}<FROM_LOCATION>
FROM_LOCATION:
{<LOCATION>}<FROM_LOCATION>

TO_LOCATION:
<TO>{<LOCATION>}
<TO_LOCATION>{<LOCATION>}
<FROM_LOCATION>{<LOCATION>}<AND>
<AND><FROM_LOCATION>{<LOCATION>}
<TO>{<CITY>}
<TO>{<AIRPORT>}
<TO>{<ASIA>}

TO_LOCATION:
<TO_LOCATION>{<LOCATION>}

TO_LOCATION:
<TO_LOCATION>{<LOCATION>}
TO_LOCATION:
<TO_LOCATION>{<LOCATION>}
TO_LOCATION:
<TO_LOCATION>{<LOCATION>}
TO_LOCATION:
<TO_LOCATION>{<LOCATION>}

ASIA_DEFIN:
<NN>{<ASIA><.*>*<VBP>}



ORIGIN:
{<FROM_LOCATION><TO><TO_LOCATION>}


ORIGINS:
{<ORIGIN><CC><LOCATION>}
{<ORIGIN><TO><TO_LOCATION>}
{<FROM_LOCATION><TO><ORIGIN>}
<VBG><IN>{<LOCATION>}
<VBG><IN>{<FROM_LOCATION>}



ORIGINATING:
{<VBG><ORIGINS>}
{<VBG><IN><ORIGINS>}
{<ORIGINS><VBG><NN>}


EXCLUDE:
{<OTHER>}
{<RB><VB><IN>}



ORIGINS_EXCLUDED:
{<VBG><EXCLUDE><ORIGINS>}
{<VBG><EXCLUDE><LOCATION>}

FLIGHT:
{<CARRIER><CD>}

FLIGHTS:
{<FLIGHT>(<TO><CD>)*}

FLIGHTS_EXCLUDED:
<DISCOUNT><MD>{<EXCLUDE><FLIGHTS>}

''')


# flightchunker=RegexpParser(r'''
# FLIGHTS_EXCLUDED:
# {<EXCLUDE><FLIGHTS>}

# ''')

def normalise_country(cntry):
    found = db.ISO_country_codes.find_one({'alias':cntry.upper()})
    if found:  # all good
        return found['ISO ALPHA-2 Code']
    else:
        return cntry

def find_origins(tagged):
    # change output format
    origins = _find_origins(tagged)
    #print(origins )
    res = []
    if origins == {}:
        return []

    if 'except' in origins.keys():  # these are excluded origins, taged with
        if origins['except']:
            res = [('-' + normalise_country(cntry)) for cntry in origins['origins']]
        else:  # these are included flights, tagged with plus
            res = [('+' + normalise_country(cntry)) for cntry in origins['origins']]
    return res


def _find_origins(tagged):
    exclude = find_subtrees(tagged, 'ORIGINS_EXCLUDED')
    if exclude:
        locs = find_locs(exclude)
        # print('ORIGINS EXCLUDING:',locs)
        return {'origins': locs, 'except': True}

    origin_chunk = find_subtrees(tagged, 'ORIGINATING')
    if origin_chunk:
        locs = find_locs(origin_chunk)
        # print('ORIGINATING FROM:',locs)
        return {'origins': locs, 'except': False}
    return {}


def find_locs(origin_chunk):
    # will return the list of LOCATIONs in this chunk
    locs = []
    for org in origin_chunk:
        s1 = sub_leaves(org, 'LOCATION')
        if s1 == []:
            s1 = sub_leaves(org, 'FROM_LOCATION')
        for tagged_tup in s1:
            # print(tagged_tup)
            tags = map(lambda x: x[0], tagged_tup)
            origin = reduce((lambda x, y: x + ' ' + y), tags).strip()
            locs += [origin]
    # check for spelling errors and short forms
    check_str = '/'.join(locs)

    # if locs are made up of countries and regions, then standardize to ISO Alpha_2 code
    cntry_cnt = 0
    cntry_locs=[]
    # for loc in locs:
    #     cntry = db.ISO_country_codes.find_one({'alias': loc})
    #     if cntry:
    #         cntry_cnt += 1
    #         cntry_locs+=[cntry['ISO ALPHA-2 Code']]
    #     else:
    #         cntry_locs+= [loc]
    # if cntry_cnt*2>len(locs): #majority countries
    #     return cntry_locs
    # else:
    #     return locs
    return locs

def get_location(route, tag):
    # tagged_tup_list=sub_leaves(myTagger(route),tag)
    route=pre_process_route(route)
    chunked_route=myTagger(route)
    tagged_tup_list = sub_leaves(chunked_route, tag)
    #print(tagged_tup_list)
    #locs = [ normalise_country(y[0]) for x in tagged_tup_list for y in x] # use normalise_country to standardise where possible
    locs = [y[0] for x in tagged_tup_list for y in x]
    #print('locs',locs)
    if locs:
        #locs=[correct2(x,speller) for x in locs] #check spelling for locations
        return find_ngram_locations(locs)
    else:
        #print('IN SYSTEM')
        carriers=[find_leaves(chunk, 'CARRIER') for chunk in find_subtrees(chunked_route, 'SYSTEM')]
        sys=find_leaves(chunked_route, 'SYSTEM') #check if the word SYSTEM is there
        #print('carriers',carriers,'sys',sys )
        if not carriers and sys:
            carriers=[['SYSTEM']]

        if carriers:
            #print('carrier',carriers[0][0])
            route_system = rsystem.find_one({'carrier': carriers[0][0]})['SYSTEM']
            #print('route_system',route_system)
            return   route_system# eg 'CX AND KA SYSTEM'
        else:
            return None
# location_to_clean = {
# 'CX AND KA': 'ALL CX AND KA',
# 'CX AND KA SYSTEM': 'ALL CX AND KA',
# 'SYSTEM': 'ALL CX AND KA',
# 'FROM': None
# }
rsys=set([list(x.values())[1] for x in list(rsystem.find())]) #list of system routes
def _get_from_location(route):
    return get_location(route, 'FROM_LOCATION')

def _get_to_location(route):
    if route in rsys:
        return route
    else:
        return get_location(route, 'TO_LOCATION')

def get_from_location(route):
    t_loc=_get_from_location(route)
    #print('t_loc',t_loc)
    if not t_loc:
        # check if TO_LOCATION exists
        f_loc=_get_to_location(route)
        if f_loc:
            return None
        else:
            l_loc= get_location(route, 'LOCATION') # need to distinguish OR as city vs conjunction intrinsically
            l_loc=[ x for x in l_loc if len(x)>2 ] # eg ['SHA', 'OR', 'HKG'] >> ['SHA', 'HKG']
            return l_loc

    if 'ALL' in t_loc:  #SYSTEM route
        return t_loc
    if t_loc:
        l_loc = [x for x in t_loc if len(x) > 2]  # eg ['SHA', 'OR', 'HKG'] >> ['SHA', 'HKG']
        return l_loc
    return None



def get_to_location(route):
    t_loc=_get_to_location(route)
    if not t_loc:
        # check if FROM_LOCATION exists
        f_loc=_get_from_location(route)
        if f_loc:
            return None
        else:
            l_loc= get_location(route, 'LOCATION') # need to distinguish OR as city vs conjunction intrinsically
            l_loc=[ x for x in l_loc if len(x)>2 ] # eg ['SHA', 'OR', 'HKG'] >> ['SHA', 'HKG']
            return l_loc

    if 'ALL' in t_loc:  #SYSTEM route
        return t_loc

    if t_loc:
        l_loc = [x for x in t_loc if len(x) > 2]  # eg ['SHA', 'OR', 'HKG'] >> ['SHA', 'HKG']
        return l_loc
    return None



def pre_process_route(route):
    # pre-processing route strings
    #fix TO,OR,AND joined with space missing
    #case example: 'MEXICO ANDLATIN AMERICA' >> 'MEXICO AND LATIN AMERICA'
    #case example: 'TOWUH' >> 'TO WUH'
    connectors=['TO','OR','AND']
    sent_tokens = flattern(preprocessLines2(route))
    spaced_tokens=[]
    for tok in sent_tokens:
        next_tok = []
        for conn in connectors:
            if (conn == tok[0:len(conn)]) and (len(tok) >=len(conn)+3):  #space missing -> split them
                spaced_tokens += [conn]
                next_tok= [tok[len(conn)-len(tok):]]
                spaced_tokens += next_tok
        if not(next_tok): # correction has not been made
            spaced_tokens += [tok]
    corrected = [correct2(x, NWORDS) if x != '-' else x for x in spaced_tokens]  # check spelling for locations
    if len(corrected)==2 and corrected[1]=='ASIA':  # eg USA ASIA
        corrected.insert(1, '-') # change to ['USA', '-', 'ASIA']

    if corrected[1] == 'A':  #need to find out why '-' is not being handled correctly
        corrected[1] = '-'

    rt = ' '.join(corrected)
    return rt


def find_ngram_locations(lst):
    #this is a post processing fix, should get Location Chunker to work correctly in the first place
    if len(lst)<=1:
        return lst
    last = []
    ngram_locations = set(['LATIN AMERICA', 'HONG KONG', 'NORTH ASIA'])

    for idx, x in enumerate(lst):
        no_bigram = True
        for y in ngram_locations:
            if x in y:
                if idx + 1 < len(lst):
                    if lst[idx + 1] in y:
                        # ngram_counter[y]+=1
                        last += [y]
                        no_bigram = False
                else:
                    no_bigram = False
        if no_bigram:
            last += [x]
    locs = list(set(last))  # case  'BOS SIN AND BOS SHA': ('BOS', ['SIN', 'SHA']) - should be single BOS
    return locs

def find_POS(chunk):
    ST = find_subtrees(chunk, 'POS')
    if ST:
        locs = find_locs(ST)
        qualified = ['+' + normalise_country(loc) for loc in locs] # change POS to standardized codes

        return [qualified]
    else:
        return [[]]


# def FareTagger(rawtextlist):
#     DEBUG = False
#     DEBUG2 = False
#     DEBUG3 = False
#     loc = LocationChunker()
#
#     sentences = preprocessLines2(rawtextlist)
#     # print(sentences)
#     chunked = []
#     for sentence in sentences:
#         if sentence:
#             pos_text = dashtagger.tag(sentence)
#             loc_chunked = loc.parse(pos_text)
#             origin_chunked = originchunker.parse(loc_chunked)
#             # flight_chunked=flightchunker.parse(origin_chunked)
#             chunked += [farechunker.parse(origin_chunked)]
#
#     return Tree('S', chunked)


def myTagger(rawtextlist,DEBUG=False):
    #DEBUG = True
    DEBUG2 = DEBUG
    DEBUG3 = DEBUG
    loc = LocationChunker()
    sentences = preprocessLines2(rawtextlist)
    # print(sentences)
    chunked = []
    for sentence in sentences:
        if sentence:
            pos_text = dashtagger.tag(sentence)
            loc_chunked = loc.parse(pos_text)
            mychunk = mychunker.parse(loc_chunked)
            ###
            origin_chunked = originchunker.parse(mychunk)
            # flight_chunked=flightchunker.parse(origin_chunked)

            if DEBUG:
                print('after mychunk', origin_chunked)
            extensions = extensionchunker.parse(origin_chunked)  ####
            if DEBUG2:
                print('after extension chunker', extensions)
            ranges = rangechunker.parse(extensions)
            if DEBUG3:
                print('ranges chunker', ranges)

            periodchunk = periodchunker.parse(ranges)

            # ranges=rangechunker.parse(mychunk)
            # if DEBUG2:
            #     print('ranges chunker',ranges)
            # extensions=extensionchunker.parse(ranges)
            # if DEBUG:
            #     print('extension chunker',extensions)
            # periodchunk=periodchunker.parse(extensions)

            if DEBUG:
                print('periodchunk chunker', periodchunk)

            chunked += [farechunker.parse(periodchunk)]
            # chunked+=[periodchunk]
            if DEBUG:
                print(chunked)
        else:
            break
    return Tree('S', chunked)



def is_tourcode(word):
    if (word in TOURCODES or TC_fmt.match(word)):
        return word
    else:
        return None


def preprocessLines2(line):
    # print('- fname', fname)
    stoplist = set(stopwords.words("english"))
    stoplist.discard('no')  # remove 'no' from stop list
    stoplist.discard('not')  # remove 'no' from stop list
    stoplist.discard('other')  # remove 'no' from stop list
    stoplist.discard('to')  # remove 'to' from stop list
    stoplist.discard('and')  # remove 'and' from stop list
    stoplist.discard('or')  # remove 'or' from stop list
    stoplist.discard('can')  # remove 'and' from stop list
    stoplist.add('apply')
    stoplist.add('applicable')
    stoplist.add('applies')
    stoplist.add('payable')
    stoplist.add('applied')
    stoplist.add('HTML')
    STOPLIST = [x.upper() for x in stoplist]
    percentage = re.compile('\d+%')
    ddmon = re.compile('\d+[JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC]')
    sentence_end = re.compile(r'\w*[:|,|"|&|\.]')
    translation_table = dict.fromkeys(map(ord, r'()!@#$"“”````'), None)
    translation_table2 = dict.fromkeys(map(ord, "。:.,'"), ' ')
    translation_table3 = dict.fromkeys(map(ord, '/'), ' / ')
    translation_table4 = dict.fromkeys(map(ord, '-'), ' - ')
    # pc=re.compile(r'commission\w*|commision\w*|comission\w*')
    pc = re.compile(r'COMMISSION\w*|COMMISION\w*|COMISSION\w*')
    result = []
    # shortforms={'issd':'sales', 'trvl': 'travel', 'eff':'period', 'owl':'ONE WORLD ALLIANCE'} #NEED GUI TO AUGMENT THIS LIST OVER TIME
    shortforms = {'ISSD': 'SALES', 'TRVL': 'TRAVEL', 'EFF': 'EFFECTIVE', 'Eff': 'EFFECTIVE',
                  'OWL': 'ONE WORLD ALLIANCE'}  # NEED GUI TO AUGMENT THIS LIST OVER TIME
    if not isinstance(line, str):
        line = ' '.join(line)
    nltksentences = nltk.sent_tokenize(line)
    sentences = []
    for sentence in nltksentences:
        sentences += sentence.split(r'&')

    for sentence in sentences:  # , encoding="utf-16"  , encoding="latin-1"
        # this is a preprocessor
        # texts = line.rstrip("\n").decode("utf-16")
        # texts = line.upper().split() #change to upper case
        texts = nltk.word_tokenize(sentence)
        texts = [percentage.match(word).group(0) if percentage.match(word) else word for word in
                 texts]  # ensure percentages are separate numbers
        texts = [word.translate(translation_table) for word in texts]  # drop parens and other odd characters
        texts = [word.translate(translation_table2) for word in
                 texts]  # ensure sentence and word boundary typos are fixed along the way
        texts = [word.translate(translation_table3) for word in
                 texts]  # insert spaces between / so routes can be recognised
        texts = [word.translate(translation_table4) for word in texts]
        texts = [word for word in texts if word]
        texts = [word if is_tourcode(word) else re.split('(\d+)', word) for word in
                 texts]  # break up dates MMMyy -> MMM yy
        texts = flattern(texts)
        texts = [word.strip() for word in texts]  # remove leading and trailing spaces
        texts = [word for word in texts if word not in STOPLIST]
        texts = [shortforms[word] if word in shortforms.keys() else word for word in texts]
        texts = ['COMMISSION' if pc.match(word) else word for word in texts]  # clean up the use of the word commission
        texts = flatten(texts)  # catch renegade strings that should be split again
        # texts = [spellchecker(word) for word in texts if word]
        result += [texts]
    return result


def flattern(A):
    rt = []
    for i in A:
        if isinstance(i, list):
            rt.extend(flattern(i))
        else:
            rt.append(i)
    return rt


def flatten(l):
    # some sentences may  contain unsplit strings after processing eg when missing spaces get inserted
    # these need to be flattened out within the host string
    flat_list = []
    for item in l:
        # print(item,sublist,)
        sublist = item.split()
        if len(sublist) == 1:
            flat_list.append(item)
        else:
            for item in sublist:
                flat_list.append(item)
    return flat_list


def find_subtrees(chunk, tag):
    sp = [subtree for subtree in chunk.subtrees(filter=lambda t: t.label().endswith(tag))]
    return sp


def find_leaves(chunk, taglist):
    return [word for word, tag in chunk.leaves() if tag in taglist]


def find_words(chunk, tag):
    leaves = sub_leaves(chunk, tag)
    words = [word for phrase in leaves for word, tag in phrase]
    sent = ' '.join(words)
    return sent


DATEDEBUG = False


# DEBUG=True
def extractDateXXv1(subtree, toYear=2016, DEBUG=False):  # extracts ddMONyy dates
    res = []

    dmy_list = sub_leaves(subtree, 'DMYDATE')
    dm_list = sub_leaves(subtree, 'DMDATE')
    ufn_list = sub_leaves(subtree, 'DATE')
    ymd_list = sub_leaves(subtree, 'YMDDATE')
    # dt_list=False
    ufn = False

    if DEBUG:
        print(subtree)
        print('toYear', toYear)
        print('dmy_list', dmy_list)
        print('dm_list', dm_list)
        print('ymd_list', ymd_list)
        print('ufn_list', ufn_list)

    if dmy_list:
        if DEBUG:
            print('dmy_list', dmy_list)

        t = dmy_list[0]
        idxDay = first_chunk_index(t, tag_equals('CD'), start=0)
        idxMon = first_chunk_index(t, tag_equals('MM'), start=0)
        if DEBUG:
            print('idxMon', idxMon)
        if idxMon is None:
            idxMon = first_chunk_index(t, tag_equals('NN'), start=0)  # mispelt month
        idxYY = first_chunk_index(t, tag_equals('CD'), start=idxDay + 1)
        if DEBUG:
            print('idxDay', idxDay, 'idxMon', idxMon, 'idxYY', idxYY)
        tday, tmon, tyear = t[idxDay], t[idxMon], t[idxYY]

        day = int(tday[0])
        year = normaliseYear(int(tyear[0]), toYear)
        # mon=int(tmon[0])
        if DEBUG:
            print('year', year, 'tmon', tmon, 'day', day)
        if 0 < day < 32 and 2015 < year < 2025:
            try:
                if DEBUG:
                    print('mon:', tmon[0])
                mon = cal[spellchecker(tmon[0])]
            except KeyError:  # invalid date
                return datetime(9999, 12, 31)
        else:
            return datetime(9999, 12, 31)

        if DEBUG:
            print('year normalising: ', 'tyear', tyear[0], 'toYear', toYear, 'year', year)
        # res += [datetime(year,mon,day)  ]
        return datetime(year, mon, day)

    elif dm_list:
        if DEBUG:
            print('dm_list', dm_list)
        t = dm_list[0]
        idxDay = first_chunk_index(t, tag_equals('CD'), start=0)
        idxMon = first_chunk_index(t, tag_equals('MM'), start=0)

        # idxYear=first_chunk_index(t, tag_equals('CD'),start=1)
        if DEBUG:
            print('idxDay', idxDay, 'idxMon', idxMon)

        TypeError
        try:
            tday, tmon = t[idxDay], t[idxMon]
        except TypeError:
            idxMon = first_chunk_index(t, tag_equals('NN'), start=0)  # mispelt month

        if DEBUG:
            print('tday', tday, 'tmon', tmon)
        day = int(tday[0])
        if day > 31:
            # this is a MON YEAR date
            year = day
            day = 1
        mon = cal[spellchecker(tmon[0])]
        year = normaliseYear(toYear, datetime.now().year)
        if DEBUG:
            print('year normalising DM date: ', day, mon, 'toYear', toYear, 'year', year)
        # res += [ datetime(year,mon,day)  ]
        return datetime(year, mon, day)
    elif ymd_list:
        if DEBUG:
            print('ymd_list', ymd_list)

        t = ymd_list[0]
        idxYY = first_chunk_index(t, tag_equals('CD'), start=0)
        idxMon = first_chunk_index(t, tag_equals('CD'), start=idxYY + 1)
        idxDay = first_chunk_index(t, tag_equals('CD'), start=idxMon + 1)
        year, tag = t[idxYY]
        mon, tag = t[idxMon]
        day, tag = t[idxDay]
        if DEBUG:
            print('YY MM DD', year, mon, day)

        if 2010 < int(year) < 2025 and 0 < int(mon) < 13 and 0 < int(day) < 32:
            return datetime(int(year), int(mon), int(day))
        else:
            return datetime(9999, 12, 31)

    elif ufn_list:  # kept last because this will test positive for other types of dates
        # res += [datetime(9999, 12, 31)]
        # print(ufn_list)
        ufn, utag = ufn_list[0][0]
        if utag == 'UFN':
            return datetime(9999, 12, 31)

        else:
            print('New date format: ', ufn_list)
            return None
    return datetime(9999, 12, 31)


def extractDateXX(subtree, toYear=2016, DEBUG=False):  # extracts ddMONyy dates
    # DEBUG=False
    res = []
    dmy_list = sub_leaves(subtree, 'DMYDATE')
    dm_list = sub_leaves(subtree, 'DMDATE')
    ufn_list = sub_leaves(subtree, 'DATE')
    ymd_list = sub_leaves(subtree, 'YMDDATE')
    # dt_list=False
    ufn = False

    if DEBUG:
        print(subtree)
        print('toYear', toYear)
        print('dmy_list', dmy_list)
        print('dm_list', dm_list)
        print('ymd_list', ymd_list)
        print('ufn_list', ufn_list)

    if dmy_list:
        if DEBUG:
            print('dmy_list', dmy_list)

        t = dmy_list[0]
        idxDay = first_chunk_index(t, tag_equals('CD'), start=0)
        idxMon = first_chunk_index(t, tag_equals('MM'), start=0)
        if DEBUG:
            print('idxMon', idxMon)
        if idxMon is None:
            idxMon = first_chunk_index(t, tag_equals('NN'), start=0)  # mispelt month
        idxYY = first_chunk_index(t, tag_equals('CD'), start=idxDay + 1)
        if DEBUG:
            print('idxDay', idxDay, 'idxMon', idxMon, 'idxYY', idxYY)
        tday, tmon, tyear = t[idxDay], t[idxMon], t[idxYY]

        day = int(tday[0])
        year = normaliseYear(int(tyear[0]), toYear)
        # mon=int(tmon[0])
        if DEBUG:
            print('year', year, 'tmon', tmon, 'day', day)
        if 0 < day < 32 and 2010 < year < 2025:
            try:
                if DEBUG:
                    print('mon:', tmon[0])
                mon = cal[spellchecker(tmon[0])]
            except KeyError:  # invalid date
                return datetime(9999, 12, 31)
        else:
            return datetime(9999, 12, 31)

        if DEBUG:
            print('year normalising: ', 'tyear', tyear[0], 'toYear', toYear, 'year', year)
        # res += [datetime(year,mon,day)  ]
        try:
            ymd_date = datetime(year, mon, day)
        except ValueError:
            (m, n_day) = calendar.monthrange(year, mon)
            ymd_date = datetime(year, mon, n_day)

        return ymd_date
    elif dm_list:
        if DEBUG:
            print('dm_list', dm_list)
        t = dm_list[0]
        idxDay = first_chunk_index(t, tag_equals('CD'), start=0)
        idxMon = first_chunk_index(t, tag_equals('MM'), start=0)

        # idxYear=first_chunk_index(t, tag_equals('CD'),start=1)
        if DEBUG:
            print('idxDay', idxDay, 'idxMon', idxMon)

        TypeError
        try:
            tday, tmon = t[idxDay], t[idxMon]
        except TypeError:
            idxMon = first_chunk_index(t, tag_equals('NN'), start=0)  # mispelt month

        if DEBUG:
            print('tday', tday, 'tmon', tmon)
        day = int(tday[0])
        if day > 31:
            # this is a MON YEAR date
            year = day
            day = 1
        mon = cal[spellchecker(tmon[0])]
        year = normaliseYear(toYear, datetime.now().year)
        if DEBUG:
            print('year normalising DM date: ', day, mon, 'toYear', toYear, 'year', year)
        # res += [ datetime(year,mon,day)  ]
        return datetime(year, mon, day)
    elif ymd_list:
        if DEBUG:
            print('ymd_list', ymd_list)

        return YMD_dater(ymd_list, toYear=toYear)

    elif ufn_list:  # kept last because this will test positive for other types of dates
        # res += [datetime(9999, 12, 31)]
        # print(ufn_list)
        ufn, utag = ufn_list[0][0]
        if utag == 'UFN':
            return datetime(9999, 12, 31)

        else:
            print('New date format: ', ufn_list)
            return None
    return datetime(9999, 12, 31)


def extractDate_single(txt, tag):
    ST = find_subtrees(myTagger(txt), tag)
    if ST:
        [dt] = [extractDateXX(x) for x in ST]
        return dt
    else:
        return None


def YMD_dater(ymd_list, toYear=2016):
    DEBUG = False
    if DEBUG:
        print('ymd_list', ymd_list)

    t = ymd_list[0]
    tag1 = first_chunk_index(t, tag_equals('CD'), start=0)
    tag2 = first_chunk_index(t, tag_equals('CD'), start=tag1 + 1)
    tag3 = first_chunk_index(t, tag_equals('CD'), start=tag2 + 1)
    tag1, tag = t[tag1]
    tag2, tag = t[tag2]
    tag3, tag = t[tag3]
    ttag1 = int(tag1)
    ttag2 = int(tag2)
    ttag3 = int(tag3)
    if DEBUG:
        print('ttag1 ttag2 ttag3', ttag1, ttag2, ttag3)

    if 2010 <= ttag1 < 9999 and 1 <= ttag2 <= 12 and 1 <= ttag3 <= 31:  # normal YMD
        if DEBUG:
            print('normal YMD')
        return datetime(year=ttag1, month=ttag2, day=ttag3)

    elif 2010 <= ttag3 < 9999 and 1 <= ttag2 < 12 and 1 <= ttag1 <= 31:  # check if DMY
        if DEBUG:
            print('check if DMY')
        return datetime(year=ttag3, month=ttag2, day=ttag1)

    elif 2010 <= ttag3 < 9999 and 1 <= ttag1 <= 12 and 1 <= ttag2 <= 31:  # check if MDY
        if DEBUG:
            print('check if MDY')
        return datetime(year=ttag3, month=ttag1, day=ttag2)

    elif 16 <= ttag3 <= 9999 and 1 <= ttag2 <= 12 and 1 <= ttag1 <= 31:  # check if DMY with 2 digit year
        if DEBUG:
            print('check if DMY with 2 digit year')
            print('ttag3', ttag3)
            print(datetime(year=2000 + ttag3, month=ttag2, day=ttag1))
        return datetime(year=2000 + ttag3, month=ttag2, day=ttag1)

    elif 16 <= ttag3 <= 9999 and 1 <= ttag1 <= 12 and 1 <= ttag2 <= 31:  # check if MDY with 2 digit year
        if DEBUG:
            print('check if MDY with 2 digit year')
            print('ttag3', ttag3)
            print(datetime(year=2000 + ttag3, month=ttag2, day=ttag1))
        return datetime(year=2000 + ttag3, month=ttag1, day=ttag2)

    elif 0 <= ttag3 < 9 and 1 <= ttag2 <= 12 and 1 <= ttag1 <= 31:  # check if DMY with 1 digit year
        if DEBUG:
            print('check if DMY with 1 digit year')
        return datetime(year=2010 + ttag3, month=ttag2, day=ttag1)

    elif 0 <= ttag3 < 9 and 1 <= ttag1 <= 12 and 1 <= ttag2 <= 31:  # check if MDY with 1 digit year
        if DEBUG:
            print('check if MDY with 1 digit year')
        return datetime(year=2010 + ttag3, month=ttag1, day=ttag2)

    else:
        return datetime(9999, 1, 1)


def normaliseYear(year, toYear):
    if not year:
        if toYear == 9999:
            return datetime.now().year
        else:
            return toYear

    if 10 <= year < 100:  # two digit year
        return year + 2000
    elif 1 <= year < 10:  # one digit guess at typo
        return year + 2010
    else:
        return year


def find_period_detail_1(chunk, tag):
    DEBUG = False
    res = []

    if DEBUG:
        print('in find_period_detail ', chunk)
        # print(chunk, tag)
    try:
        # periods=[subtree for subtree in chunk.subtrees(filter=lambda t: t.label().tag_equals(tag))]
        periods = find_subtrees(chunk, tag)
    except ValueError:  # tag not found
        return [[datetime(2016, 1, 1), datetime(9999, 1, 1)]]
    if DEBUG:
        print('periods', periods)
    if not periods:  # PERIOD not found
        return [[datetime(2016, 1, 1), datetime(9999, 1, 1)]]
    # print('Found ', len(periods), ' ',tag,'s')
    for period in periods:
        ranges = find_subtrees(period, 'RANGE')
        if DEBUG:
            print('period', period)
            print('range', ranges)
        if ranges:
            for r in ranges:
                if DEBUG:
                    print('RANGE ', r)
                [td] = find_subtrees(r, 'TODATE')
                [fd] = find_subtrees(r, 'FROMDATE')
                # process TODATE first, as likely to have YEAR information
                if td:
                    to_date = extractDateXX(Tree('S', td))
                    if DEBUG:
                        print('to_date', to_date)
                else:
                    to_date = datetime(9999, 1, 1)
                if DEBUG:
                    print('rang: ', r, 'td: ', td, 'fd: ', fd)

                if fd:
                    if DEBUG:
                        print('In find_period_detail')
                        print('to_date', to_date, 'fd', fd)
                    from_date = extractDateXX(Tree('S', fd), to_date.year)
                    if DEBUG:
                        print('from_date', from_date)
                else:
                    from_date = datetime(2016, 1, 1)
                res.append([from_date, to_date])
        else:
            try:

                if DEBUG:
                    print('no range found')
                    print('TODATEs', find_subtrees(period, 'TODATE'))
                [td] = find_subtrees(period, 'TODATE')
            except ValueError:
                td = []
            try:
                [fd] = find_subtrees(period, 'FROMDATE')
            except ValueError:
                fd = []
            # process TODATE first, as likely to have YEAR information
            if td:
                to_date = extractDateXX(Tree('S', td))
                if DEBUG:
                    print('to_date', to_date)
            else:
                to_date = datetime(9999, 1, 1)

            if DEBUG:
                print('rang: ', r, 'td: ', td, 'fd: ', fd)

            if fd:
                from_date = extractDateXX(Tree('S', fd), to_date.year)
                if DEBUG:
                    print('from_date', from_date)
            else:
                from_date = datetime(2016, 1, 1)
            res.append([from_date, to_date])

    return res


def setup_classifications(srch={}):
    cat_tags = list(db.categorytags.find())
    for cnt, fare in enumerate(fs.find(srch)):
        print(cnt, 'Processing', fare['filename'])
        tags = {}
        for cat_tag in cat_tags:
            classifier = cat_tag['category']
            if classifier in fare.keys():  # pre-existing classification
                tags[classifier] = fare[classifier]
            else:
                tags[classifier] = None

        db.CXfaresheets.update_one({'_id': fare['_id']}, {'$set': {'classifications': tags}})
    print('CLASSIFICIATIONS SET UP')




#################
### Tests   #####
#################
sel='honoured for ticket issue on/before 10Oct17'
ext_dt=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
assert ext_dt==datetime(2017, 10, 10, 0, 0), "FAILED EXTENSION DATE TEST"
