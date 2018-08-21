import collections
import datetime
import os
import csv
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from nlp.cat16utilities import *

client=MongoClient('localhost:27017')
db = client.raxdb
cat16 = client.cat16

Farefolder = collections.namedtuple('Farefolder', ['fileID', 'filename', 'salesdays', 'salesperiod', 'travelperiod', 'carriers'])
dateToken=collections.namedtuple('dateToken' ,['tix_date', 'fare_period'])

FAREDEBUG=True
dirname='./checking/'
#BLACKLIST=[11179, 12361, 6515, 6501, 12376,3842, 10789, 10789]
BLACKLIST=[2999, 8726]
#reissues=db.MarJun2017V2
reissues=db.UL_JUL_NOV_2017  #process refunds no fee
private_reissues=reissues.find({"Fare Source": 'NOT FOUND' })
#reissues=db.refund
#tourcode='GB01DN'
#tourcodes= ['CA01SO/DIIR' , 'ITL', 'FR01WB' ,'CA01SO','GB01DN' ]
#tourcodes=list(TOURCODES)



#tourcode=""
print('Number of refund cases: ', reissues.count())
#private_reissues=reissues.find({"tour_cd" : tourcode})
#private_reissues=reissues.find({"tour_cd": {'$ne': ""} })
#private_reissues=reissues.find({"tour_cd": {'$in': tourcodes} })
private_reissues=reissues.find({"Fare Source": 'NOT FOUND' }) # based on Brindley's output
print('Number of reissue coupons with tourcodes: ', private_reissues.count())
t0=datetime.now()
t=t0.strftime('%y%m%d%M') #to timestamp output files

tkt_cnt=0
fn="matchedfares"+ t +".csv"
with open(os.path.join(dirname,fn),'w') as matchedfares: 
    
    raxwriter=csv.writer(matchedfares)
    #,delimiter=',',
    #                        quotechar='', quoting=csv.QUOTE_MINIMAL
    raxwriter.writerow(['rec' , 'doc_nbr_prime', 'cpn_nbr', 'TKT POS cntry', 'TKT mkt_flt_carr', 'TKT Tourcode','trnsc_date', 'dep_date', 
        'FareID', 'Fare Currency', 'Carriers', 'sales_period begin', 'sales_period end', 
        'travel_period begin', 'travel_period end', 'fileID' , 'file'])
    for tkt in private_reissues:
        t1=datetime.now()
        doc_nbr_prime = tkt['doc_nbr_prime']
        cpn_nbr=tkt['cpn_nbr']
        tkt_cnt+=1
        print( 'Rec:', tkt_cnt,'Processing ticket: ',doc_nbr_prime, 'coupon:',cpn_nbr)
        tourcode=tkt['tour_cd']

        if not tourcode:
            print('Ticket has no Tourcode!')
            continue
        #tkt_departure_date= datetime.strptime(tkt['dep_date'], '%d/%m/%y')
        tkt_departure_date= datetime.strptime(tkt['dep_date'], '%d-%b-%y')
        
        #tkt_transaction_date = datetime.strptime(tkt['trnsc_date'], '%d/%m/%y') 
        tkt_transaction_date = datetime.strptime(tkt['trnsc_date'], '%d-%b-%y')
        #print ('Transaction date:', tkt['trnsc_date'], 'Travel Date: ', tkt['dep_date'] )
        print ('Transaction date:', tkt_transaction_date.strftime('%d-%b-%y'), 'Travel Date: ', tkt_departure_date.strftime('%d-%b-%y') )
        try:
            privatefares=db.faresheets.find( { "$text": { "$search": tourcode } } )
            #privatefares=db.faresheets.find( { "$text": { "$search": "  \"UK05\" \"GB01TO\" \"CHANGE FEE\"" } } )
        except OperationFailure:
            print('Unable to locate faresheets for Tourcode:',tourcode)
            raxwriter.writerow([tkt_cnt, tkt ['doc_nbr_prime'], tkt ['cpn_nbr'], 
                                tkt ['POS cntry'], tkt['tour_cd'], tkt ['trnsc_date'], 
                                tkt ['dep_date'], 'Unable to locate faresheets'])
            continue
        
        search=" \"" + tourcode + "\" \"CHANGE FEE\""
        CAT16privatefares=db.faresheets.find( { "$text": { "$search": search } } )
        if CAT16privatefares.count()>0:
            privatefares=CAT16privatefares

        print ('Private faresheet count for Tourcode: ', tourcode, ' is ', privatefares.count())
        print ('Private faresheet count for Tourcode and CHANGE FEE: ', tourcode, ' is ', CAT16privatefares.count())
        # find the set of fares for valid travel date and transaction date
        candidatefares=[]
        carrier_fares=[]
        for privatefare in privatefares:
            
            #fare = FareFile(getULfile(privatefare['id']))
            #print('fileID' , privatefare['id'])
            #print ('Travel date: ', fare.travel_period, 'Sales Period', fare.sales_period)
            # if any dates are not specified, we want to pass them for this
            #sales_periods=fare.sales_period
            filepath=privatefare['filepath']
            if FAREDEBUG:
                print('processing fare:', privatefare['id'], 'filepath: ', filepath)
            if privatefare['id'] in BLACKLIST:
                print ('processing fare:', privatefare['id'], 'blacklisted')
                continue
            try:
                sales_periods=privatefare['sales_period']
            except KeyError:
                print(('Error fare:', privatefare['id'], 'not processed. Sales period missing'))
                continue
            #print('Sales Period',sales_periods )
            
            
            #print('Travel Period',travel_periods )
            sp_tuples=[dateToken(tkt_transaction_date, x) for x in sales_periods] 
            res= list(map((lambda x: x.fare_period if x.fare_period[0]<= x.tix_date <= x.fare_period[1] else [] ),sp_tuples))
            if res:
                valid_sales_period=res[0]
            else:
                valid_sales_period=False
            
            #travel_periods=fare.travel_period
            #try:
            #    travel_periods= privatefare['travel_period']
            #except KeyError:
            #    print(('Error fare:', privatefare['id'], 'not processed. Travel period missing'))
            #    continue
            
            travel_periods= privatefare['travel_period']
            tp_tuples=[dateToken(tkt_departure_date, x) for x in travel_periods] 
            res = list(map((lambda x: x.fare_period if x.fare_period[0]<= x.tix_date <= x.fare_period[1] else [] ),tp_tuples))
            if res:
                valid_travel_period=res[0]
            else:
                valid_travel_period=False
 
            if FAREDEBUG:
                print('tp_tuples',tp_tuples)
                print('res',res)
                print('valid_travel_period',valid_travel_period)

            carrier_fares.append(privatefare['carriers'])
            if valid_sales_period and valid_travel_period and tkt['mkt_flt_carr'] in privatefare['carriers']:
                #ff=Farefolder(privatefare['id'], privatefare ['filename'], 
                ff=Farefolder(privatefare['id'], privatefare ['filepath'], 
                    valid_sales_period[0]-tkt_transaction_date, 
                    valid_sales_period, 
                    valid_travel_period, 
                    privatefare['carriers'] )
                candidatefares.append(ff)
                print(ff.filename)
            
        candidatefares.sort(key=lambda tup: (tup.salesdays))
        #shortlist=[fare for fare in candidatefares if fare.salesdate < datetime(9999,1,1)] #remove sheets with default date info
        print ('Total sheets with tourcode and compatible sales and travel dates:' ,len(candidatefares))
        
        if len(candidatefares)==0:
            print("NO MATCHING FARES!")
            print('mkt_flt_carr:',tkt['mkt_flt_carr'])
            print('Carriers with this tourcode:',carrier_fares)
            #checkfares=db.faresheets.find( { "$text": { "$search": search } , 'carriers': { '$all': [tkt['mkt_flt_carr']] } } )
            checkfares=db.faresheets.find( { "$text": { "$search": search }  } )
            raxwriter.writerow([tkt_cnt, tkt ['doc_nbr_prime'], tkt ['cpn_nbr'], 
                                tkt ['POS cntry'], tkt['mkt_flt_carr'],tkt['tour_cd'], 
                                tkt ['trnsc_date'], tkt ['dep_date'], "MATCHING FARE NOT FOUND!"])
            for eachfare in checkfares:
                
                print(eachfare['id'], eachfare ['filepath'] )
                print('Travel date: ', eachfare['travel_period'], 
                      'Sales Period', eachfare['sales_period'] ,eachfare['carriers'] )
                select_salesperiod = [dt.strftime('%d-%b-%Y') for dt in eachfare['sales_period'][0]]
                select_travelperiod = [dt.strftime('%d-%b-%Y') for dt in eachfare['travel_period'][0]]
                raxwriter.writerow([tkt_cnt, tkt ['doc_nbr_prime'], tkt ['cpn_nbr'], 
                                tkt ['POS cntry'], tkt['mkt_flt_carr'],
                                tkt['tour_cd'], tkt ['trnsc_date'], 
                                tkt ['dep_date'], eachfare ['id'], eachfare['currency'], eachfare['carriers'],
                                select_salesperiod[0], select_salesperiod[1], 
                                select_travelperiod[0], select_travelperiod[1], 
                                eachfare['fileID'], eachfare['filename']])
            continue

        if FAREDEBUG:
            for fare in candidatefares:
                print (print('ID: ', fare.fileID, 'file: ', fare.filename))
                print ('MOST RECENT FARESHEET')
                print('ID: ', mostrecent.fileID, 'file: ', mostrecent.filename)        

        mostrecent=candidatefares[-1] #get the closest to the ticket transaction date
        ff=db.faresheets.find_one({"id": mostrecent.fileID })
        select_salesperiod = [dt.strftime('%d-%b-%Y') for dt in ff['sales_period'][0]]
        select_travelperiod = [dt.strftime('%d-%b-%Y') for dt in ff['travel_period'][0]]
        print ('Sales Period', select_salesperiod ,'Travel date: ',select_travelperiod)
        print ('TKT POS country', tkt['POS cntry'], 'Fare currency', ff['currency'])
        print('Mkt mkt_flt_carr',tkt['mkt_flt_carr'] , 'Faresheet for:',mostrecent.carriers )

        raxwriter.writerow([tkt_cnt, tkt ['doc_nbr_prime'], tkt ['cpn_nbr'], 
                                tkt ['POS cntry'], tkt['mkt_flt_carr'],
                                tkt['tour_cd'], tkt ['trnsc_date'], 
                                tkt ['dep_date'], ff ['id'], ff['currency'], ff['carriers'],
                                select_salesperiod[0], select_salesperiod[1], 
                                select_travelperiod[0], select_travelperiod[1], 
                                mostrecent.fileID,  mostrecent.filename])
        t2=datetime.now()
        print('fare processing time: ',(t2-t1))
        print ('---------------------------------------------------------------')
        print('  ')
    print('Results file:', fn)
    t3=datetime.now()
    bench=(t3-t0).total_seconds()
    msg=f'TOTAL processing time: {bench:.1f} seconds'
    print(msg)

from collections import Counter, defaultdict


fares=Counter()
fare_tags=defaultdict(None)
fareIDs=set([x['fare_id'] for x in (cat16.find({},{'fare_id':1,'_id':0}))])
print('there are ',len(fareIDs), ' fare_id s')
for idx in fareIDs:
    for rules in cat16.find({'fare_id':idx}):
        fares[rule['fare_id']]+=1
        new_tags=cat16_rule(rule).train()
        curr_tags=fare_tags[rule['fare_id']]
        cols=get_union_keys(curr_tags,new_tags)
        #base=dict({(x,False) for x in cols})
        fare_tags[rule['fare_id']]=dict({(x,True) if new_tags[x] or curr_tags[x] else (x,False) for x in cols })


fares=Counter()
fare_tags=defaultdict(None)
fareIDs=set([x['fare_id'] for x in (cat16.find({},{'fare_id':1,'_id':0}))])
print('there are ',len(fareIDs), ' fare_id s')
for idx in fareIDs:
    tag_list = [ cat16_rule(rule).train() for rule in cat16.find({'fare_id':idx})]
    cols=get_union_keys(tag_list)
    base=dict({(x,False) for x in cols})
    tag_list=base+tag_list




t1=datetime.now()
fares=Counter()
fare_tags=defaultdict(None)
fareIDs=list(set([x['fare_id'] for x in (cat16.find({},{'fare_id':1,'_id':0}))]))
total=len(fareIDs)
cnt=0
print('there are ',total, ' fare_id s')
for idx in fareIDs:
    tag_list = [ cat16_rule(rule).train() for rule in cat16.find({'fare_id':idx},{'disc_tag1':1,'disc_tag2':1})]
    #print(tag_list)
    cols=get_union_keys(tag_list)
    #print(cols)
    base=dict({(x,False) for x in cols})
    tag_list=[base]+tag_list
    for el in tag_list:
        fare_tags[idx]=dict({(x,True) if el[x] or base[x] else (x,False) for x in cols })
    cat16.update_many({'fare_id':idx},{'$set': fare_tags[idx]})
    cnt+=1
    if (cnt % 500) == 0:
        t3=datetime.now()
        cum=(t3-t1).total_seconds()
        rem=((total-cnt)*cum/cnt)/60
        etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
        print(cnt, ' fares processed')
        stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'        
        print(stat)

t4=datetime.now()    
bench=(t4-t1).total_seconds()/60
msg=f'TOTAL features setup time: {bench:.1f} minutes'
print(msg)