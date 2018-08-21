from nlp.tagger import *
from nlp.production import *

######################################
### TESTING ORIGINATING LOCATIONS  ###
######################################
print('#####################')
TEST='TESTING ORIGINATING LOCATIONS: '
cnt=0


txt='USA/MEXICO and LATIN AMERICA UPFRONT COMMISSION  (all published fare rules apply):'
originating=find_origins(myTagger(txt))
assert originating==[], "FALSE POSITIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")

txt='Originating OTHER THAN USA/MEX/LATIN AMERICA'
originating=find_origins(myTagger(txt))

assert originating==['-US', '-MX', '-LATIN AMERICA'], "FALSE NEGATIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")

txt='Commission level allows for USA/MEX/Latin America domestic carriage to the Cathay Gateway. All Commissions are only valid for USA /MEXICO / LATIN AMERICA originating traffic.'
originating=find_origins(myTagger(txt))
print(originating)
assert originating==['+US', '+MX', '+LATIN AMERICA'], "FALSE NEGATIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")


txt='Originating from USA UPFRONT DISCOUNT'
originating=find_origins(myTagger(txt))
print(originating)
assert originating==['+US'], "FALSE NEGATIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")

txt='Originating from DFW on CX CODE SHARE FLIGHT NUMBER CX7681/ CX7680 UPFRONT DISCOUNT'
originating=find_origins(myTagger(txt))
print(originating)
assert originating==['+DFW'], "FALSE NEGATIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")

txt='Originating OTHER THAN USA (Below discounts apply to JFK-YVR also)'
originating=find_origins(myTagger(txt))
print(originating)
assert originating==['-US'], "FALSE NEGATIVE:"+txt
cnt+=1
print (cnt,txt, "PASSED")

print('PASSED IDENTIFYING ORIGINATING LOCATIONS')



##################################
###          YMD DATER         ###
##################################
print('#####################')
print ('Testing YMD dater')



ymd_list=[[('10', 'CD'), ('/', 'TO'), ('16', 'CD'), ('/', 'TO'), ('2017', 'CD')], [('2017', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: MDY format'

ymd_list=[[('2017', 'CD'), ('/', 'TO'), ('10', 'CD'), ('/', 'TO'), ('16', 'CD')], [('2017', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: YMD format'

ymd_list=[[('16', 'CD'), ('/', 'TO'), ('10', 'CD'), ('/', 'TO'), ('2017', 'CD')], [('2017', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: DMY format'

ymd_list=[[('10', 'CD'), ('/', 'TO'), ('16', 'CD'), ('/', 'TO'), ('2017', 'CD')], [('17', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: MDY format 2 DIGIT YEAR'

ymd_list=[[('16', 'CD'), ('/', 'TO'), ('10', 'CD'), ('/', 'TO'), ('2017', 'CD')], [('17', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: DMY format 2 DIGIT YEAR'

ymd_list=[[('16', 'CD'), ('/', 'TO'), ('10', 'CD'), ('/', 'TO'), ('7', 'CD')], [('2017', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: DMY format 1 DIGIT YEAR'

ymd_list=[[('10', 'CD'), ('/', 'TO'), ('16', 'CD'), ('/', 'TO'), ('7', 'CD')], [('7', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 16, 0, 0), 'FAIL: MDY format 1 DIGIT YEAR'

ymd_list=[[('10', 'CD'), ('/', 'TO'), ('12', 'CD'), ('/', 'TO'), ('7', 'CD')], [('7', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 12, 10, 0, 0), 'FAIL: MDY format 1 DIGIT YEAR'

ymd_list=[[('12', 'CD'), ('/', 'TO'), ('10', 'CD'), ('/', 'TO'), ('7', 'CD')], [('7', 'CD'), ('-', 'TO'), ('10', 'CD'), ('-', 'TO'), ('09', 'CD')]]
assert YMD_dater(ymd_list,toYear=2016)==datetime(2017, 10, 12, 0, 0), 'FAIL: MDY format 1 DIGIT YEAR'

print('PASSED ALL YMD DATER TESTS')



########################################
###        EXTENSION DATES           ###
########################################
print('#####################')
print('Testing Extension Date tagging and extraction')

sel='original_filename HKG2407_AUS_R2017 001_Working_holiday_001_wef_01Apr17 31Mar18 Wef 01Apr17 fare revised and extended to 31Mar18 Previous version V2016 003 Australia Working Holiday Fares Sales Ticketing 01APR2017 31MAR2018 V2017 001 Tour Code HKG2407FF538 3 24 2017'
print('TESTING: {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}<ISSUE>')
tagged=myTagger(sel)
#print(tagged)
ed_chunk=find_subtrees(tagged,'EXTENSION_DATE')
#ext_date=extractDateXX(ed_chunk)
#print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ed_chunk==[], "FALSE POSITIVE: fare revised and extended to 31Mar18 "
print("1 Avoided","FALSE POSITIVE: fare revised and extended to 31Mar18 ")

sel='original_filename HKG0415_NZ_R2017 005_EB_005_iss on_30Mar17 26Apr17_dep until 30Nov17 Addon Please refer Addon faresheet Southwest Pacific Add on Fares Conditions Fares extended for sales till 26Apr17 with travel period revised New Zealand EarlyBird Sales Ticketing 30MAR2017 26APR2017 V2017 005 30 03'
tagged=myTagger(sel)
#print(tagged)
ed_chunk=find_subtrees(tagged,'EXTENSION_DATE')
#ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('2 ed_chunk:',ed_chunk)
assert ed_chunk==[], "FALSE POSITIVE: Fares extended for sales till 26Apr17 "
print("2 Avoided","FALSE POSITIVE: Fares extended for sales till 26Apr17 " )



sel='Previous version V2017-006 dated 28/09/2017 will be honoured for ticket issue on/before 10Oct17'
#ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
ext_date=extract_extension_date([sel])
print(ext_date)
print('3 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 10, 10, 0, 0), "Failed: honoured for ticket issue on/before 10Oct17 "

sel='prior to departure as per ADVP Auto ticketing time limit TKTL applies to all bookings generated by the GDS at the time of booking being made All bookings made on previous version LON2300FF500 HKG F J YR 2016 18v4 to be issued as per normal ticketing time limit or by latest 02 April 2017 whichever is earlier Taxes HKG Security'
#ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
ext_date=extract_extension_date([sel])

print('4 TESTING: {<PREVIOUS><VERSION><.*>*<TICKET><.*>*<JJS><TODATE>}')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 4, 2, 0, 0), "Failed: All bookings made on previous version LON2300FF500 HKG F J YR 2016 18v4 to be issued as per normal ticketing time limit or by latest 02 April 2017 "

sel='Previous version V2017-005 (26/07/2017) will be honour for tkt iss up to 16Aug17.'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('5 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 8, 16, 0, 0), "Failed: will be honour for tkt iss up to 16Aug17"

sel='Wef 01May17, fares / validity revised and extended till 30Apr18.Business class fares uplifted.CKG/CTU RBD'
tagged=myTagger(sel)
#print(tagged)
ed_chunk=find_subtrees(tagged,'EXTENSION_DATE')
#ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
#print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ed_chunk==[], "FALSE POSITIVE: validity revised and extended till 30Apr18"

sel='30Apr19 Companion Package Fare min 2 pax RGN Addon Please refer Addon fare sheet South East Asia Add on Fares Wef 01Jan18 fare revised and introduced for travel 01May18 30Apr19 Previous version valid for sales upto 31Dec17 only Sales Ticketing 01JAN2018 30APR2019 V2017 Tour Code HKG9239FF800 20 12 2 Carrier of the first outbound sector determines the applicable Ticket Stock'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('7 TESTING {<PREVIOUS><VERSION><VALIDITY><IN><SALES><TO><TODATE>}')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 12, 31, 0, 0), "Failed: Previous version valid for sales upto 31Dec17"
print('TESTING TRAVELPERIOD range')

sel='Previous version will be honored for ticket issue upto 13Sep17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('8 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 9, 13, 0, 0), "Failed: Previous version will be honored for ticket issue upto 13Sep17"

sel='Previous version V2017 001 will honour for ticket issued upto 29Jul17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('9 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 7, 29, 0, 0), "Failed:"+sel

sel='Previous version valid for sales upto 31Dec17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('10 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 12, 31, 0, 0), "Failed:"+sel


sel='Previous version V2017 011 26 05 2017 will be honour for tkt iss up to 14Jun17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('11 TESTING SHORT FORMS: tkt, iss')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 6, 14, 0, 0), "Failed:"+sel


sel='Previous version V2017 006 dated 10 08 2017 will be honoured for ticket issue upto 13Sep17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('12 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 9, 13, 0, 0), "Failed:"+sel

sel='HKM0816_FOC_Prom_17003_dep_19May17 - 28Oct17(20170519).html'
fare=FareFile("16FLL888FF520_Rev1_(Hotsent_23Jun16).html","US")
fn_list=sel.split('_')
fn=' '.join(fn_list)

tagged_filename=myTagger(fn)
print(tagged_filename)
effective_date=min(fare.find_period_detail(tagged_filename,'SALESPERIOD')[0])

assert effective_date==datetime(2016, 1, 1, 0, 0), "Failed:"+sel
effective_date=min(fare.find_period_detail(tagged_filename,'TRAVELPERIOD')[0])
print('13 Effective date:',effective_date.strftime('%d-%b-%Y'))
assert effective_date==datetime(2017, 5, 19, 0, 0), "Failed:"+sel


sel='Fare & class code revised and extended for sales till 31Dec16. Previous vers V2015-003 will honoured for tkt issued upto 05Mar16.'
print('TESTING: {<HONORED><.*><TICKET><SALES><.*>*<TODATE>}')
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('14 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2016, 3, 5, 0, 0), "Failed:"+sel

sel='Previous version V2017 006 dated 10 08 2017 will be honoured for ticket issue upto 13Sep17'
ext_date=extractDateXX(find_subtrees(myTagger(sel),'EXTENSION_DATE')[0])
print('15 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 9, 13, 0, 0), "Failed:"+sel

sel='original_filename HKG6340_India_R2016 001_CX_Exp_YCL_001_wef_01Mar16 31Dec16 xlsx 秘密文件 Previous version V2015 003 Fare class code revised and extended for sales till 31Dec16 Previous vers V2015 003 will honoured for tkt issued upto 05Mar16 V2016 001 CX KA India original_filename HKG6340_India_R2016 001_CX_Exp_YCL_001_wef_01Mar16 31Dec16 xlsx 秘密文件 Previous version V2015 003 Fare class code revised and extended for sales till 31Dec16 Previous vers V2015 003 will honoured for tkt issued upto 05Mar16 V2016 001 CX KA India Express Confidential HKG6340'
ext_date=extract_extension_date([sel])
print('16 Extension date:', ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2016, 3, 5, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1703 宽 限 期 延 至 2017-09-08 最 后 更 新 日 期 ：2017-09-01'
print('TESTING {<PREVIOUS><VERSION><.*>*<EXTENDED><VALIDITY><SALES><.*>*<TODATE>}<ISSUE>')
ext_date=extract_extension_date([sel])
print('17 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 9, 8, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1703 开 票 宽 限 期 为 2017-05-12 最 后 更 新 日 期 ：2017-05-05'
print('TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<ISSUE>')
ext_date=extract_extension_date([sel])
print('18 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 5, 12, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1705 开 票 宽 限 期 至 ： 2017-12-11 最 后 更 新 日 期 ：2017-12-04'
ext_date= extract_extension_date([sel])
print('19 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 12, 11, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1701 开 票 宽 限 期 至 2017-12-16 。 2017-12-09'
ext_date= extract_extension_date([sel])
print('20 TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}<DATE>')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 12, 16, 0, 0), "Failed:"+sel

sel='上 一 版 本 的 开 票 代 码 是 SGV T1:WNZ9701FF701，SGV T2:WNZ9701FF708， 开 票 宽 限 期 至 23NOV17 最 后 更 新 日 期 ：2017-11-16'
ext_date= extract_extension_date([sel])
print('21 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 11, 23, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1707 开 票 宽 限 期 至 ： 2017 年 12 月 9 日 最 后 更 新 日 期 ：2017-12-05'
ext_date= extract_extension_date([sel])
print('22 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 12, 9, 0, 0), "Failed:"+sel

sel='FOC9501FF701&FOC9501FF708 V1702 开 票 宽 限 期 至 ：2017-11-23 最 后 更 新 日 期 ：2017-11-16'
ext_date= extract_extension_date([sel])
print('23 TESTING {<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<ISSUE>')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 11, 23, 0, 0), "Failed:"+sel

sel='重 要 更 新 提 示 ： 全 新 2017 价 单 版 本 号 ：NCA1701A 上 一 版 本 NCA1701 宽 限 期 至 2017-06-27 最 后 更 新 日 期 ：2017-04-14'
ext_date= extract_extension_date([sel])
print('24 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 6, 27, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1605 开 票 宽 限 期 至 ：2017-02-22 最 后 更 新 日 期 ：2016-02-15'
ext_date= extract_extension_date([sel])
print('25 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 2, 22, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1704 开 票 宽 限 期 至 ：2018-01-05 2017/12/29'
ext_date= extract_extension_date([sel])
print('26 TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<DATE>}<DATE>')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2018, 1, 5, 0, 0), "Failed:"+sel

sel='上 一 版 本 开 票 宽 限 期 至 ：2016-12-31 2017-01-01'
ext_date= extract_extension_date([sel])
print('27 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2016, 12, 31, 0, 0), "Failed:"+sel

sel='上 一 版 本 TAO9300FF700 V1703 最 后 开 票 日 期 到 2017-11-23 。 最 后 更 新 日 期 ：2017-11-16'
ext_date= extract_extension_date([sel])
print('28 TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><DAY><SALES><.*>*<DATE>}<ISSUE>')
print('Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 11, 23, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1723 最 后 开 票 日 期 为 2018-01-04 最 后 更 新 日 期 ：2017-12-28'
ext_date= extract_extension_date([sel])
print('29 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2018, 1, 4, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1701 最 后 开 票 期 限 为 2017-03-01 最 后 更 新 日 期 ：2017-02-22'
ext_date= extract_extension_date([sel])
print('30 Extension date:',ext_date.strftime('%d-%b-%Y'))
print('TESTING {<PREVIOUS><VERSION><NN><.*>*<TRAVEL><.*>*<SALES><VALIDITY><.*>*<DATE>}<ISSUE>')
assert ext_date==datetime(2017, 3, 1, 0, 0), "Failed:"+sel


sel='上 一 版 本 CGOA1704 开 票 宽 限 期 至 2017-09-27. 最 后 更 新 日 期 ：2017-09-20'
ext_date= extract_extension_date([sel])
print('31 TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><TO><.*>*<TODATE>}')
print('31 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 9, 27, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1711 开 票 宽 限 期 至 : 10/16/2017 2017-10-09'
ext_date= extract_extension_date([sel])
print('32 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 10, 16, 0, 0), "Failed:"+sel

sel='must be issued at least 45 days prior to departue If booked within 45 days of departure Ticket Time limit then must be ticketed immediately Bookings MAD e under the previous version LON6062FF880 CONSOL SPO_2015 17v2 must be issued by 31May 16 TKTL is auto generated regardless ot the fare type Please enter TOUR XXX XXX = agency name into'
ext_date= extract_extension_date([sel])
print('32 TESTING {<PREVIOUS><VERSION><.*>*<MD><VB><SALES><IN><TODATE>}')
print('32 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2016, 5, 31, 0, 0), "Failed:"+sel

sel='上 一 版 本 V1704 开 票 宽 限 期 2017-08-02 2017-07-26'
ext_date= extract_extension_date([sel])
print('33 TESTING {<PREVIOUS><VERSION><.*>*<TRAVEL><EXTENDED><VALIDITY><SALES><.*>*<FROMDATE>}<TODATE>')
print('33 Extension date:',ext_date.strftime('%d-%b-%Y'))
assert ext_date==datetime(2017, 8, 2, 0, 0), "Failed:"+sel

print('PASSED ALL EXTENSION DATE TESTS')

print()

####################################
###        TESTING ROUTES        ###
####################################
print('#####################')
print ('Testing routes')
rt='DFW – HKG'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['DFW'], "INCORRECT LOCATION:"+rt
assert to_location==['HKG'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

rt='USA OR MEXICO - ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['USA','MEXICO']), "INCORRECT LOCATION:"+rt
assert to_location==['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

rt='ORD-HKG'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['ORD'], "INCORRECT LOCATION:"+rt
assert to_location==['HKG'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

rt='BOS SIN AND BOS SHA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['BOS'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)==set(['SIN', 'SHA']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'BOS TO PVG': ('BOS', 'PVG'),
rt='BOS TO PVG'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['BOS'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)==set(['PVG']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'CANADA TO': ('CANADA', None)
rt='CANADA TO'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['CANADA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== None, "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'CANADA TO ASIA': ('CANADA', 'ASIA'),
rt='CANADA TO ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['CANADA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'CHI OR CID TO ASIA': (['CHI', 'CID'], 'ASIA'),
rt='CHI OR CID TO ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['CHI', 'CID']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'LAX TO ASIA': ('LAX', 'ASIA'),
rt='LAX TO ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['LAX'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'LAX TO ASIA ONLY': ('LAX', 'ASIA'),
rt='LAX TO ASIA ONLY'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['LAX'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'USA TO ASIA': ('USA', 'ASIA'),
rt='USA TO ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['USA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'USA-AISA': ('USA', 'ASIA'),
rt='USA-AISA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['USA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# USA ORLATIN AMERICA - ASIA': ('USA', 'ASIA'),
rt='USA OR LATIN AMERICA - ASIA'
#rt=pre_process_route(rt)
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['USA', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'USA ASIA': ('USA', 'ASIA')
rt='USA ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['USA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'CANADA – ASIA*': ('CANADA', 'ASIA')
rt='CANADA – ASIA*'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==['CANADA'], "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'USA ORLATIN AMERICA - ASIA': ('USA', 'ASIA'),
rt= 'USA ORLATIN AMERICA - ASIA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['USA', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'JFK/EWR-AISA': (['EWR', 'JFK'], 'ASIA'),
rt= 'JFK/EWR-AISA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['JFK', 'EWR']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== ['ASIA'], "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


# 'TOWUH, CAN, BJS,': (None, ['WUH', 'CAN', 'BJS']),
rt='TOWUH, CAN, BJS,'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location==None, "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)== set(['WUH', 'CAN', 'BJS']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

# 'SHA OR HKG': (['SHA', 'HKG'], ['SHA', 'HKG']),
rt= 'SHA OR HKG'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['SHA', 'HKG']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)== set(['SHA', 'HKG']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


#'MEXICO ANDLATIN AMERICA'
rt='MEXICO ANDLATIN AMERICA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['MEXICO', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)== set(['MEXICO', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)


#'MEXICO AND LATIN AMERICA'
rt='MEXICO AND LATIN AMERICA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['MEXICO', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)== set(['MEXICO', 'LATIN AMERICA']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

#'WUH, CAN, BJS, SHA OR HKG'
rt='WUH, CAN, BJS, SHA OR HKG'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert set(from_location)==set(['CAN', 'HKG', 'WUH', 'BJS', 'SHA']), "INCORRECT LOCATION:"+rt + 'was'+from_location
assert set(to_location)== set(['CAN', 'HKG', 'WUH', 'BJS', 'SHA']), "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

#ALL CX AND KA
rt='ALL CX AND KA'
from_location=get_from_location(rt)
to_location=get_to_location(rt)
assert from_location=='ALL CX AND KA', "INCORRECT LOCATION:"+rt + 'was'+from_location
assert to_location== 'ALL CX AND KA', "INCORRECT LOCATION:"+rt
print('from_location',from_location)
print('to_location',to_location)
print ('PASSED',rt)

print()

print('PASSED ALL ROUTE TESTS')
# 'BOS TO PVG': ('BOS', 'PVG'),
# 'CANADA TO': ('CANADA', None),
# 'CANADA TO ASIA': ('CANADA', 'ASIA'),
# 'CHI OR CID TO ASIA': (['CHI', 'CID'], 'ASIA'),
# 'LAX TO ASIA': ('LAX', 'ASIA'),
# 'LAX TO ASIA ONLY': ('LAX', 'ASIA'),
# 'SHA OR HKG': (['SHA', 'HKG'], ['SHA', 'HKG']),
# 'TOWUH, CAN, BJS,': (None, ['WUH', 'CAN', 'BJS']),
# 'USA ORLATIN AMERICA - ASIA': ('USA', 'ASIA'),
# 'CANADA – ASIA*': ('CANADA', 'ASIA'),
# 'JFK/EWR-AISA': (['EWR', 'JFK'], 'ASIA'),
# 'USA-AISA': ('USA', 'ASIA'),
# 'USA TO ASIA': ('USA', 'ASIA'),
# 'USA ASIA': ('USA', 'ASIA')
print('ROUTE COMPREHENSION CHECKS PASSED')