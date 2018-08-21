from collections import namedtuple
import unicodedata
from bs4 import BeautifulSoup
import unicodedata

from nlp.tagger import *
from nlp.tools import *



client = MongoClient('localhost:27017')
db = client.raxdb

class FareFile(object):

    def __init__(self, filename, country):
        t1 = datetime.now()
        self.filename = filename
        self.country = country
        self.tagged_filename = self.process_filename()
        t2 = datetime.now()

        htmls = fetch_htmls(client, db_name='raxdb', collection_name="CX_fare_html", file_name=filename, cntry=country)
        self.html_string = ' '.join(htmls)
        self.htmls = fetch_htmls(client, "raxdb", "CX_fare_html", filename, country) # 5 Aug 2018 changed db to "raxdb" as string is required in 'dbname' in get_html
        self.html_string = ' '.join(self.htmls)
        self.soup = BeautifulSoup(self.html_string, 'html.parser')
        self.trs = self.soup.find_all("tr")
        # self.teststring=self._teststring()
        self.teststring = ','.join(map(str, self.text_new()))
        self.s1s2 = ''
        self.word_tokens_list = ''
        self.wordlist = ''
        self.long_words = ''
        self.tagged_hdrs = myTagger(self.teststring)
        # following variables may be overidden by specific fares in the Fare File
        # they capture any file level information
        t3 = datetime.now()
        # print('tagged body',(t3-t2).total_seconds())
        self.currency = self.find_currency()
        self.commision = 0
        self.sales_period = []
        self.travel_period = []
        self.extension = 0
        self.effective_date = ''
        self.extension_date = ''
        self.codeshare = ''
        self.codeshare_permitted = False
        t4 = datetime.now()
        # print('tagged periods',(t4-t3).total_seconds())
        # self.tourcode = self.find_tourcodes(self.tagged_hdrs)
        self.tourcodes = []
        self.farebasis = ''
        self.nettfare = 0
        self.grossfare = 0
        self.rbd = ''
        self.origin = ''
        self.destination = ''
        self.gateway = ''
        self.point_of_sale = self.pos()
        self.location = self.location(self.tagged_filename) + self.location(self.tagged_hdrs)
        self.carriers = self._carriers()
        self.paragraph_data = {}
        self.tabledata = {}
        t5 = datetime.now()
        # print('total processing',(t5-t1).total_seconds())

    def __repr__(self):
        # return f'''{{"filename": {self.filename}, "html_string":  {self.html_string}, }}'''
        return f'''{{"filename": {self.filename}, "string":  {self.teststring}, }}'''

    def test_func(self):
        return "We are here"

    def soup2(self):
        return BeautifulSoup(self.html_string, 'html.parser')

    def paras(self):
        return self.extractParagraphs(self.soup)

    def get_s1s2(self):
        color = '#F00'
        cleanText = self.get_cleanText_with_color_tag(color)
        return cleanText

    def CX_periods(self):
        tp = []
        sp = []
        res = {}
        DEBUG = False
        # color='black'
        # cleanText=self.get_cleanText_with_color_tag(color)
        # cleanText=','.join(map(str, self.text()))
        # txt=' '.join(cleanText)
        if DEBUG:
            print('TAGGER')
        # tagged=myTagger(cleanText)
        tagged = self.tagged_hdrs
        if DEBUG:
            print('in CX_periods')
            print(tagged)
            print('TP')
        tp_list = self.find_period_detail(tagged, 'TRAVELPERIOD')

        if DEBUG:
            print('tp_list:', tp_list)
        if tp_list:
            tp = tp_list[0]
            self.travel_period = tp
            res['TRAVELPERIOD'] = tp
        if DEBUG:
            print('SP')
        sp_list = self.find_period_detail(tagged, 'SALESPERIOD')
        if DEBUG:
            print('sp_list:', sp_list)
        if sp_list:
            sp = sp_list[0]
            self.sales_period = sp
            self.effective_date = sp[0]
            res['SALESPERIOD'] = sp
        if self.effective_date and self.extension_date:
            self.extension = (self.extension_date - self.effective_date).days
            res['extension_date'] = self.extension_date
            res['extension'] = self.extension
        return res

    def get_cleanText_with_color_tag(self, color):
        translation_table2 = dict.fromkeys(map(ord, "。'"), ' ')
        s1s2_list = self.extract_color_styles(self.soup, color)
        s1s2_list = [x for x in s1s2_list if '#' not in x]  # exclude unusual tags that crash the system
        souplist = []  # default incase they are in another color
        for style in s1s2_list:
            souplist += self.soup.select(style)
        # souplist=list(set(souplist)) #remove any duplicate s1 and s2
        cleanText = [(sent.text.translate(translation_table2)) for sent in souplist]
        return cleanText

    def extract_color_styles(self, soup, color):
        color_dict = {}
        try:
            style = soup.style
            css = cssutils.parseString(style.text)
            rules = css.cssRules
            for rule in rules:
                # print('selectorText:',rule.selectorText)
                try:
                    color_dict[rule.selectorText] = rule.style.color
                except AttributeError:
                    print("CSSComment object has no attribute style")
        except AttributeError:
            print("soup.style is a NoneType object and has no attribute text")

        return [key for (key, value) in color_dict.items() if value == color]

    def tables(self):
        # returns list of normalised tables, eliminating merged rows and columns
        normtables = []
        souptext = self.soup
        rawtables = souptext.find_all('table')
        for rawtable in rawtables:
            matrix = parse(rawtable)
            normtables.append(matrix)
        return normtables

    def text(self):
        souptext = self.soup
        hTagText = [tag.text for tag in souptext.find_all(re.compile('^h[1-6]$'))]
        pTagText = hTagText + [item.text for item in souptext.find_all('p') if item.text is not '']
        tdTagText = hTagText + [item.text for item in souptext.find_all('td') if item.text is not '']
        if len(pTagText) is not 0:
            return pTagText
        else:
            return tdTagText

    def text_new(self):
        """
        A function to get a list of text from html
        :param soup:
        :return:
        """
        souptext = self.soup
        hTagText = [tag.text.replace(u'\xa0', '').replace('\n', ' ').replace('\r', '') for tag in
                    souptext.find_all(re.compile('^h[1-6]$'))]
        pTagText = hTagText + [item.text.replace(u'\xa0', '').replace('\n', ' ').replace('\r', '') for item in
                               souptext.find_all('p') if item.text is not '']
        tdTagText = hTagText + [item.text.replace(u'\xa0', '').replace('\n', ' ').replace('\r', '') for item in
                                souptext.find_all('td') if item.text is not '']
        if len(pTagText) is not 0:
            return [text for text in pTagText if text]
        else:
            return [text for text in tdTagText if text]

    def _teststring(self):
        f1string = ','.join(map(str, self.text_new()))
        try:
            title = self.soup.title.text
            title = ' '.join(title.split('_'))
        except AttributeError:
            title = 'NONE'
        # try:
        #     h1=self.soup.h1.text
        # except AttributeError:
        #     h1=''
        # all_text='original_filename '+', '+title+ ', '+f1string
        all_text = f1string
        return all_text

    def rowtext(self, row):
        # returns text in specified row
        tr = self.trs[row]
        # print(tr)
        return tr.text

    def location(self, tagged_sentence):
        locations = list(find_leaves(tagged_sentence, ['LOCATION', 'CITY', 'AIRPORT']))
        return locations

    def _carriers(self):
        cnt = Counter()
        carrs_fn = list(find_leaves(self.tagged_filename, ['CARRIER']))
        carrs_body = list(find_leaves(self.tagged_hdrs, ['CARRIER']))
        return list(set(carrs_fn + carrs_body))  # to get unique list of carrier codes in this faresheet
        # return (carrs_fn + carrs_body)

    def find_flights(self, para_tagged, allowed_carrier_codes):
        res = []
        flight_numbers = []
        flight_nums = []
        carrier = []
        try:
            fx = find_subtrees(para_tagged, 'FLIGHTS_EXCLUDED')
        except AttributeError:
            fx = None

        if fx:
            carrier = find_leaves(fx[0], 'CARRIER')
        else:
            try:
                carrier = find_leaves(para_tagged, 'CARRIER')
            except AttributeError:
                return []
        if len(carrier) == 1:  # eg CX7680/7681/7682
            # print('CARRIER')
            [carrier] = carrier
            if carrier in allowed_carrier_codes:
                for flight_tag in find_subtrees(para_tagged, 'FLIGHTS'):
                    flight_nums += find_leaves(flight_tag, 'CD')
                    flight_numbers = [carrier + code for code in flight_nums]
                    # print('case1',flight_numbers)
            else:
                return []
        elif len(carrier) > 1:  # eg CX7680/CX7681/CX7682
            for flight_tag in find_subtrees(para_tagged, 'FLIGHT'):
                flight_nums += find_leaves(flight_tag, 'CD')
                # print('case2',flight_nums)
            flight_tags = find_subtrees(para_tagged, 'FLIGHT')
            list_extract = [find_leaves(tag, ['CARRIER', 'CD']) for tag in flight_tags]
            flight_numbers = [carrier + code for carrier, code in list_extract if carrier in allowed_carrier_codes]
            if flight_numbers == []:
                return []
        if fx:  # these are excluded flights, taged with minus
            res = [('-' + num) for num in flight_numbers]
        else:  # these are included flights, tagged with plus
            res = [('+' + num) for num in flight_numbers]
        return res

    def pos(self):
        location_list = self.location(self.tagged_hdrs)  # exclude filename
        if location_list:
            return location_list[0]  # for UL
        else:
            return location_list

    def find_period(self, tag):
        default = [datetime(2016, 1, 1, 0, 0), datetime(9999, 12, 31, 0, 0)]
        fn = self.find_period_detail(self.tagged_filename, tag)
        sales_periods_filename = [period for period in fn if period != default]

        try:
            body = self.find_period_detail(self.tagged_hdrs, tag)
        except KeyError:
            return sales_periods_filename
        if DEBUG:
            print('tagged filename', self.tagged_filename)
            print('from filename: ', fn)
            print('from body: ', body)
        sales_periods_body = [period for period in body if period != default]

        if sales_periods_body:  # check for empty list
            if DEBUG:
                print(tag, sales_periods_body)
            return sales_periods_body  # priority to data in the faresheet
        elif sales_periods_filename:  # if not look at file name for clues
            if DEBUG:
                print(tag, sales_periods_filename)
            return sales_periods_filename
        else:
            return [default]

    def find_travel_period(self):
        default = [datetime(2016, 1, 1, 0, 0), datetime(9999, 12, 31, 0, 0)]
        fn = self.find_period(self.tagged_filename, 'TRAVELPERIOD')
        # print('tagged filename',self.tagged_filename)
        # print('from filename: ',fn)
        body = self.find_period(self.tagged_hdrs, 'TRAVELPERIOD') + fn
        # print('from body: ',body)
        sales_periods = [period for period in body if period != default]
        if not sales_periods:  # check for empty list
            sales_periods = [default]
        return sales_periods

    def find_period_detail(self, chunk, tag):
        res = []
        DEBUG = False
        if DEBUG:
            print('in find_period_detail , looking for ', tag)
            print(chunk)
        try:
            # periods=[subtree for subtree in chunk.subtrees(filter=lambda t: t.label().tag_equals(tag))]
            periods = find_subtrees(chunk, tag)
        except ValueError:  # tag not found
            return [[datetime(2016, 1, 1), datetime(9999, 12, 31)]]
        if DEBUG:
            print('periods', periods)
        if not periods:  # PERIOD not found
            return [[datetime(2016, 1, 1), datetime(9999, 12, 31)]]
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

                    td = find_subtrees(r, 'TODATE')[0]
                    fd = find_subtrees(r, 'FROMDATE')[0]
                    # process TODATE first, as likely to have YEAR information
                    if td:
                        to_date = extractDateXX(Tree('S', td), DEBUG=DEBUG)
                        if DEBUG:
                            print('to_date', to_date)
                    else:
                        to_date = datetime(9999, 12, 31)
                    if DEBUG:
                        print('rang: ', r, 'td: ', td, 'fd: ', fd)

                    if fd:
                        if DEBUG:
                            print('In find_period_detail')
                            print('to_date', to_date, 'fd', fd)
                        from_date = extractDateXX(Tree('S', fd), to_date.year, DEBUG=DEBUG)
                        if DEBUG:
                            print('from_date', from_date)
                    else:
                        from_date = datetime(2016, 1, 1)
                    res.append([from_date, to_date])
            else:
                try:
                    [td] = find_subtrees(period, 'TODATE')
                except ValueError:
                    td = []
                try:
                    [fd] = find_subtrees(period, 'FROMDATE')
                except ValueError:
                    fd = []
                # process TODATE first, as likely to have YEAR information
                if td:
                    to_date = extractDateXX(Tree('S', td), DEBUG=DEBUG)
                    if DEBUG:
                        print('to_date', to_date)
                else:
                    to_date = datetime(9999, 12, 31)

                if DEBUG:
                    print('rang: ', '', 'td: ', td, 'fd: ', fd)

                if fd:
                    from_date = extractDateXX(Tree('S', fd), to_date.year, DEBUG=DEBUG)
                    if DEBUG:
                        print('from_date', from_date)
                else:
                    from_date = datetime(2016, 1, 1)
                res.append([from_date, to_date])

        return res

    def find_tkt_dis_date_txt(self, txt):
        chunk = myTagger(txt)
        return self.find_tkt_dis_date_tagged(self, chunk)

    def find_tkt_dis_date_tagged(self, chunk):
        return self.find_chunk_dis_date_tagged(chunk, 'TKT_DIS_DATE')

    def find_trv_dis_date_tagged(self, chunk):
        return self.find_chunk_dis_date_tagged(chunk, 'TRV_DIS_DATE')

    def find_chunk_dis_date_tagged(self, chunk, tag):
        default_period = [[datetime(2016, 1, 1, 0, 0), datetime(9999, 12, 31, 0, 0)]]
        date_list = self.find_period_detail(chunk, tag)
        if date_list == default_period:
            return None
        return date_list[0][1]

    def find_header_rows(self):
        # Works out what are the header rows by looking at colors
        return [0, 5]
        mysoup = self.soup
        trs = mysoup.find_all("tr")
        max_row = 0
        previous_color = ''
        # look for headers based on colours
        hdrs = []
        for rownum, row in enumerate(trs):
            tds = row.find_all('td')
            for el in tds:
                if el.has_attr('bgcolor'):
                    color = el['bgcolor']
                    # print(rownum, color, el.text)
                    if rownum == 0:
                        previous_color = color
                        max_row = rownum
                    else:
                        # print('Previous: {}  Current: {} '.format(previous_color,color))
                        if previous_color == color:
                            # print(rownum, color, 'same')
                            max_row = rownum
                        else:
                            previous_color = color
                            hdrs.append(max_row)

                else:
                    if previous_color:
                        hdrs.append(max_row)
                        previous_color = ''

        return hdrs

    def process_hdrs(self):
        # hdrs=self.find_header_rows()
        # [print(fare.rowtext(row)) for row in range(0,hdrs[-1:][0])]
        # hdr_txt = ". ".join([self.rowtext(row) for row in range(0,hdrs[-1:][0])])
        # hdr_txt = [fare.rowtext(row) for row in range(0,hdrs[-1:][0])]

        # hdr_txt=self.paras()
        hdr_txt = self.text_new()  # look at everything written down
        tagged_sentences = [myTagger(sentence) for sentence in hdr_txt]

        return Tree('LINE', tagged_sentences)

    def process_filename(self):
        fn_list = self.filename.split('_')
        fn = ' '.join(fn_list)
        return Tree('S', myTagger(fn))

    def print_hdrs(self):
        print('TEXT for ')
        print(self.filename)
        hdr_txt = self.paras()
        return hdr_txt

    def find_currency(self):
        t = self.tagged_hdrs
        cnt = Counter()
        currlist = [text for text, tag in t.leaves() if tag == 'CURRENCYCODE']
        if not currlist:
            return None
        for cur in currlist:
            cnt[cur] += 1
        i = len(cnt)
        code, cnt = cnt.most_common(i)[0]
        return code

    # def find_currency(self):
    #     tokenlist=sub_leaves(self.tagged_hdrs,'CURRENCY')
    #     if tokenlist:
    #         curr,tag = tokenlist[0]
    #         return curr
    #     else:
    #         locs=set(fare.location)
    #         u = set.intersection(locs, CURRENCY)
    #         if u:
    #             return list(u)[0]
    #         else:
    #             return None

    def find_tourcodes(self, chunk):
        DEBUGTOURCODE = False
        tl_subtrees = find_subtrees(chunk, 'TOURCODELOCATION')
        tc_subtrees = find_subtrees(chunk, 'TOURCODE')
        if DEBUGTOURCODE:
            print('TOURCODELOCATION', tl_subtrees)
            print('TOURCODE', tc_subtrees)
        res = []
        if tl_subtrees:  # 'TOURCODELOCATION'
            if DEBUGTOURCODE:
                print('TOURCODELOCATION', tl_subtrees)
            for st in tl_subtrees:
                [tc_leaves] = sub_leaves(st, 'TOURCODE')
                # tc=[('–', 'NN')]
                tourcode, x = tc_leaves[0]

                if DEBUGTOURCODE:
                    print('in tl_subtrees', tc_leaves)
                    print('tourcode', tourcode, 'tag', x)
                # tourcode, x = tc_leaves[first_chunk_index(tc_leaves, tag_equals('TC'))]
                [tc_leaves] = sub_leaves(st, 'LOCATION')
                loc1 = [word for word, tag in tc_leaves]
                location = " ".join(str(x) for x in loc1)  # handle ['Hong', 'Kong']
                # location,x=tc_leaves[0] #select closest
                if DEBUGTOURCODE:
                    print('LOCATION LEAVES', tc_leaves)
                    print('LOCATION ', location)
                if tourcode != '–':
                    res += [(tourcode, location)]
            return res
        elif tc_subtrees:  # 'TOURCODE'
            for st in tc_subtrees:
                if DEBUGTOURCODE:
                    print('st', st)
                # tc_leaves=Tree('S',st).leaves()
                # print('tc_leaves',tc_leaves )
                token = [tk for tk, tag in st.leaves() if tag == 'TC']
                if DEBUGTOURCODE:
                    print('Tourcode TC=', token)
                if token:
                    [tourcode] = token
                else:
                    break
                location = None
                res += [(tourcode, location)]
            return res
        else:
            return res

    def extractParagraphs(self, soup):
        txt = self.extractPtags(soup)
        fixedTxt = txt
        table_soup = soup.find_all('table')
        blankline = re.compile('\s+')

        # use unicode library to remove \Xa0 from strings: https://stackoverflow.com/questions/10993612/python-removing-xa0-from-string
        paragraph = []
        if not table_soup:
            paragraph = txt  # there are no tables in this document

        tableNo = 1
        # paragraph_no=1
        for rawtable in table_soup:
            table_rows = self.extractPtags(rawtable)
            lines = []
            for row in fixedTxt:
                if table_rows != []:
                    if txt[0] == table_rows[0]:
                        table_rows.pop(0)
                        txt.pop(0)
                    else:
                        para_text = [txt.pop(0)]
                        lines += para_text
                        paragraph += para_text

            # else:
            table_rows = parse(rawtable)
            self.paragraph_data[tableNo] = lines
            self.tabledata[tableNo] = table_rows
            paragraph += ['<table ' + str(tableNo) + '>']  # bookmark location of table

            tableNo += 1
        final_paragraph = [item for item in paragraph if item is not ' ']
        return final_paragraph

    def extractPtags(self, soup):
        txt3 = [item.text for item in soup.find_all('p')]
        # txt3 = [item.text for item in soup.find('p') ]
        txt2 = [item for item in txt3 if item is not '']
        txt = [unicodedata.normalize("NFKD", item) for item in txt2]
        return txt


class Fare_Row(object):
    def __init__(self, tourcodes,
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
                 pos):  # **add_column**
        self.tourcodes = tourcodes
        # if excluded:
        #     self.originating=['EXCLUDING',originating]
        # else:
        self.originating = originating
        # self.excluded=excluded
        self.route = route
        self.flights = flights
        self.rbd_cleaned = [x[0] for x in rbds.split('/')]  # clean up trailing comments eg ''J/C/D/I– PER RULE''
        self.rbd = iter(self.rbd_cleaned)
        self.fbc = fbc
        disc = re.compile(r'\d+.*\d*\s*%$')
        d = disc.match(discount)
        self.discount = int(d.group(0)[:-1]) if d else discount
        d = disc.match(corporate_discount)
        self.corporate_discount = int(d.group(0)[:-1]) if d else corporate_discount
        self.discount_unit = 'P' if d else 'T'  # P= percentage T= per Ticket
        self.fare_basis = rbds + '-TYPE'
        self.sales_period_from = str_format_date(sales_period[0][0])
        self.sales_period_to = str_format_date(sales_period[0][1])
        self.trv_dis_date = str_format_date(trv_dis_date)  # may need to treat in same way as TKT_DIS_DATE
        self.tkt_dis_date = str_format_date(tkt_dis_date)
        self.pos = pos
        test_tkt_dis_date = extractDate_single(route, 'TKT_DIS_DATE')  # check for tkt discontinue comments in route
        test_trv_dis_date = extractDate_single(route, 'TRV_DIS_DATE')
        if test_tkt_dis_date or test_trv_dis_date:
            self.tkt_dis_date = str_format_date(test_tkt_dis_date)
            self.test_trv_dis_date = str_format_date(test_trv_dis_date)
            self.route = self.extract_leaf_string(route, 'ORIGIN')  # clean up any ticketing comments
        self.rtw_fare_comm = rtw_fare_comm
        self.ct_fare_comm = ct_fare_comm

        def extract_leaf_string(self, txt, tag):
            # returns a string of leaf words separated by a space chunked by the tag
            tagged = myTagger(txt)
            OT = find_subtrees(tagged, tag)
            branches = [x.leaves() for x in OT]
            leaves = [word for branch in branches for word, tag in branch]
            return ' '.join(leaves)

def str_format_date(txt):
    if txt:
        return txt.strftime('%Y-%m-%d')
    else:
        return ''









