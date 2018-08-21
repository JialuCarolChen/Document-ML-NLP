import re, collections, calendar
from collections import Counter
#############################################
# Spell checker                             #
#############################################

def words(text):
    return re.findall('[A-Z]+', text.upper())

def words1(text):
    return re.findall('[a-z]+', text.lower())

def train2(features):
    model = collections.defaultdict(lambda: 1)
    for f in features:
        model[f] += 1
    return model

def train(features, model= collections.defaultdict(lambda: 1)):
    #model = collections.defaultdict(lambda: 1)
    for f in features:
        model[f] += 1
    return model



# NWORDS = train(words(open('sherlock.txt','r').read()))
ENG_WORDS = train(words(open('./data/big.txt', 'r').read()))
#NWORDS = train(words(open('sherlock.txt','r').read()))
NWORDS = train(words(open('./data/big.txt','r').read()))
NWORDS['ASIA']+=1
NWORDS['WUH']+=1000
NWORDS['LATIN AMERICA']+=1000
NWORDS['-']+=1
NWORDS = train(words(open('./data/IATA-airport-city-ref.csv', 'r').read()))

# NWORDS=collections.defaultdict(lambda: 1)
# alphabet = 'abcdefghijklmnopqrstuvwxyz'

# alphabet = '0123456789'
alphabet = '-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
# alphabet='AR317TF586029'
CODES = train(words(open('./raxutil/data/IATA-airport-city-ref.csv', 'r').read()))
CODES = train(words(open('./raxutil/data/currency.csv', 'r').read()), CODES)
CODES = train(words(open('./raxutil/data/codeshare.txt', 'r').read()), CODES)
KNOWN = ['UFN']  # turn this into a training cycle

def edits1(word):
    s = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    deletes    = [a + b[1:] for a, b in s if b]
    transposes = [a + b[1] + b[0] + b[2:] for a, b in s if len(b)>1]
    replaces   = [a + c + b[1:] for a, b in s for c in alphabet if b]+[a + c.upper() + b[1:] for a, b in s for c in alphabet if b]
    inserts    = [a + c + b     for a, b in s for c in alphabet]+[a + c.upper() + b     for a, b in s for c in alphabet]
    return set(deletes + transposes + replaces + inserts)

def edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1))


def edits3(word):
    x = (set(e3 for e1 in edits1(word) for e2 in edits1(e1) for e3 in edits1(e2)))
    print('3 edits:', len(x), 'possibilities')
    #print('AR217RTFF809 in set', 'AR217RTFF809' in x)
    return x


def edits4(word):
    x = (set(e4 for e1 in edits1(word)
             for e2 in edits1(e1)
             for e3 in edits1(e2)
             for e4 in edits1(e3)))
    print('4 edits:', len(x), 'possibilities')
    #print('AR217RTFF809 in set', 'AR217RTFF809' in x)
    return x

def known_edits2(word, NWORDS):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1) if e2 in NWORDS)

def known_edits3(word, NWORDS):
    print(set([e3 for e1 in edits1(word) for e2 in edits1(e1) for e3 in edits1(e2) if e3 in NWORDS]))
    return set(e3 for e1 in edits1(word) for e2 in edits1(e1) for e3 in edits1(e2) if e3 in NWORDS)

def known(words,NWORDS):
    return set(w for w in words if w in NWORDS)

def correct(word, NWORDS):
    candidates = known([word], NWORDS) or known(edits1(word), NWORDS) or known_edits2(word, NWORDS) or known_edits3(
        word, NWORDS) or [word]
    # print(candidates)
    return max(candidates, key=NWORDS.get)

def correct2(word, NWORDS):
    candidates = (known([word], NWORDS)
                  or known(edits1(word), NWORDS)
                  or known(edits2(word), NWORDS)
                  or [word])
    # print(candidates)
    return max(candidates, key=NWORDS.get)

def correct3(word, NWORDS):
    candidates = (known([word], NWORDS)
                  or known(edits1(word), NWORDS)
                  or known(edits2(word), NWORDS)
                  or known(edits3(word), NWORDS)
                  or [word])
    # print(candidates)
    return max(candidates, key=NWORDS.get)


def correct4(word, NWORDS):
    candidates = (known([word], NWORDS)
                  or known(edits1(word), NWORDS)
                  or known(edits2(word), NWORDS)
                  or known(edits3(word), NWORDS)
                  or known(edits4(word), NWORDS)
                  or [word])
    # print(candidates)
    return max(candidates, key=NWORDS.get)

def correct_CXtourcode(word, NWORDS):
    # candidates = known([word],NWORDS) or known(edits1_CX(word),NWORDS) or    known_edits2_CX(word,NWORDS) or [word]
    # candidates =    [known([word],NWORDS) or known(edits1_CX(word),NWORDS) or known_edits2(word,NWORDS) or known_edits3(word,NWORDS) or known([word])]
    # 5MAY18 only correct for 1 edit and show all candidates
    [candidates] = [known([word], NWORDS) or known(edits1(word), NWORDS) or {word}]
    return candidates


def edits1_CX(word):
    ff_pos = word.find('FF') + 2  # only look for spelling errors after FF
    s = [(word[:i], word[i:]) for i in range(ff_pos, len(word) + 1)]
    deletes = [a + b[1:] for a, b in s if b]
    transposes = [a + b[1] + b[0] + b[2:] for a, b in s if len(b) > 1]
    replaces = [a + c + b[1:] for a, b in s for c in alphabet if b] + [a + c.upper() + b[1:] for a, b in s for c in
                                                                       alphabet if b]
    inserts = [a + c + b for a, b in s for c in alphabet] + [a + c.upper() + b for a, b in s for c in alphabet]
    return set(deletes + transposes + replaces + inserts)


def known_edits2_CX(word, NWORDS):
    return set(e2 for e1 in edits1_CX(word) for e2 in edits1_CX(e1) if e2 in NWORDS)



#############################################
# Calendar Spell Checker                    #
#############################################

cal2={v.upper(): k for k,v in enumerate(calendar.month_abbr)}
cal2.update({v: k for k,v in enumerate(calendar.month_abbr)})
cal2.update({v.upper(): k for k,v in enumerate(calendar.month_name)})
cal2.update({v: k for k,v in enumerate(calendar.month_name)})
del cal2[""] #remove blank string keyupdat
cal={}
calkeys = set(cal2.keys())
for key in calkeys:
    cal[key]=cal2[key]
    #alternate spellings for months
    eds= edits1(key)
    for alt_spell in eds:
        if alt_spell not in calkeys:
            cal.update({alt_spell: cal2[key]})

month_number_to_month={k:v.upper() for k,v in enumerate(calendar.month_abbr)}

def isalpha(word):
    return word.isalpha()

def istitle(word):
    return word.istitle()

def isupper(word):
    return word.isupper()

def islower(word):
    return word.islower()

def isdigit(word):
    return word.isdigit()

def alphait(word):
    return word

def titleit(word):
    return word.title()

def upperit(word):
    return word.upper()

def lowerit(word):
    return word.lower()

def digitit(word):
    return word

casetests=[isalpha, istitle, isupper, islower, isdigit]
caseit=[alphait,titleit,upperit,lowerit, digitit]

def myReduce(fn,word):
    for x in fn:
        word=x(word)
    return word

def spellchecker(word):
    if not word.isalpha():
        return word
    if word.lower() in CODES:
        return word
    if word in KNOWN:
        return word
    try:
        month_number=cal[word]
        return month_number_to_month[month_number]
    except KeyError:  #not a month
        m=map(lambda x: x(word), casetests) #store the case of the word
        ct=[y for x,y in zip(m,caseit) if x] #prepare to reinstate the case
        checkedword= correct(word.lower(), NWORDS) #check using lowercase
        if len(checkedword) != len(word) and word.isupper():
            return word #This is most likely a CODE
        word=myReduce(ct,checkedword) #reinstate original case
        return word

