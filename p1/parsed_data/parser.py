
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014
Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:
1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.
Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

# Table attributes
ITEM_ID, NAME, CURRENTLY, FIRST_BID, NUMBER_OF_BIDS, BUY_PRICE, SELLER_ID,\
    ENDS, STARTED, DESCRIPTION, CATEGORY, USER_ID, COUNTRY, LOCATION, RATING,\
    BID_ID, BIDDER_ID, TIME, AMOUNT = range(19)

# Table attributes orders
item_attr_order = [ITEM_ID, NAME, CURRENTLY, FIRST_BID, NUMBER_OF_BIDS, BUY_PRICE,\
    SELLER_ID, ENDS, STARTED, DESCRIPTION]
category_attr_order = [ITEM_ID, CATEGORY]
user_attr_order = [USER_ID, COUNTRY, LOCATION, RATING]
bid_attr_order = [BIDDER_ID, ITEM_ID, TIME, AMOUNT]

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Transform an entity table to a unique, SQL-interpretable-formatted string array
"""
def transformTable(table, order):
    arr = list()
    for entry in table:
        string = ''
        for attr in order:
            string += str(entry[attr]) if entry[attr] is not None else 'NULL'
            string += columnSeparator
        string = string[:-len(columnSeparator)] # discard columnSeparator at the end of the string
        arr.append(string)
    return list(set(arr))

"""
dump arr into a file named output_name
"""
def dumpArray(output_name, arr):
    with open(output_name, 'w') as f:
        for entry in arr:
            f.write(entry + '\n')

"""
replace each single double quote with a couple of double quotes and 
add double quote to the start and end of string
"""
def escapeDoubleQuote(string):
    return '"{}"'.format(sub(r'"', '""', string))

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    # A tables is represented by a list of dictionaries
    item_table, category_table, user_table, bid_table = (list() for i in range(4))
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:
            # parse item_table attributes from item
            item_entry = dict()
            item_id = item_entry[ITEM_ID] = int(item['ItemID'])
            item_entry[SELLER_ID] = item['Seller']['UserID']
            item_entry[NAME] = escapeDoubleQuote(item['Name'].strip())
            item_entry[DESCRIPTION] = escapeDoubleQuote(item['Description'].strip()) if item['Description'] is not None\
                else None
            item_entry[NUMBER_OF_BIDS] = int(transformDollar(item['Number_of_Bids']))
            item_entry[FIRST_BID] = transformDollar(item['First_Bid'])
            item_entry[CURRENTLY] = transformDollar(item['Currently'])
            item_entry[BUY_PRICE] = transformDollar(item['Buy_Price']) if 'Buy_Price' in item else None
            item_entry[STARTED] = transformDttm(item['Started'])
            item_entry[ENDS] = transformDttm(item['Ends'])
            item_table.append(item_entry)
            # parse category_table attributes from item
            category_table.extend([{CATEGORY:category, ITEM_ID:item_id} for category in item['Category']])
            # parse user_table and bid_table attributes from item
            user_table.append({USER_ID:item['Seller']['UserID'], RATING:item['Seller']['Rating'],
                              LOCATION:item['Location'], COUNTRY:item['Country']})
            if item['Bids'] is not None:
                for bid in item['Bids']:
                    bid = bid['Bid']
                    bid_entry = dict()
                    bidder = bid['Bidder']
                    bidder_id = bid_entry[BIDDER_ID] = bidder['UserID']
                    amount = bid_entry[AMOUNT] = transformDollar(bid['Amount'])
                    time = bid_entry[TIME] = transformDttm(bid['Time'])
                    bid_entry[ITEM_ID] = item_id
                    #bid_entry[BID_ID] = hash('{}{}{}{}'.format(item_id, bidder_id, amount, time))
                    bid_table.append(bid_entry)
                    user_entry = dict()
                    user_entry[USER_ID] = bidder['UserID']
                    user_entry[LOCATION] = bidder['Location'] if 'Location' in bidder else None
                    user_entry[COUNTRY] = bidder['Country'] if 'Country' in bidder else None
                    user_entry[RATING] = bidder['Rating']
                    user_table.append(user_entry)
    # transform tables into unique, SQL interpretable string arrays
    item_arr = transformTable(item_table, item_attr_order)
    category_arr = transformTable(category_table, category_attr_order)
    user_arr = transformTable(user_table, user_attr_order)
    bid_arr = transformTable(bid_table, bid_attr_order)
    # dump string arrays into .dat files
    output_name = json_file[:-5]
    dumpArray(output_name + '-item.dat', item_arr)
    dumpArray(output_name + '-category.dat', category_arr)
    dumpArray(output_name + '-user.dat', user_arr)
    dumpArray(output_name + '-bid.dat', bid_arr)

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print('Usage: python skeleton_json_parser.py <path to json files>', file=sys.stderr)
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print("Success parsing " + f)

if __name__ == '__main__':
    main(sys.argv)