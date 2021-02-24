#! /bin/sh
python ./parser.py ./ebay_data/items-*.json
cd ./ebay_data
cat *-item.dat | sort | uniq > item.dat
rm *-item.dat
cat *-category.dat | sort | uniq > category.dat
rm *-category.dat
cat *-bid.dat | sort | uniq > bid.dat
rm *-bid.dat
cat *-user.dat | sort | uniq > user.dat
rm *-user.dat



