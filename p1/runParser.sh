#! /bin/bash
cd ./dataloader
python ./parser.py ../ebay_data/items-*.json
cd ..
mv ./ebay_data/*.dat ./parsed_data
