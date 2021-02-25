# Ebay Auction Database

- Task A: Examine the Json data files
- Task B: Design your relational schema
    - [Entity-Relation (ER) diagram](Relational_Schema.jpg)
    - [Relational Schema](Relational_Schema.docx)
- Task C: Write a data transformation program
    - run [runParser.sh](runParser.sh) to generate parsed .dat files in the [parsed_data](parsed_data) directory
- Task D: Load your data into SQLite
    - run the following commands to populate AuctionBase.db
      ```SHELL 
      ./runParser.py
      sqlite3 AuctionBase.db < create.sql
      sqlite3 AuctionBase.db < load.txt
      ```
- Task E: Test your SQLite Database
  - run the following commands for testing
    ```SHELL
    sqlite3 AuctionBase.db < query1.sql # expect 13422
    sqlite3 AuctionBase.db < query2.sql # expect 80
    sqlite3 AuctionBase.db < query3.sql # expect 8365
    sqlite3 AuctionBase.db < query4.sql # expect 1046871451 
    sqlite3 AuctionBase.db < query5.sql # expect 3130 
    sqlite3 AuctionBase.db < query6.sql # expect 6717 
    sqlite3 AuctionBase.db < query7.sql # expect 150
    ```

