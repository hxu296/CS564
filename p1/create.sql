drop table if exists Items;
drop table if exists Categories;
drop table if exists Users;
drop table if exists Bids;


CREATE TABLE Items (
    	ItemID			INT		UNIQUE NOT NULL,    --itemID
	Name			CHAR(255)	NOT NULL, 	    --name
	Currently		DOUBLE,				    --currently
	First_Bid		DOUBLE,				    --first_bid
	Number_of_Bids		INT	 	NOT NULL,	    --bid_count
	Buy_Price		DOUBLE,				    --buy_price
	SellerID		CHAR(255)   	NOT NULL,	    --sellerID
	Ends			Datetime	NOT NULL,	    --ends
	Started			Datetime	NOT NULL,	    --started
	Item_Country	    	CHAR(255),			    --country
	Item_Location		CHAR(255),			    --location
	Description		CHAR(255),			    --description
	PRIMARY KEY (ItemID),
	FOREIGN KEY (SellerID) 	REFERENCES User (UserID),
	FOREIGN KEY (ItemID) 	REFERENCES Bid (ItemID),
	FOREIGN KEY (ItemID) 	REFERENCES Category (ItemID)
);
 
CREATE TABLE Categories (
	ItemID			INT		NOT NULL,	    --itemID
	Category		CHAR(255)	NOT NULL,	    --category
	PRIMARY KEY (ItemID, Category),
	FOREIGN KEY (ItemID) 	REFERENCES Item (ItemID)
);

CREATE TABLE Users (
	UserID			CHAR(255)	UNIQUE NOT NULL,    --userID
	User_Country	    	CHAR(255),			    --country
	User_Location		CHAR(255),			    --location
	Rating			INT		NOT NULL,	    --rating
	PRIMARY KEY (UserID)
);

CREATE TABLE Bids (
	BidderID		CHAR(255)	NOT NULL,	    --bidderID
	ItemID			INT		NOT NULL,	    --itemID
	Time			Datetime	NOT NULL,	    --time
	Amount			DOUBLE	        NOT NULL,	    --amount
	FOREIGN KEY (BidderID) 	REFERENCES User (UserID)	
	FOREIGN KEY (ItemID) 	REFERENCES Item (ItemID)
);
