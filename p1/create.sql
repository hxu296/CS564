drop table if exists Items;
drop table if exists Categories;
drop table if exists Users;
drop table if exists Bids;


CREATE TABLE Items (
    	itemID			INT		UNIQUE NOT NULL,    --itemID
	name			CHAR(255)	NOT NULL, 	    --name
	currently		DOUBLE,				    --currently
	first_bid		DOUBLE,				    --first_bid
	bid_count		INT	 	NOT NULL,	    --bid_count
	buy_price		DOUBLE,				    --buy_price
	sellerID		CHAR(255)   	NOT NULL,	    --sellerID
	ends			Datetime	NOT NULL,	    --ends
	started			Datetime	NOT NULL,	    --started
	country	        	CHAR(255),			    --country
	location		CHAR(255),			    --location
	description		CHAR(255),			    --description
	PRIMARY KEY (itemID)
	--FOREIGN KEY (sellerID) 	REFERENCES Users (userID),
);
 
CREATE TABLE Categories (
	itemID			INT		NOT NULL,	    --itemID
	category		CHAR(255)	NOT NULL,	    --category
	PRIMARY KEY (itemID, category)
	--FOREIGN KEY (itemID) 	REFERENCES Items (itemID)
);

CREATE TABLE Users (
	userID			CHAR(255)	UNIQUE NOT NULL,    --userID
	country 	    	CHAR(255),			    --country
	location		CHAR(255),			    --location
	rating			INT		NOT NULL,	    --rating
	PRIMARY KEY (userID)
);

CREATE TABLE Bids (
	bidderID		CHAR(255)	NOT NULL,	    --bidderID
	itemID			INT		NOT NULL,	    --itemID
	time			Datetime	NOT NULL,	    --time
	amount			DOUBLE	        NOT NULL	    --amount
	--FOREIGN KEY (bidderID) 	REFERENCES Users (userID)	
	--FOREIGN KEY (itemID) 	REFERENCES Items (itemID)
);
