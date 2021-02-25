drop table if exists Items;
drop table if exists Categories;
drop table if exists Users;
drop table if exists Bids;


CREATE TABLE Items (
    itemID			INT		    UNIQUE NOT NULL,
	name			CHAR(255)	NOT NULL, 	  
	currently		DOUBLE,				 
	first_bid		DOUBLE,				
	bid_count		INT	 	    NOT NULL,
	buy_price		DOUBLE,				   
	sellerID		CHAR(255)   NOT NULL,
	ends			Datetime	NOT NULL,	 
	started			Datetime	NOT NULL,	
	description		CHAR(255),			  
	PRIMARY KEY (itemID),
	FOREIGN KEY (sellerID) 	    REFERENCES Users (userID)
);
 
CREATE TABLE Categories (
	itemID			INT		    NOT NULL,
	category		CHAR(255)	NOT NULL,	
	PRIMARY KEY (itemID, category),
	FOREIGN KEY (itemID) 	    REFERENCES Items (itemID)
);

CREATE TABLE Users (
	userID			CHAR(255)   UNIQUE NOT NULL,
	country 	    CHAR(255),
	location		CHAR(255),	
	rating			INT		    NOT NULL,
	PRIMARY KEY (userID)
);

CREATE TABLE Bids (
	bidderID		CHAR(255)	NOT NULL,	 
	itemID			INT		    NOT NULL,
	time			Datetime	NOT NULL,
	amount			DOUBLE	    NOT NULL,
	FOREIGN KEY (bidderID) 	    REFERENCES Users (userID),
	FOREIGN KEY (itemID) 	    REFERENCES Items (itemID)
);
