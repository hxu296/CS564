drop table if exists Items;
drop table if exists Categories;
drop table if exists Users;
drop table if exists Bids;


CREATE TABLE Items (
    ItemID			INT		    UNIQUE NOT NULL,
	Name			CHAR(255)	NOT NULL,
	Currently		DOUBLE,
	First_Bid		DOUBLE,
	Number_of_Bids	INT		    NOT NULL,
	Buy_Price		DOUBLE,
	SellerID		INT		    NOT NULL,
	Ends			Datetime	NOT NULL,
	Started			Datetime	NOT NULL,
	Description		CHAR(255)	NOT NULL,
	PRIMARY KEY (ItemID),
	FOREIGN KEY (SellerID) REFERENCES User (UserID),
	FOREIGN KEY (ItemID) REFERENCES Bid (ItemID),
	FOREIGN KEY (ItemID) REFERENCES Category (ItemID)
);
 
CREATE TABLE Categories (
	ItemID		INT		        NOT NULL,
	Category	CHAR(255)	    NOT NULL,
	PRIMARY KEY (ItemID, Category),
	FOREIGN KEY (ItemID) REFERENCES Item (ItemID)
);

CREATE TABLE Users (
	UserID		CHAR(255)	    UNIQUE NOT NULL,
	Country	    CHAR(255)	    NOT NULL,
	Location	CHAR(255)	    NOT NULL,
	Rating		INT(DOUBLE?)	NOT NULL,
	PRIMARY KEY (UserID)
);

CREATE TABLE Bids (
	BidID		INT		        UNIQUE NOT NULL,
	BidderID	CHAR(255)	    NOT NULL,
	ItemID		INT		        NOT NULL,
	Time		Datetime	    NOT NULL,
	Amount	DOUBLE	            NOT NULL,
	PRIMARY KEY (BidID),
	FOREIGN KEY (BidderID) REFERENCES User (UserID)	
	FOREIGN KEY (ItemID) REFERENCES Item (ItemID)
);
