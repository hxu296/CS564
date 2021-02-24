WITH BidGreater(ID, bid) as
    (SELECT itemID, amount
    FROM Bids
    WHERE amount > 100),
     CategoriesWithBids(ID, Category) as
    (SELECT Categories.*
     FROM Categories, BidGreater
     WHERE Categories.itemID = BidGreater.ID),
     CategoryCount(Category, Cnt) as
    (SELECT Category, COUNT(ID)
     FROM CategoriesWithBids
     GROUP BY Category)

SELECT COUNT(*)
FROM CategoryCount
WHERE Cnt >= 1;
