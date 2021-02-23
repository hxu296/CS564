WITH BidGreater(ID, bid) as
    (SELECT ItemID, Amount
    FROM Bids
    WHERE Amount > 100),
     CategoriesWithBids(ID, Category) as
    (SELECT Categories.*
     FROM Categories, BidGreater
     WHERE Categories.ItemID = BidGreater.ID),
     CategoryCount(Category, Cnt) as
    (SELECT Category, COUNT(ID)
     FROM CategoriesWithBids
     GROUP BY Category)

SELECT COUNT(*)
FROM CategoryCount
WHERE Cnt >= 1;