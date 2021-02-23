WITH NumCategories(ID, Num) as
    (SELECT Categories.ItemID, COUNT(Categories.Category)
    FROM Categories
    GROUP BY Categories.ItemID),
     CombinedBids(ID, Num) as
    (SELECT Bids.ItemID, COUNT(Bids.ItemID)
    FROM Bids
    GROUP BY Bids.ItemID)

SELECT SUM(CombinedBids.Num)
FROM NumCategories, CombinedBids
WHERE NumCategories.Num = 4 AND NumCategories.ID = CombinedBids.ID;


