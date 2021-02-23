WITH NumCategories(ID, Num) as
    (SELECT Categories.ItemID, COUNT(Categories.Category) AS Num_categories
    FROM Categories
    GROUP BY Categories.ItemID)

SELECT COUNT(*)
FROM NumCategories, Bids
WHERE NumCategories.Num = 4 AND NumCategories.ID = Bids.ItemID;
