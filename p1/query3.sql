WITH NumCategories(ID, Num) as
    (SELECT Categories.ItemID, COUNT(Categories.Category) AS Num_categories
    FROM Categories, Bids
    WHERE Categories.ItemID = Bids.ItemID
    GROUP BY Categories.ItemID)

SELECT COUNT(*)
FROM NumCategories
WHERE Num = 4;
