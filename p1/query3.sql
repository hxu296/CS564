WITH NumCategories(ID, Num) as
    (SELECT Categories.ItemID, COUNT(Categories.Category)
    FROM Categories
    GROUP BY Categories.ItemID)

SELECT COUNT(*)
FROM NumCategories
WHERE NumCategories.Num = 4;


