WITH NumCategories(ID, Num) as
    (SELECT Categories.itemID, COUNT(Categories.category)
    FROM Categories
    GROUP BY Categories.itemID)

SELECT COUNT(*)
FROM NumCategories
WHERE NumCategories.Num = 4;


