SELECT COUNT(*)
FROM Users, (SELECT SellerID
             FROM Items
             WHERE Item_Location LIKE "New York") AS Exclude

WHERE Users.User_Location LIKE "New York" AND NOT(Users.UserID LIKE Exclude.SellerID);