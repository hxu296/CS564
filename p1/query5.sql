SELECT COUNT(*)
FROM (SELECT DISTINCT Users.UserID
      FROM Items, Users
      WHERE Items.SellerID = Users.UserID AND Users.Rating > 1000);