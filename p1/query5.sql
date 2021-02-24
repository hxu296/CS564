SELECT COUNT(*)
FROM (SELECT DISTINCT Users.userID
      FROM Items, Users
      WHERE Items.sellerID = Users.userID AND Users.rating > 1000);
