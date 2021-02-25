SELECT COUNT(*)
FROM (SELECT DISTINCT Items.sellerID
      FROM Items, Bids
      WHERE Items.sellerID = Bids.bidderID);
