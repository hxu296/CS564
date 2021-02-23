SELECT COUNT(*)
FROM (SELECT DISTINCT Items.SellerID
      FROM Items, Bids
      WHERE Items.SellerID = Bids.BidderID);
