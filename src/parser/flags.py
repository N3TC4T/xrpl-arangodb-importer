txFlags = {
   "Universal": {
      "FullyCanonicalSig": 0x80000000,
   },
   "AccountSet": {
      "RequireDestTag": 0x00010000,
      "OptionalDestTag": 0x00020000,
      "RequireAuth": 0x00040000,
      "OptionalAuth": 0x00080000,
      "DisallowXRP": 0x00100000,
      "AllowXRP": 0x00200000,
   },
   "TrustSet": {
      "SetAuth": 0x00010000,
      "NoRipple": 0x00020000,
      "SetNoRipple": 0x00020000,
      "ClearNoRipple": 0x00040000,
      "SetFreeze": 0x00100000,
      "ClearFreeze": 0x00200000,
   },
   "OfferCreate": {
      "Passive": 0x00010000,
      "ImmediateOrCancel": 0x00020000,
      "FillOrKill": 0x00040000,
      "Sell": 0x00080000,
   },
   "Payment": {
      "NoRippleDirect": 0x00010000,
      "PartialPayment": 0x00020000,
      "LimitQuality": 0x00040000,
   },
   "PaymentChannelClaim": {
      "Renew": 0x00010000,
      "Close": 0x00020000,
   },
   "NFTokenMint":{
      "Burnable":0x00000001,
      "OnlyXRP":0x00000002,
      "TrustLine":0x00000004,
      "Transferable":0x00000008,
      "ApproveTransfers":0x00000010,
      "IssuerCanCancelOffers":0x00000010,
      "IssuerApprovalRequired":0x00000020
   },
   "NFTokenCreateOffer":{
      "SellToken":0x00000001,
      "Approved":0x00000002
   }
}

txFlagIndices = {
   "AccountSet": {
      "asfRequireDest": 1,
      "asfRequireAuth": 2,
      "asfDisallowXRP": 3,
      "asfDisableMaster": 4,
      "asfAccountTxnID": 5,
      "asfNoFreeze": 6,
      "asfGlobalFreeze": 7,
      "asfDefaultRipple": 8,
      "asfDepositAuth": 9,
      "asfAuthorizedMinter": 10,
   },
};

accountRootFlags = {
   "DefaultRipple": 0x00800000,
   "DepositAuth": 0x01000000,
   "DisableMaster": 0x00100000,
   "DisallowXRP": 0x00080000,
   "GlobalFreeze": 0x00400000,
   "NoFreeze": 0x00200000,
   "PasswordSpent": 0x00010000,
   "RequireAuth": 0x00040000,
   "RequireDestTag": 0x00020000,
};

AccountFlags = {
   "passwordSpent": accountRootFlags["PasswordSpent"],
   "requireDestinationTag": accountRootFlags["RequireDestTag"],
   "requireAuthorization": accountRootFlags["RequireAuth"],
   "depositAuth": accountRootFlags["DepositAuth"],
   "disallowIncomingXRP": accountRootFlags["DisallowXRP"],
   "disableMasterKey": accountRootFlags["DisableMaster"],
   "noFreeze": accountRootFlags["NoFreeze"],
   "globalFreeze": accountRootFlags["GlobalFreeze"],
   "defaultRipple": accountRootFlags["DefaultRipple"],
};

AccountFlagIndices = {
   "requireDestinationTag": txFlagIndices["AccountSet"]["asfRequireDest"],
   "requireAuthorization": txFlagIndices["AccountSet"]["asfRequireAuth"],
   "depositAuth": txFlagIndices["AccountSet"]["asfDepositAuth"],
   "disallowIncomingXRP": txFlagIndices["AccountSet"]["asfDisallowXRP"],
   "disableMasterKey": txFlagIndices["AccountSet"]["asfDisableMaster"],
   "enableTransactionIDTracking": txFlagIndices["AccountSet"]["asfAccountTxnID"],
   "noFreeze": txFlagIndices["AccountSet"]["asfNoFreeze"],
   "globalFreeze": txFlagIndices["AccountSet"]["asfGlobalFreeze"],
   "defaultRipple": txFlagIndices["AccountSet"]["asfDefaultRipple"],
};
