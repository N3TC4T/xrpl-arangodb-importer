from decimal import Decimal
from itertools import groupby


class Meta:
   def __init__(self, meta):
      self.nodes = []

      if  "AffectedNodes" in meta:
         self.nodes = [self.normalizeNode(v) for v in meta["AffectedNodes"]]

   def normalizeNode(self, affectedNode):
      diffType = next(iter(affectedNode))
      node = affectedNode[diffType]
      return {
         **node,
         "diffType": diffType,
         "entryType": node.get("LedgerEntryType"),
         "ledgerIndex": node.get("LedgerIndex"),
         "newFields": node.get("NewFields", {}),
         "finalFields": node.get("FinalFields", {}),
         "previousFields": node.get("PreviousFields", {}),
      }

   def groupByAddress(self, balanceChanges):
      result = {}

      keyfunc = lambda x: x['address']

      for address, grouped in groupby(sorted(balanceChanges, key=keyfunc), keyfunc):
         result[address] = []
         for g in grouped:
            result[address].append(g["balance"])

      return result


   def parseValue(self, value):
      _value = value.get("value") if isinstance(value, dict) else value
      return Decimal(_value)

   def computeBalanceChange(self, node):
      value = None
      if "Balance" in node["newFields"] :
         value = self.parseValue(node["newFields"]["Balance"])
      elif "Balance" in node["previousFields"] and "Balance" in node["finalFields"]:
         value = self.parseValue(node["finalFields"]["Balance"]) - self.parseValue(node["previousFields"]["Balance"])

      return None if value == None else None if value == 0 else value.normalize()

   def parseFinalBalance(self, node):
      if node["newFields"].get("Balance"):
         return self.parseValue(node["newFields"]["Balance"])
      if node["finalFields"].get("Balance"):
         return self.parseValue(node["finalFields"]["Balance"])

      return None

   def parseXRPQuantity(self, node, valueParser):
      value = valueParser(node)

      if value == None:
         return None

      return {
         "address": node["finalFields"].get("Account") or node["newFields"].get("Account"),
         "currency": 'XRP',
         "issuer": None,
         "value":  Decimal(value / Decimal(1000000.0))
      }

   def flipTrustlinePerspective(self, quantity):
      return {
         "address": quantity["issuer"],
         "issuer": quantity["address"],
         "currency": quantity["currency"],
         "value": Decimal(Decimal(quantity["value"]) * -1)
      }

   def parseTrustlineQuantity(self, node, valueParser):
      value = valueParser(node)

      if value == None:
         return None

      fields = node["finalFields"] if not bool(node["newFields"]) else node["newFields"]

      result = {
         "address": fields["LowLimit"]["issuer"],
         "issuer": fields["HighLimit"]["issuer"],
         "currency": fields["Balance"]["currency"],
         "value": value,
      }
      return [result, self.flipTrustlinePerspective(result)]

   def parseBalanceChanges(self):
      values = []
      for node in self.nodes:
         if node["entryType"] == 'AccountRoot':
            value = self.parseXRPQuantity(node, self.computeBalanceChange)
            if value:
               values.append([value])

         if node["entryType"] == 'RippleState':
            value = self.parseTrustlineQuantity(node, self.computeBalanceChange)
            if value:
               values.append(value)

      return sum(values, [])
