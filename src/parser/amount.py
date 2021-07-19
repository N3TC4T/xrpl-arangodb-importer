from decimal import Decimal

def dropsToXRP(value):
   return Decimal(value) / Decimal(1000000)

def XRPToDrops(value):
   return int(Decimal(value) * 1000000)
