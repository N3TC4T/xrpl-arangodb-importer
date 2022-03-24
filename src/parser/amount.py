from decimal import Decimal
from math import frexp

def drops_to_xrp(value):
   return Decimal(value) / Decimal(1000000)
