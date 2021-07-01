import json
import ctypes

from .hex import toHex

_lib = ctypes.CDLL("../xrpl-deserializer-c/xd.so")
_lib.f.argtypes=[ctypes.c_char_p, ctypes.c_uint16]
_lib.f.restype = ctypes.c_char_p


def deser(tx_bytes):
   try:
      tx_bytes += bytes(chr(0), 'ascii')
      return json.loads(_lib.f(ctypes.c_char_p(tx_bytes), len(tx_bytes)))
   except Exception as e:
      print(e)
      print(toHex(tx_bytes))
      # print(_lib.f(ctypes.c_char_p(tx_bytes), len(tx_bytes)))
      return {}


# gcc main.c base58.c sha-256.c -O3  -fPIC -shared -o xd.so
# 
# char* f(uint8_t* raw, uint16_t len)
# {
#     b58_sha256_impl = calc_sha_256;
# 
#     uint8_t* output = 0;
#     if (!deserialize(&output, raw, len, 0, 0, 0))
#         return "";
# 
#     return ((char*) output);
# }
