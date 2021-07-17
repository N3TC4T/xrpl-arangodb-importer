import os
import json
import ctypes

from .hex import toHex, fromHex

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_lib = ctypes.CDLL(os.path.join(ROOT_DIR, "deps/xrpl-deserializer-c/xd.so"))
_lib.de.argtypes=[ctypes.c_char_p, ctypes.c_uint16]
_lib.de.restype = ctypes.c_void_p

_lib.freeme.argtypes = ctypes.c_void_p,
_lib.freeme.restype = None

def deser(tx_bytes):
   try:
      tx_bytes += bytes(chr(0), 'ascii')
      res = _lib.de(ctypes.c_char_p(tx_bytes), len(tx_bytes))
      loaded = json.loads(ctypes.cast(res, ctypes.c_char_p).value)
      _lib.freeme(res)
      return loaded
   except Exception as e:
      print(e)
      print(toHex(tx_bytes))
      # print(_lib.f(ctypes.c_char_p(tx_bytes), len(tx_bytes)))
      return {}

# uint8_t *de(uint8_t* raw, uint16_t len)
# {
#     b58_sha256_impl = calc_sha_256;
# 
#     uint8_t* output = 0;
#     if (!deserialize(&output, raw, len, 0, 0, 0))
#         return 0;
# 
#     return output;
# }
# 
# void freeme(char *ptr)
# {
#     free(ptr);
#     ptr = NULL;
# }
