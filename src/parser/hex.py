import binascii


def toHex(x):
    if not type(x) == bytes:
        raise Exception("cannot convert to hex type " + str(type(x)))

    return str(binascii.hexlify(x), 'utf-8').upper()


def fromHex(x):
    if not type(x) == str:
        raise Exception("cannot convert to bytes type " + str(type(x)))

    return binascii.unhexlify(x)
