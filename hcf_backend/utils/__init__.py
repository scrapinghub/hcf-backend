import hashlib
import six


def convert_from_bytes(data):
    if data is not None:
        data_type = type(data)
        if data_type == bytes:
            return data.decode('utf8')
        if data_type in (str, int, bool):
            return data
        if data_type == dict:
            data = data.items()
        return data_type(map(convert_from_bytes, data))


def convert_to_bytes(data):
    if data is not None:
        data_type = type(data)
        if data_type == str:
            return data.encode('utf8')
        if data_type in (bytes, int, bool):
            return data
        if data_type == dict:
            data = data.items()
        return data_type(map(convert_to_bytes, data))


def get_hcf_fingerprint(request):
    return request.meta.get(b'frontier_fingerprint', request.url)


def hash_mod(text, divisor):
    """
    returns the module of dividing text md5 hash over given divisor
    """
    if isinstance(text, six.text_type):
        text = text.encode('utf8')
    md5 = hashlib.md5()
    md5.update(text)
    digest = md5.hexdigest()
    return int(digest, 16) % divisor


def assign_slotno(path, numslots):
    """
    Standard way to assign slot number from url path
    """
    return str(hash_mod(path, numslots))