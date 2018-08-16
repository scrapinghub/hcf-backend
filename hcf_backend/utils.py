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
