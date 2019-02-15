def format_string(key):
    if isinstance(key, str):
        key = key.replace(',', r'\,')
        key = key.replace(' ', r'\ ')
        key = key.replace('=', r'\=')
    return key


def format_value(value):
    if isinstance(value, str):
        value = value.replace('"', '\"')
        value = u'"{0}"'.format(value)
    elif isinstance(value, bool):
        value = str(value)
    elif isinstance(value, int):
        value = "{0}i".format(value)
    elif isinstance(value, float):
        value = str(value)
    return value

