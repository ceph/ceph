from typing import Union

ValueType = Union[str, bool, int, float]


def format_string(key: ValueType) -> str:
    if isinstance(key, str):
        return key.replace(',', r'\,') \
                  .replace(' ', r'\ ') \
                  .replace('=', r'\=')
    else:
        return str(key)


def format_value(value: ValueType) -> str:
    if isinstance(value, str):
        value = value.replace('"', '\"')
        return f'"{value}"'
    elif isinstance(value, bool):
        return str(value)
    elif isinstance(value, int):
        return f"{value}i"
    elif isinstance(value, float):
        return str(value)
    else:
        raise ValueError()
