def format_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        return f"'{value}'"