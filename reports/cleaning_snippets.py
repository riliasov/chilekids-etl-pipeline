def clean_currency(val):
    if not val: return None
    return str(val).upper().strip()

