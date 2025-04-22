# utils/text_utils.py
import re

def remove_extra_spaces(text):
    """Remove extra spaces from text."""
    if text:
        return re.sub(r'\s{2,}', ' ', text.strip())
    return None
