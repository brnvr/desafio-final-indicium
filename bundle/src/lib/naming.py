import re


def pascal_to_snake(string: str):
    """
    Conforms the input string, in pascal or camel case, to snake case.

    Args:
        string (str): The  pascal or camel case input string.

    Returns:
        str: The snake case string.
    """
    string = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', string)
    string = re.sub(r'([a-z])([A-Z]{2,})', r'\1_\2', string)
    return string.lower()
