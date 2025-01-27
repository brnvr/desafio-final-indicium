import re


def pascal_to_snake(name):
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = re.sub(r'([a-z])([A-Z]{2,})', r'\1_\2', name)
    return name.lower()
