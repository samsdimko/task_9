import json


def load_config(filename='config.json'):
    with open(filename, 'r') as f:
        try:
            return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Error loading config file '{filename}': {e}")