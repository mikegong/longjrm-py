def load_ini(file):
    import configparser

    config = configparser.ConfigParser()

    with open(file) as f:
        config.read_file(f)
    return config


def load_json(file):
    import json
    with open(file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config


def yaml_loader(file):
    import yaml
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
    return config
