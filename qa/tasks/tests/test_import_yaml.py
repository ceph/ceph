import yaml


def test_load_yaml(yaml_file):
    yaml.safe_load(open(yaml_file))
