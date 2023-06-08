import logging
import yaml
import os


def normalize_dict(test_dict):
    res = dict()
    for key in test_dict.keys():
        if isinstance(test_dict[key], dict):
            res[key.lower()] = normalize_dict(test_dict[key])
        else:
            res[key.lower()] = test_dict[key]
    return res


class Config:

    def __init__(self,
                 config_file='/etc/ceph/node-proxy.yaml',
                 default_config={}):
        self.config_file = config_file
        self.default_config = default_config

        self.load_config()

    def load_config(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self.default_config

        for k, v in self.default_config.items():
            if k not in self.config.keys():
                self.config[k] = v

        for k, v in self.config.items():
            setattr(self, k, v)

        # TODO: need to be improved
        for _l in Logger._Logger:
            _l.logger.setLevel(self.logging['level'])
            _l.logger.handlers[0].setLevel(self.logging['level'])

    def reload(self, config_file=None):
        if config_file != '':
            self.config_file = config_file
        self.load_config()


class Logger:
    _Logger = []

    def __init__(self, name, level=logging.INFO):
        self.name = name
        self.level = level

        Logger._Logger.append(self)
        self.logger = self.get_logger()

    def get_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        handler = logging.StreamHandler()
        handler.setLevel(self.level)
        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)

        return logger
