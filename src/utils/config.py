import json

class Config(object):
    def __init__(self):
        with open('config.json') as config_file:
            self.config = json.load(config_file)

    def get_config_itme(self, cm='conf_module', ci='conf_item'):
        return self.config[cm][ci]
