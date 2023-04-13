import configparser
from optparse import OptionParser

class ConfigProcessor:

    def __init__(self, config_name):
        config_parser = configparser.RawConfigParser()
        config_parser.optionxform = lambda option: option
        config_parser.read(config_name)
        parser = OptionParser()
        parser.add_option("-f", "--file", dest="filename", type="string", help="Input filename - Mandatory")
        parser.add_option("-d", "--database", dest="hostname", type="string", help="Elasticsearch server ip/host name with port in the format <IP/HOST:PORT> default: localhost:9200. Not a mandatory option")
        parser.add_option("--log", dest="log_toggle", type="string", help="output log on and off")
        (options, args) = parser.parse_args()
        if not options.filename:
            raise Exception("Fiename has to be entered as an argument (-f)")
        if options.log_toggle:
            if options.log_toggle != 'True' and options.log_toggle != 'False':
                raise Exception("The log argument can only be True or False")
        config_parser['DETAILS']['filename'] = options.filename
        if options.hostname:
            config_parser['DETAILS']['db_hostname'] = options.hostname
        if options.log_toggle:
            config_parser['DETAILS']['log'] = options.log_toggle
        self.config = config_parser

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_config(self, config_param):
        if config_param not in self.config:
            raise Exception(config_param + " configuration param is missing or not defined")
        else:
            return self.config[config_param]

    def get_section_values(self, config_param):
        if config_param not in self.config:
            raise Exception(config_param + " configuration param is missing or not defined")
        else:
            return dict(self.config(config_param))

    def get_option(self, section, key):
        if self.config.has_option(section, key):
            return self.config.get(section, key)
        else:
            raise Exception("Section"+section+" or "+key+" not configured")

