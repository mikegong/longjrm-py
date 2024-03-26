import logging
import sys


class Logger(object):

    def __init__(self, logname, loglevel, module):

        self.logname = logname
        self.loglevel = loglevel
        self.logmodule = module

        self.logger = None

    def getlog(self):
        if not self.logger:
            self.logger = self.set_logger()

        return self.logger

    def set_logger(self):

        format_dict = {
               logging.DEBUG: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
               logging.INFO: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
               logging.WARNING: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
               logging.ERROR: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
               logging.CRITICAL: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            }

        # set logger
        self.logger = logging.getLogger(self.logmodule)
        self.logger.setLevel(self.loglevel)

        if not len(self.logger.handlers):
            # create handler to write log file
            fh = logging.FileHandler(filename=self.logname, encoding='utf-8')
            fh.setLevel(self.loglevel)

            # create another handler to output console
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(self.loglevel)

            # define handler format
            # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            formatter = format_dict[self.loglevel]
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            # add handler to logger
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

        return self.logger
