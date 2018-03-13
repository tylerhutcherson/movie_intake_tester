# -*- coding: utf-8 -*-
import logging
import os
import sys

def get_logger(name='app'):
  ch = logging.StreamHandler(sys.stdout)
  ch.setLevel(os.environ.get('LOG_LEVEL', 'DEBUG'))
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  logger = logging.getLogger(name)
  logger.setLevel(os.environ.get('LOG_LEVEL', 'DEBUG'))
  logger.addHandler(ch)
  return logger
