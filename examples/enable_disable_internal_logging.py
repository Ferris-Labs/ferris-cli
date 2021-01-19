"""
ferris-cli is a library meant to be used in other applications.
But as software itself it uses logging like many other third-party
libraries.

One can disable/enable third-party libraries logging.

In this snippet, we provide an example of how you can silence or enable 
ferris-cli logging.
"""


import logging


logging.basicConfig(level=logging.CRITICAL)  # to silence most third-party libraries logging

## This will activate loggers for ferris_cli library

ferris_cli_logger = logging.getLogger('ferris_cli')
ferris_cli_logger.setLevel(logging.DEBUG)


## This will disable debug loggers for ferris_cli library
# In this case only if an error happens you will get logs from ferris_cli

ferris_cli_logger = logging.getLogger('ferris_cli')
ferris_cli_logger.setLevel(logging.INFO)


## To disable any logs from ferris_cli library you could set on CRITICAL, which we do not raise as of version 0.7.0
# or just use the logging.basicConfig(level=logging.CRITICAL) or equivalent in celery, etc. apps.
ferris_cli_logger = logging.getLogger('ferris_cli')
ferris_cli_logger.setLevel(logging.CRITICAL)
