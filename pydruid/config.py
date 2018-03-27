# -*- coding: utf-8 -*-
"""The main config file for pydruid

All configuration in this file can be overridden by providing a druid_config
in your PYTHONPATH as there is a ``from druid_config import *``
at the end of this file.
"""

import imp
import os
import sys

REQUESTS_AUTH = None

CONFIG_PATH_ENV_VAR = 'DRUID_CONFIG_PATH'

try:
    if CONFIG_PATH_ENV_VAR in os.environ:
        # Explicitly import config module that is not in pythonpath; useful
        # for case where app is being executed via pex.
        print('Loaded your LOCAL configuration at [{}]'.format(
            os.environ[CONFIG_PATH_ENV_VAR]))
        module = sys.modules[__name__]
        override_conf = imp.load_source(
            'druid_config',
            os.environ[CONFIG_PATH_ENV_VAR])
        for key in dir(override_conf):
            if key.isupper():
                setattr(module, key, getattr(override_conf, key))
    else:
        from druid_config import *  # noqa
        import druid_config
        print('Loaded your LOCAL configuration at [{}]'.format(
            druid_config.__file__))
except ImportError:
    pass

