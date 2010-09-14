import os
import logging

debuglevel = logging.WARNING

outputdir = '../test_output'
datadir = '../testfiles'

try:
    os.mkdir(outputdir)
except OSError:
    pass

