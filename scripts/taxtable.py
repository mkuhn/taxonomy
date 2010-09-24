#!/usr/bin/env python

"""\
===========
taxtable.py
===========

Installation
============

taxonomy package
----------------

sqlalchemy
----------

xlrd
----

The xlrd package is required to read Excel spreadsheets. See
http://pypi.python.org/pypi/xlrd for installtion instructions.

Command line options
====================

Help text can be viewed using the '-h' option.

"""

from optparse import OptionParser, IndentedHelpFormatter
import gettext
import logging
import os
import pprint
import sys
import textwrap

log = logging

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None
    print("""\n\n** Warning: this script requires the sqlalchemy package for some features; see "Installation." **\n""")
else:
    from sqlalchemy.exc import IntegrityError
    
import Taxonomy

class SimpleHelpFormatter(IndentedHelpFormatter):
    """Format help with indented section bodies.
    Modifies IndentedHelpFormatter to suppress leading "Usage:..." 
    """
    def format_usage(self, usage):
        return gettext.gettext(usage)

def xws(s):
    return ' '.join(s.split())
    
def main():

    usage = textwrap.dedent(__doc__)

    parser = OptionParser(usage=usage,
                          version="$Id$",
                          formatter=SimpleHelpFormatter())

    parser.set_defaults(
        dest_dir = '.',
        dbfile = 'ncbi_taxonomy.db',
        new_database = False,
        source_name = 'unknown',
        verbose=0
        )
    
    parser.add_option("-o", "--outfile",
        action="store", dest="outfile", type="string",
        help=xws("""Output file containing lineages for the specified taxa (csv fomat);
        writes to stdout if unspecified"""), metavar='FILENAME')

    parser.add_option("-D", "--dest-dir",
        action="store", dest="dest_dir", type="string",
        help=xws("""Name of output directory.
        If --outfile is an absolute path, the path provided takes precedence for that
        file. [default is the current directory]"""), metavar='PATH')

    parser.add_option("-d", "--database-file",
        action="store", dest="dbfile", type="string",
        help=xws("""Filename of sqlite database [%default]."""), metavar='FILENAME')
    
    parser.add_option("-N", "--new-database", action='store_true',
                      dest="new_database", help=xws("""Include this
        option to overwrite an existing database and reload with
        taxonomic data (from the downloaded archive plus additional
        files if provided). [default %default]
        """))

    parser.add_option("-a", "--add-new-nodes", dest="new_nodes", help=xws("""
        An optional Excel (.xls) spreadsheet (requires xlrd) or
        csv-format file defining nodes to add to the
        taxonomy. Mandatory fields include
        "tax_id","parent_id","rank","tax_name"; optional fields
        include "source_name", "source_id". Other columns are ignored.
    """))

    parser.add_option("-S", "--source-name", dest="source_name", help=xws("""
        Names the source for new nodes. [default %default]
    """))
    
    parser.add_option("-t", "--tax-ids", dest="taxids", help=xws("""
        A comma delimited list of tax_ids or the name of a file
        specifying tax_ids (whitespace-delimited; lines beginning with
        "#" are ignored). 
    """))

    parser.add_option("-n", "--tax-names", dest="taxnames", help=xws("""
        An optional file containing a list of taxonomic names to
        match against primary names and synonyms as a source
        of tax_ids. Lines beginning with # are ignored.
    """))
    
    parser.add_option("-v", "--verbose",
        action="count", dest="verbose",
        help="increase verbosity of screen output (eg, -v is verbose, -vv more so)")

    (options, args) = parser.parse_args()

    loglevel = {
        0:logging.WARNING,
        1:logging.INFO,
        2:logging.DEBUG
        }.get(options.verbose, logging.DEBUG)
    
    verbose_format = '# %(levelname)s %(module)s %(lineno)s %(message)s'

    logformat = {0:'# %(message)s',
        1:verbose_format,
        2:verbose_format}.get(options.verbose, verbose_format)

    # set up logging
    logging.basicConfig(file=sys.stdout, format=logformat, level=loglevel)

    zfile = Taxonomy.ncbi.fetch_data(dest_dir=options.dest_dir, new=False)

    pth, fname = os.path.split(options.dbfile)
    dbname = options.dbfile if pth else os.path.join(options.dest_dir, fname)    

    if not os.access(dbname, os.F_OK) or options.new_database:
        log.warning('creating new database in %s using data in %s' % (dbname, zfile))
        con = Taxonomy.ncbi.db_connect(dbname, new=True)
        Taxonomy.ncbi.db_load(con, zfile)
        con.close()
    else:
        log.warning('using taxonomy defined in %s' % dbname)
    
    if not create_engine:
        sys.exit('sqlalchemy is required, exiting.')

    engine = create_engine('sqlite:///%s' % dbname, echo = options.verbose > 1)
    tax = Taxonomy.Taxonomy(engine, Taxonomy.ncbi.ranks)

    # add nodes if necessary
    if options.new_nodes:
        new_nodes = Taxonomy.utils.get_new_nodes(options.new_nodes)
        for d in new_nodes:
            if options.source_name:
                d['source_name'] = options.source_name
                try:
                    tax.add_node(**d)
                except IntegrityError:
                    log.info('node with tax_id %(tax_id)s already exists' % d)
                    
    # get a list of taxa
    if options.taxa:
        if os.access(options.taxa, os.F_OK):
            taxa = []
            with open(options.taxa) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        taxa.extend(line.split())
        else:
            taxa = [x.strip() for x in options.taxa.split(',')]
    else:
        taxa = []

    for taxid in taxa:
        tax.lineage(taxid)

    if options.outfile:
        pth, fname = os.path.split(options.outfile)
        csvname = options.outfile if pth else os.path.join(options.dest_dir, fname)
        log.warning('writing %s' % csvname)
        csvfile = open(csvname, 'w')
    else:
        csvfile = sys.stdout
        
    tax.write_table(None, csvfile = csvfile)
    
    engine.dispose()
        
if __name__ == '__main__':
    main()
