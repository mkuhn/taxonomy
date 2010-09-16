import logging
import csv
import itertools

log = logging

import sqlalchemy
from sqlalchemy import MetaData, create_engine, and_
from sqlalchemy.sql import select

class Taxonomy(object):

    def __init__(self, engine, ranks, undefined_rank='no_rank', undef_prefix='below'):
        """
        The Taxonomy class defines an object providing an interface to
        the taxonomy database.

        * engine - sqlalchemy engine instance providing a connection to a
          database defining the taxonomy
        * ranks - list of rank names, root first
        * undefined_rank - label identifying a taxon without
          a specific rank in the taxonomy.
        * undef_prefix - string prepended to name of parent
          rank to create new labels for undefined ranks.

        Example:
        > engine = create_engine('sqlite:///%s' % dbname, echo=False)
        > tax = Taxonomy(engine, Taxonomy.ncbi.ranks)

          """

        # TODO: should ranks be defined in a table in the database?
        # TODO: assertions to check for database components

        # see http://www.sqlalchemy.org/docs/reference/sqlalchemy/inspector.html
        # http://www.sqlalchemy.org/docs/metadata.html#metadata-reflection

        log.debug('using database %s' % engine.url)

        self.engine = engine
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.meta.reflect()

        self.nodes = self.meta.tables['nodes']
        self.names = self.meta.tables['names']
        self.source = self.meta.tables['source']

        self.ranks = ranks
        self.rankset = set(self.ranks)

        # keys: tax_id
        # vals: lineage represented as a list of tuples: (rank, tax_id)
        self.cached = {}

        # keys: tax_id
        # vals: lineage represented as a dict of {rank:tax_id}
        self.taxa = {}
        
        self.undefined_rank = undefined_rank
        self.undef_prefix = undef_prefix

    def _add_rank(self, rank, parent_rank):
        """
        inserts rank into self.ranks.
        """
        
        if rank not in self.rankset:
            self.ranks.insert(self.ranks.index(parent_rank) + 1, rank)
        self.rankset = set(self.ranks)
            
    def _node(self, tax_id):
        """
        Returns parent, rank
        """

        s = select([self.nodes.c.parent_id, self.nodes.c.rank],
                   self.nodes.c.tax_id == tax_id)
        res = s.execute()
        output = res.fetchone()

        if not output:
            raise KeyError('value "%s" not found in nodes.tax_id' % tax_id)

        # parent_id, rank
        return output

    def primary_name(self, tax_id):
        """
        Returns primary taxonomic name associated with tax_id
        """

        s = select([self.names.c.tax_name],
                   and_(self.names.c.tax_id == tax_id, self.names.c.is_primary == 1))
        res = s.execute()
        output = res.fetchone()

        if not output:
            raise KeyError('value "%s" not found in names.tax_id' % tax_id)
        else:
            return output[0]

    def _rename_undefined(self, lineage):

        undefined = self.undefined_rank        

        d = {}
        parent_rank, parent_id = None, None
        for rank, tax_id in lineage:
            if rank == undefined:
                rank = self.undef_prefix+'_'+parent_rank
                self._add_rank(rank, parent_rank)
            d[rank] = tax_id
            parent_rank, parent_id = rank, tax_id

        return d

            
    def _get_lineage(self, tax_id):
        """
        Returns cached lineage from self.cached or recursively builds
        lineage of tax_id until the root node is reached.
        """
        
        lineage = self.cached.get(tax_id)

        if not lineage:
            log.debug('reconstructing lineage of tax_id "%s"' % tax_id)
            parent_id, rank = self._node(tax_id)
            lineage = [(rank, tax_id)]
            
            # recursively add parent_ids until we reach the root
            if parent_id != tax_id:
                lineage = self._get_lineage(parent_id) + lineage

            self.cached[tax_id] = lineage

        return lineage

    def lineage(self, tax_id, cache_parents=True):
        """
        Public method for returning a lineage; includes tax_name and rank

        TODO: should handle merged tax_ids
        """

        # check the cache of lineages before calculating
        ldict = self.taxa.get(tax_id)
        if not ldict:        
            lineage = self._get_lineage(tax_id)
            ldict = self._rename_undefined(lineage)

            ldict['tax_id'] = tax_id
            ldict['parent_id'], ldict['rank'] = self._node(tax_id)
            ldict['tax_name'] = self.primary_name(tax_id)
            self.taxa[tax_id] = ldict

            # cache parents
            if cache_parents:
                [self.lineage(tid, cache_parents=False) for rank, tid in self.cached[tax_id]]
                                
        return ldict

    def write_table(self, taxa=None, csvfile=None, full=False):
        """
        Represent the currently defined taxonomic lineages as a rectangular
        array with columns named "tax_id","rank","tax_name", followed
        by a column for each rank proceeding from the root to the more
        specific ranks.

         * taxa - list of taxids to include in the output; if none are
           provided, use self.cached.keys() (ie, those taxa loaded into the cache).
         * csvfile - an open file-like object (see "csvfile" argument to csv.writer)
         * full - if True (the default), includes a column for each rank in self.ranks;
           otherwise, omits ranks (columns) the are undefined for all taxa.
        """

        if not taxa:
            taxa = self.cached.keys()

        # which ranks are actually represented?
        if full:
            ranks = self.ranks
        else:
            represented = set(itertools.chain.from_iterable(
                    lineage.keys() for lineage in self.taxa.values() 
                    ))
            
            # represented = set(itertools.chain.from_iterable(
            #         [[node[0] for node in lineage] for lineage in self.cached.values()])
            #)
            ranks = [r for r in self.ranks if r in represented]
            
        fields = ['tax_id','parent_id','rank','tax_name'] + ranks
        writer = csv.DictWriter(csvfile, fieldnames=fields,
                                extrasaction='ignore', quoting=csv.QUOTE_NONNUMERIC)
        
        # header row
        writer.writerow(dict(zip(fields, fields)))
              
        for tax_id in taxa:
            lin = self.lineage(tax_id)
            # lin['tax_id'] = tax_id

            writer.writerow(lin)

    def add_source(self, name, description=None):
        """
        Attempts to add a row to table "source". Returns (source_id,
        True) if the insert succeeded, (source_id, False) otherwise.
        """

        try:
            result = self.source.insert().execute(name = name, description = description)
            source_id, success = result.inserted_primary_key[0], True
        except sqlalchemy.exc.IntegrityError:
            s = select([self.source.c.id], self.source.c.name == name)
            source_id, success = s.execute().fetchone()[0], False

        return source_id, success

    def add_node(self, tax_id, parent_id, rank, tax_name, source_id=None, source_name=None, **kwargs):

        if not (source_id or source_name):
            raise ValueError('Taxonomy.add_node requires source_id or source_name')

        if not source_id:
            source_id, source_is_new = self.add_source(name=source_name)

        result = self.nodes.insert().execute(tax_id = tax_id,
                                             parent_id = parent_id,
                                             rank = rank,
                                             source_id = source_id)

        result = self.names.insert().execute(tax_id = tax_id,
                                             tax_name = tax_name,
                                             is_primary = 1)

        lineage = self.lineage(tax_id)

        log.debug(lineage)
        return lineage

