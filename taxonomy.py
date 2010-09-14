import logging
import csv

log = logging

import sqlalchemy
from sqlalchemy import MetaData, create_engine, and_
from sqlalchemy.sql import select

class Taxonomy(object):

    def __init__(self, engine, ranks, undefined_rank='no_rank', undef_prefix='below'):
        """
        Taxonomy class.

        * engine - sqlalchemy engine instance providing a connecgtion to a
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
        # vals: lineage represented as a dict of {rank:tax_id}
        self.taxa = {}

        self.undefined_rank = undefined_rank
        self.undef_prefix = undef_prefix

    def _add_rank(self, rank, parent_rank):
        if rank not in self.rankset:
            self.ranks.insert(self.ranks.index(parent_rank) + 1, rank)

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
                    
    def _get_lineage(self, tax_id):
        undefined = self.undefined_rank
        lineage = self.taxa.get(tax_id)

        if not lineage:
            log.debug('reconstructing lineage of tax_id "%s"' % tax_id)
            parent, rank = self._node(tax_id)

            # rename undefined ranks
            if rank == undefined:
                parent_id, parent_rank = self._node(parent)
                rank = self.undef_prefix+'_'+parent_rank
                self._add_rank(rank, parent_rank)

            lineage = {rank:tax_id}
            if parent != tax_id:
                # only if we haven't reached the root
                parent_lineage = self._get_lineage(parent)
                lineage.update(parent_lineage)
                
            lineage['rank'] = rank 
            self.taxa[tax_id] = lineage

        return lineage

    def lineage(self, tax_id):
        """
        Public method for returning a lineage; includes tax_name and rank

        TODO: should handle merged tax_ids
        """

        lineage = self._get_lineage(tax_id).copy()
        lineage['tax_name'] = self.primary_name(tax_id)

        return lineage
        
    def has_lineage(self, tax_id):
        return tax_id in self.taxa

    def write_table(self, csvfile=None):
        
        fields = ['tax_id','rank','tax_name'] + self.ranks        
        writer = csv.DictWriter(csvfile, fieldnames=fields,
                                extrasaction='ignore', quoting=csv.QUOTE_NONNUMERIC)

        # header row
        writer.writerow(dict(zip(fields, fields)))
        
        for tax_id in self.taxa.keys():
            lin = self.lineage(tax_id)
            lin['tax_id'] = tax_id
            
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
        
    def add_node(self, tax_id, parent_id, rank, tax_name, source_id=None, source_name=None):

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
        
