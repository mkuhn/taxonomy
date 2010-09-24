#!/usr/bin/env python

import sys
import os
import unittest
import logging
import itertools
import sqlite3
import shutil
import time
import pprint

from sqlalchemy import create_engine

import config
import Taxonomy

log = logging

module_name = os.path.split(sys.argv[0])[1].rstrip('.py')
outputdir = os.path.abspath(config.outputdir)
datadir = os.path.abspath(config.datadir)

dbname = os.path.join(outputdir, 'ncbi_taxonomy.db')
echo = False

zfile = Taxonomy.ncbi.fetch_data(dest_dir=outputdir, new=False)
if False:
    con = Taxonomy.ncbi.db_connect(dbname, new=True)
    Taxonomy.ncbi.db_load(con, zfile)
    con.close()

class TestTaxonomyInit(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        self.tax._node('2')

    def test02(self):
        self.assertRaises(KeyError, self.tax._node, 'buh')

class TestGetLineagePrivate(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        lineage = self.tax._get_lineage('1')
        self.assertTrue(lineage == [('root','1')])

    def test02(self):
        tax_id = '1280' # staph aureus

        self.assertFalse(tax_id in self.tax.cached)
        lineage = self.tax._get_lineage(tax_id)
        self.assertTrue(tax_id in self.tax.cached)
        self.assertTrue(lineage[0][0] == 'root')
        self.assertTrue(lineage[-1][0] == 'species')


class TestTaxNameSearch(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo) # echo=echo
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        tax_id, tax_name, is_primary = self.tax.primary_from_name('Gemella')
        self.assertTrue(tax_id == '1378')
        self.assertTrue(is_primary)

    def test02(self):
        self.assertRaises(KeyError, self.tax.primary_from_name, 'buggabugga')

    def test03(self):
        tax_id, tax_name, is_primary = self.tax.primary_from_name('Gemella Berger 1960')

        self.assertTrue(tax_id == '1378')
        self.assertFalse(is_primary)
        

class TestSynonyms(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo) # echo=echo
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        synonyms = self.tax.synonyms(tax_id='1378')

    def test02(self):
        synonyms = self.tax.synonyms(tax_name='Gemella')

        
        
class TestGetLineagePublic(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        lineage = self.tax.lineage('1')
        self.assertTrue(lineage['root'] == '1')
        self.assertTrue(lineage['rank'] == 'root')

    def test02(self):
        tax_id = '1280' # staph aureus

        self.assertFalse(tax_id in self.tax.cached)
        lineage = self.tax.lineage(tax_id)
        self.assertTrue(tax_id in self.tax.cached)
        self.assertTrue(lineage['rank'] == 'species')

        keys = set(lineage.keys())
        ranks = set(self.tax.ranks)
        self.assertTrue(keys - ranks == set(['parent_id', 'tax_id', 'rank', 'tax_name']))
        
    def test03(self):
        tax_id = '1378' # Gemella; lineage has two successive no_rank taxa 
        lineage = self.tax.lineage(tax_id)
        self.assertTrue(lineage['rank'] == 'genus')

        keys = set(lineage.keys())
        ranks = set(self.tax.ranks)
        self.assertTrue(keys - ranks == set(['parent_id', 'tax_id', 'rank', 'tax_name']))

    def test04(self):
        self.assertRaises(ValueError, self.tax.lineage, tax_id=None, tax_name=None)

    def test05(self):
        self.assertRaises(ValueError, self.tax.lineage, tax_id='1', tax_name='root')

    def test06(self):
        tax_id = '1378' # Gemella; lineage has two successive no_rank taxa 
        tax_name = 'Gemella'
        lineage = self.tax.lineage(tax_name=tax_name)

        # lineage = self.tax.lineage(tax_id)
        # self.assertTrue(lineage['rank'] == 'genus')

        

        
class TestTaxTable(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)
        self.fname = os.path.join(outputdir, self.funcname)+'.csv'
        log.info('writing to ' + self.fname)
        
    def tearDown(self):
        self.engine.dispose()

    def test02(self):
        tax_id = '1280' # staph aureus
        lineage = self.tax.lineage(tax_id)

        with open(self.fname,'w') as fout:
            self.tax.write_table(taxa=None, csvfile=fout)

    def test03(self):
        tax_id = '1378' # Gemella; lineage has two successive no_rank taxa 
        lineage = self.tax.lineage(tax_id)

        with open(self.fname,'w') as fout:
            self.tax.write_table(taxa=None, csvfile=fout)

            
            
class TestMethods(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)

    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        taxname = self.tax.primary_from_id('1280')
        self.assertTrue(taxname == 'Staphylococcus aureus')

    def test02(self):
        self.assertRaises(KeyError, self.tax.primary_from_id, 'buh')

    def test03(self):
        res = self.tax.add_source(name='new source', description='really new!')
        res = self.tax.add_source(name='new source', description='really new!')
        self.assertTrue(res == (2, False))


    # def test04(self):
    #     self.tax.add_node(tax_id = "186802_1",
    #                       parent_id = "186802",
    #                       rank = "species",
    #                       source_name = "Fredricks Lab",
    #                       tax_name = 'BVAB1')

