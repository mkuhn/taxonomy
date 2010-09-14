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

zfile = os.path.join(outputdir, 'taxdmp.zip')
dbname = os.path.join(outputdir, 'taxonomy_test.db')
echo = False

if True:
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
        self.assertTrue(lineage['root'] == '1')
        
    def test02(self):
        tax_id = '1280' # staph aureus

        self.assertFalse(tax_id in self.tax.taxa)        
        lineage = self.tax._get_lineage(tax_id)
        self.assertTrue(tax_id in self.tax.taxa)        

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
        
        self.assertFalse(tax_id in self.tax.taxa)        
        lineage = self.tax.lineage(tax_id)
        self.assertTrue(tax_id in self.tax.taxa)        
        self.assertTrue(lineage['rank'] == 'species')
        
class TestTaxTable(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)
        
    def tearDown(self):
        self.engine.dispose()

    def test02(self):
        tax_id = '1280' # staph aureus
        lineage = self.tax._get_lineage(tax_id)

        fname = os.path.join(outputdir, self.funcname)+'.csv'
        with open(fname,'w') as fout:
            self.tax.write_table(fout)
        
class TestMethods(unittest.TestCase):

    def setUp(self):
        self.funcname = '_'.join(self.id().split('.')[-2:])
        self.engine = create_engine('sqlite:///%s' % dbname, echo=echo)
        self.tax = Taxonomy.Taxonomy(self.engine, Taxonomy.ncbi.ranks)
        
    def tearDown(self):
        self.engine.dispose()

    def test01(self):
        taxname = self.tax.primary_name('1280')
        self.assertTrue(taxname == 'Staphylococcus aureus')
        
    def test02(self):
        self.assertRaises(KeyError, self.tax.primary_name, 'buh')

    def test03(self):
        res = self.tax.add_source(name='new source', description='really new!')
        print res
        
    def test04(self):
        
        self.tax.add_node(tax_id = "186802_1",
                          parent_id = "186802",
                          rank = "species",
                          source_name = "Fredricks Lab",
                          tax_name = 'BVAB1')
        
