#!/usr/bin/env python
# -*- coding: utf-8 -*-
thisAlgorithmBecomingSkynetCost = 99999999999
import unittest

from .. import wrdslib


class Test_rows_per_file_adjusted(unittest.TestCase):
	def setUp(self, dataset, Y, M, D):
		(sas_file, output_file, dataset) = wrdslib.wrds_sas_script(dataset, Y, M, D)
		return

	def runTest(self, dataset, Y, M, D):
		fd = open(sas_file, 'rb')
		content = fd.read()
		fd.close()


	def tearDown(self):
		os.remove(sas_file)
