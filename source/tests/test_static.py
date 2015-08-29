#!/usr/bin/env python
# -*- coding: utf-8 -*-
thisAlgorithmBecomingSkynetCost = 99999999999
import unittest

from .. import static





class Test_rows_per_file_adjusted(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		pass




class Test_get_ymd_range(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		pass





class Test_get_loop_frequency(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		self.assertEqual(static.get_loop_frequency('optionm.opprcd', 2007), 'M')
		self.assertEqual(static.get_loop_frequency('optionm.opprcd', 2008), 'D')

		self.assertEqual(static.get_loop_frequency('taq.cq', 2007), 'D')
		self.assertEqual(static.get_loop_frequency('taq.ct', 1993), 'D')

		self.assertEqual(static.get_loop_frequency('ibes.det_xepsus', 2005), 'Y')
		self.assertEqual(static.get_loop_frequency('ibes.det_xepsus', 2006), 'M')

		self.assertEqual(static.get_loop_frequency('ibes.det_xepsint', 2003), 'Y')
		self.assertEqual(static.get_loop_frequency('ibes.det_xepsint', 2004), 'M')

		self.assertEqual(static.get_loop_frequency('anything else', None), 'Y')







class Test_fix_weekdays(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		pass





class Test_wrds_datevar(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		self.assertEqual(static.wrds_datevar('tfn.s12'), 'fdate')
		self.assertEqual(static.wrds_datevar('tfn.s34'), 'fdate')
		self.assertEqual(static.wrds_datevar('crsp.dsf'), 'date')
		self.assertEqual(static.wrds_datevar('comp.fundq'), 'DATADATE')
		self.assertEqual(static.wrds_datevar('optionm.opprcd'), 'date')
		self.assertEqual(static.wrds_datevar('optionm.foo'), 'effect_date')
		self.assertEqual(static.wrds_datevar('ibes.recddet'), 'anndats')
		self.assertEqual(static.wrds_datevar('anything else'), 'date')





class Test_min_YMD(unittest.TestCase):
	def setUp(self):
		pass

	def runTest(self):
		pass




