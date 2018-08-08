import multiprocessing as mp
import pandas as pd
import numpy as np

class PreprocessClustering:
	""" Preprocessing script for clustering algorithm 

		Note:
			This Python object aggregates at a user-specified 
			key level. 



			Configuration 
	""" 

	def __init__(self, data, config):
		"""
		:data (pandas dataframe):  
		:config (dict): contains configuration file  

		Note: 
			config should be in the following format

			{
				'variables': {
					'continuous': [],
					'categorical': [],
					'datetime': [],
				},
				'continuous_config': {
					'min': True,
					'max': True,
					'var': True,
					'median': True,
					'qauntiles': [],
					'f'
				}
			}
		"""
		self.data = data
		self.config = config

	def __min__(self, x):
		return

	def __max__(self, x):
		return 

	def __mean__(self, x):
		return 

	def ___median__(self, x):
		return 

	def __quantile__(self, x):
		return

	def counts(self, x):
		return 

	def datetime_to_epoch(self, x):
		return

	def continuous_preprocess(self, vars):
		"""
		:param : 
		""" 
		return  

	def categorical_preprocess(self, vars): 
		"""
		:param : 
		"""
		return 

	def nlp_preprocess(self, vars):
		"""

		""" 
		return 

	def geo_location_preprocess(self, vars): 
		"""
		"""
		return 

	def preprocess(self): 
		"""
		:param : 
		"""
		return 


df1 = pd.DataFrame(np.random.randint(0,10,size=(100, 4)), columns=list('ABCD')).reset_index()
df2 = pd.DataFrame(np.random.randint(0,10,size=(100, 4)), columns=list('ABCD')).reset_index()
df = pd.concat([df1, df2], axis=0).reset_index(drop=True)
print(df)