import pandas as pd
import dask.dataframe as dd 
import numpy as np
import json

class ContinuousImputation:
	"""

	"""

	def __init__(self, data, config):
		""" """
		self.setting = config['setting']['continuous']
		self.variables = config['continuous']
		self.data = data

	@staticmethod
	def mean_impute(data):
		""""""
		mean = data.mean()
		return data.fillna(mean)

	@staticmethod
	def median_impute(data):
		""""""
		return
	
	@staticmethod
	def fixed_value_imputation(data):
		""""""
		return
	
	@staticmethod
	def deletion(data):
		""""""
		return

	def get_null_rate(self):
		""""""
		return data.isnull().mean().compute()

	def impute(self, null_rate):
		""""""
		impute_funcs = {'center_impute': self.mean_impute,
						'median_impute': self.median_impute,
						'fixed_value_imputate': self.fixed_value_imputation,
						'deletion': self.deletion}
		
		for k, v in null_rate.items():
			for d in self.setting:
				l, u = d['range'][0], d['range'][1]
				if l < v <= u:
					impute_method = d['method'][0]
					impute_func = impute_funcs[impute_method]
					self.data[k] = impute_func(self.data[k])
		return self.data.compute()

if __name__ == '__main__':

	with open('config.json', 'r') as f:
		config = json.load(f)

	data = pd.DataFrame({'c1': [5, 4, 3, 5, np.nan], 
						 'c2': [1, 2, 3, 4, 5]})

	data = dd.from_pandas(data, npartitions=3)

	# print(data.isnull().mean().compute())
	ci = ContinuousImputation(data=data, config=config)
	missing_rate = ci.get_null_rate()
	print(ci.impute(missing_rate))


