import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from scipy.stats import wilcoxon, kstest, chi2_contingency, ranksums
from collections import OrderedDict

class ContinuousEDA:
	""" 
	This function generates useful statistics on a continuous variable

	:param df: dataframe that contain the explantatory and target variables
	:param v:: explanatory variable
	:param t: target variable
	"""

	def __init__(self, df, vars=[], target=''):
		self.df = df
		self.vars = vars
		self.target = target

	def remove_nulls(self, x, y):
		x_notmissing = x[~x.isnull()]
		y_notmissing = y[~x.isnull()]
		return x_notmissing, y_notmissing

	def mean(self, x):
		return x.mean()

	def median(self, x):
		return x.median()

	def std(self, x):
		return x.std()

	def var(self, x):
		return x.var()

	def min(self, x):
		return x.min()

	def max(self, x):
		return x.max()

	def quantile(self, x, q):
		return x.quantile(q)

	def normalized_mean(self, x):
		x = x.reshape(-1, 1)
		return MinMaxScaler().fit(x).transform(x).mean()

	def normalized_median(self, x):
		x = x.reshape(-1, 1)
		return  np.median(MinMaxScaler().fit(x).transform(x))

	def normalized_std(self, x):
		x = x.reshape(-1, 1)
		return  MinMaxScaler().fit(x).transform(x).std()

	def normalized_var(self, x):
		x = x.reshape(-1, 1)
		return  MinMaxScaler().fit(x).transform(x).var()

	def null_rate(self, x):
		return x.isnull().mean()

	def null_flag_rate(self, x, y):
		isnull = x.isnull()
		y_flag_missing = y[isnull].mean()
		y_flag_not_missing = y[~isnull].mean()
		return y_flag_missing, y_flag_not_missing

	def null_signifiance(self, x, y):
		x = x.copy()
		x_nullpos = x.isnull()
		x[x_nullpos] = 1
		x[~x_nullpos] = 0
		ct = pd.crosstab(x, y)
		return chi2_contingency(ct)[:2]

	def kstest(self, x, distribution='norm'):
		"""
		:param x: explanatory variable to test
		:param distribution: choose distribution to test the observed value against
		:returns: statistic and pvalue
		"""
		return kstest(x, distribution)

	def ranksums(self, x, y):
		""" 
		:param x: explanatory variable to test
		:param y: target variable 
		:returns: statistic and pvalue
		"""
		return ranksums(x, y)

	def get_statistics(self, x, y, name):

		x_flag, y_flag = x[y == 1], y[y == 1]
		x_noflag, y_noflag = x[y == 0], y[y == 0]

		x_notmissing, y_notmissing = self.remove_nulls(x, y)
		x_flag_notmissing, y_flag_notmissing = self.remove_nulls(x_flag, y_flag)
		x_noflag_notmissing, y_noflag_notmissing = self.remove_nulls(x_noflag, y_noflag)

		eda = OrderedDict()
		eda['variable'] = name
		for label, col in {'': x_notmissing, '_flag': x_flag_notmissing, '_noflag': x_noflag_notmissing}.items():
			eda['mean' + label] = self.mean(col)
			eda['median' + label] = self.median(col)
			eda['std' + label] = self.std(col)
			eda['var' + label] = self.var(col)
			eda['min' + label] = self.min(col)
			eda['max' + label] = self.max(col)
			eda['quantile_25' + label] = self.quantile(col, 0.20)
			eda['quantile_40' + label] = self.quantile(col, 0.40)
			eda['quantile_60' + label] = self.quantile(col, 0.60)
			eda['quantile_80' + label] = self.quantile(col, 0.80)
			eda['quantile_90' + label] = self.quantile(col, 0.90)
			eda['quantile_95' + label] = self.quantile(col, 0.95)
			eda['quantile_99' + label] = self.quantile(col, 0.99)
			eda['normalized_mean' + label] =  self.normalized_mean(col)
			eda['normalized_median' + label] = self.normalized_median(col)
			eda['normalized_std' + label] = self.normalized_std(col)
			eda['normalized_var' + label] = self.normalized_var(col)

		eda['null_rate'] = self.null_rate(x)
		eda['null_rate_flag_group'] = self.null_rate(x_flag)
		eda['null_rate_nonflag_group'] = self.null_rate(x_noflag)		
		eda['null_flag_rate_missing'], eda['null_flag_rate_notmissing'] = self.null_flag_rate(x, y)
		eda['null_signifiance_chisquare'], eda['null_signifiance_pvalue'] = self.null_signifiance(x, y)
		eda['kstest_stat'], eda['kstest_pvalue'] = self.kstest(x_notmissing)
		eda['wilcoxon_stat'], eda['wilcoxon_pvalue'] = self.ranksums(x_flag_notmissing, x_noflag_notmissing)
		return eda

	def create_eda_df(self):

		stat_list = []
		for var in self.vars:
			print("Generating statistics on " + var)
			x = self.df[var]
			y = self.df[self.target]
			stat_list.append(self.get_statistics(x, y, var))
		return pd.DataFrame(stat_list)

