
import pandas as pd

def data_report(df, message, key):
    print('[INFO] Row size in {0}: {1}'.format(message, df.shape[0]))
    print('[INFO] Distinct {2} size in {0}: {1}'.format(message, df[key].nunique(), key))