
import pandas as pd

def data_report(df, message):
    print('[INFO] Row size in {0}: {1}'.format(message, df.shape[0]))
    print('[INFO] Distinct IDs size in {0}: {1}'.format(message, df.eid.nunique()))