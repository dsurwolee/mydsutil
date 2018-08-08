import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class BinaryClassResult:
    
    def __init__(self, model):
        self.model = model
        
    def binary_quantile_table(self, data, percentiles=[0.01,1,0.01]):
        """ Creates a quantile analysis table for binary class 
        :model (H2O Model): 
        :data (H2O Frame):
        :percentiles (list): 
        """ 

        from numpy import arange
        from collections import OrderedDict

        stats = OrderedDict({
            'ntiles': [], 
            'thresholds': [],
            'precision': [],
            'recall': [],
            'F1': []
        })
        
        # Retrieve model scores for class 1
        scores = self.model.predict(data)['p1'].as_data_frame()
        # Model performance on test
        model_test = self.model.model_performance(data)

        # Retrieve statistics at each top ntile buckets of scores:
        for i in arange(*percentiles):
            t = scores.quantile(1-i).values[0]  # Return threshold corresponding to the quantile
            stats['ntiles'].append(i)
            stats['thresholds'].append(t)
            stats['precision'].append(model_test.precision([t])[0][1]) # Return precision 
            stats['recall'].append(model_test.recall([t])[0][1]) # Return precision 
            stats['F1'].append(model_test.F1([t])[0][1]) # Return F1 score 

        return pd.DataFrame(stats)   
    
    def plot_curves(self, stats_df):
        
        x = stats_df['thresholds'].tolist()
        
        trace0 = go.Scatter(
                x = t,
                y = stats_df['precision'].tolist(),
                mode = 'lines',
                name = 'precision'
        )
        trace1 = go.Scatter(
            x = t,
            y = stats_df['recall'].tolist(),
            mode = 'lines',
            name = 'recall'
        )
        trace2 = go.Scatter(
            x = t,
            y = stats_df['F1'].tolist(),
            mode = 'lines',
            name = 'f1'
        )
        data = [trace0, trace1, trace2]

        print('AUC', dice_event_3_model.auc(valid=True))
        return py.iplot(data, filename='event_3_model') 
    
def plot_features(model):
    plt.rcdefaults()
    fig, ax = plt.subplots(figsize=(14,20))
    variables = model._model_json['output']['variable_importances']['variable']
    y_pos = np.arange(len(variables))
    scaled_importance = model._model_json['output']['variable_importances']['scaled_importance']
    ax.barh(y_pos, scaled_importance, align='center', color='green', ecolor='black')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(variables)
    ax.invert_yaxis()
    ax.set_xlabel('Scaled Importance')
    ax.set_title('Variable Importance')
    plt.show()
    
# Score Distribution
def get_score_histogram(score_df, score, label):
    f, ax = plt.subplots(figsize=(15,8))
    score_df[score_df[label] == 1].hist(column=score, label='flagged', normed=True, ax=ax, bins=100, alpha=0.5)
    score_df[score_df[label] == 0].hist(column=score, label='Not flagged', normed=True, ax=ax, bins=100, alpha=0.5)
    plt.legend(loc='upper right')
    plt.title('Distribution of scores by flagged versus not-flagged groups')
    plt.xlabel('Scores from 0 to 1.0')
    plt.ylabel('Normalized counts')
    plt.show()
    
def biclass_hist(df, col='', by='', cutoff=None, title=None, xlabel=None, ylabel=None, **kwargs):
    if cutoff:
        cutoff_value = df[col].quantile(cutoff)
        df = df[df[col] < cutoff_value]
    for c, v in df.groupby(by)[col]:
        v.hist(**kwargs, label=str(c))
    if title:
        plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    plt.legend()
    plt.show()


def column_types(cols=[], ignore=None):
    column_type_dict = {}
    for c in cols:
        if c not in ignore: 
            if 'impute' in c: 
                column_type_dict[c] = 'numeric'
            if 'binned' in c: 
                column_type_dict[c] = 'enum'
    return column_type_dict

def score(df, model, column_types={}):
    predictors = [k for k in column_types.keys()]
    hf = h2o.H2OFrame(df[predictors], column_types=column_types)
    return model.predict(hf).as_data_frame()['p1']