from datetime import *
import pandas as pd
import time
import glob
import  numpy as np
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')


path = r'/home/sourav/DataSet/IndianNSEEquity'
files = glob.glob(path + "/*.csv")

print(len(files))

Symbols = []

for file in files:
    Symbols.append(file.split('/')[-1].split('.')[0])
    
testing_time = datetime(2017,1,1)
starting_period = '2017-01-01'
ending_period = '2017-12-31'


df = pd.read_csv(path + r'/TCS.csv')
df.date = pd.to_datetime(df.date)
df.set_index('date' , inplace=True)
dateIndex = df[(df.index > starting_period) & ((df.index < ending_period) )].index
Orders = pd.DataFrame(columns=['entry', 'type', 'entry_time', 'exittime',
                      'entry_value', 'exitvalue', 'exit_type', 'UP', 'SP', 'dd', 'BasketID', 'HourlyMove'])


SelectedFiles = []
SelectedSymbols = []
NoDataCount = 0
Count = 0
for file in files:
    Symbol = Symbols[Count]
    df = pd.read_csv(file)
    df.date = pd.to_datetime(df.date)
    df.set_index('date' , inplace=True)
    
    if df.index[0] > testing_time:
        NoDataCount+=1
        Count+=1
        continue
    
    
    df2017 = df[(df.index > starting_period) & ((df.index < ending_period) )]
    df2017Daily = df2017.resample('D' ).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }).dropna()
    #SelectedFiles.append(file)
    Volume2017 = df2017Daily['volume'].mean()
    
    if Volume2017 > 100000:
        SelectedFiles.append(file)
        SelectedSymbols.append(Symbol)
    else:
        pass
    

    
    
    Count+=1
    




SelectedSymbols.remove('ICEMAKE')
SelectedFiles.remove(path + '/ICEMAKE.csv')



def get_top_stocks(dfs, TargetDate , LookBacks , NoOfStocks , files,Symbols, HoldingBar):
    
    AvgPerChanges = []
    returndf = pd.DataFrame(columns=['stock','returns_lb1' , 'returns_lb2' , 'returns_lb3' , 'returns_lb4' , 'returns_lb5' , 'returns_lb6',\
        'returns_lb7' , 'returns_lb8' , 'returns_lb9' , 'returns_lb10' , 'returns_lb11' , 'returns_lb12' ,\
            'returns_lb13' , 'returns_lb14' , 'returns_lb15' , 'returns_lb16' , 'returns_lb17' , 'returns_lb18' , \
                'returns_lb19' , 'returns_lb20' , 'returns_lb21' , 'path'])
  
  
  
    for Symbol, file in zip(Symbols, files):
        df = dfs[Symbol].copy()
        if df.index[0] > TargetDate:
            continue
        returns = [((df.loc[:TargetDate].iloc[-Lookback:].iloc[-1]['close'] - df.loc[:TargetDate].iloc[-Lookback:].iloc[0]['open'])/df.loc[:TargetDate].iloc[-Lookback:].iloc[0]['open']) * 100   \
            for Lookback in LookBacks]
        returndf.loc[len(returndf)] = [Symbol] + returns + [file]
        
        
        
    for LookBack in LookBacks:
        rtdf2 = returndf.sort_values(by=f'returns_lb{LookBack}', ascending=False ).reset_index(drop=True).iloc[:NoOfStocks]
        
        PerChanges = list(((dfs[Symbol].copy().loc[TargetDate:].iloc[1:].iloc[:HoldingBar].iloc[-1]['close'] -dfs[Symbol].copy().loc[TargetDate:].iloc[1:].iloc[:HoldingBar].iloc[0]['open'] )/(dfs[Symbol].copy().loc[TargetDate:].iloc[1:].iloc[:HoldingBar].iloc[0]['open'] ) * 100) for Symbol in rtdf2['stock'])
        AvgPerChange = sum(PerChanges)/len(PerChanges)
        AvgPerChanges.append(AvgPerChange)
        
        
    return AvgPerChanges , dfs[Symbol].copy().loc[TargetDate:].iloc[1:].iloc[:HoldingBar].index[-1]
        
        
        
    

BacktesingStartDate = '2018-01-01'
BacktesingEndDate = '2021-12-31'



# ReturnsDf = pd.DataFrame(columns= ['EntryTime' , 'AvgReturn_lb1' , 'AvgReturn_lb2' , 'AvgReturn_lb3' , 'AvgReturn_lb4' , 'AvgReturn_lb5' , 'AvgReturn_lb6' , 'AvgReturn_lb7' , \
#     'AvgReturn_lb8' , 'AvgReturn_lb9' , 'AvgReturn_lb10' , 'AvgReturn_lb11' , 'AvgReturn_lb12' , 'AvgReturn_lb13' , 'AvgReturn_lb14' , \
#         'AvgReturn_lb15' , 'AvgReturn_lb16' , 'AvgReturn_lb17' , 'AvgReturn_lb18' , 'AvgReturn_lb19' , 'AvgReturn_lb20' , 'AvgReturn_lb21' ])

def get_one_comb_avg_ret(dateIndex , Lookback, TopStocks , HoldingBars ,ReturnsDf):
    dfs = {Symbol: pd.read_csv(file) for Symbol, file in zip(SelectedSymbols, SelectedFiles)}
    for i in dfs.keys():
        dfs[i]['date'] = pd.to_datetime(dfs[i]['date'])
        dfs[i].set_index('date', inplace=True)
    EndTime = dateIndex[0] - timedelta(days=1)
    for i in range(len(dateIndex)):
        if dateIndex[i] < EndTime:
            continue
        else:
            t1 = time.time()
            testdf , EndTime= get_top_stocks(dfs,dateIndex[i],Lookback,TopStocks,SelectedFiles, SelectedSymbols , HoldingBars )
            print([dateIndex[i] , testdf])
            ReturnsDf.loc[len(ReturnsDf)] = [dateIndex[i]] + testdf
            print(f'Total Time : {t1-time.time()}')
            
        
            
            
df = pd.read_csv(path + r'/TCS.csv')
df.date = pd.to_datetime(df.date)
df.set_index('date' , inplace=True)
dateIndex = df[(df.index > BacktesingStartDate) & ((df.index < BacktesingEndDate) )].index


import pandas as pd
from multiprocessing import Pool

def process_iteration(i):
    print(f'Holding Bar No : {i}')
    
    ReturnsDf = pd.DataFrame(columns= ['EntryTime' , 'AvgReturn_lb1' , 'AvgReturn_lb2' , 'AvgReturn_lb3' , 'AvgReturn_lb4' , 'AvgReturn_lb5' , 'AvgReturn_lb6' , 'AvgReturn_lb7' , \
    'AvgReturn_lb8' , 'AvgReturn_lb9' , 'AvgReturn_lb10' , 'AvgReturn_lb11' , 'AvgReturn_lb12' , 'AvgReturn_lb13' , 'AvgReturn_lb14' , \
        'AvgReturn_lb15' , 'AvgReturn_lb16' , 'AvgReturn_lb17' , 'AvgReturn_lb18' , 'AvgReturn_lb19' , 'AvgReturn_lb20' , 'AvgReturn_lb21' ])
    get_one_comb_avg_ret(dateIndex, [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21],10,i, ReturnsDf)
    
    ReturnsDf.to_csv(f'returns_Holdibar_{i}_Top10.csv', index=False)



if __name__ == '__main__':
    # Create a pool of processes
    with Pool(5) as pool:
        # Map the process_iteration function to each item in the list
        pool.map(process_iteration, sorted([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21], reverse=True))

