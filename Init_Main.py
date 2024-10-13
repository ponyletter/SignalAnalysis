import datetime
import numpy as np
import pandas as pd
import os


Ratio=0.6

if not os.path.exists('Signal'):
    os.mkdir('Signal')

items=pd.read_csv('Items.csv',header=None)
items=items[0].values.tolist()
items=[item[:-4] for item in items]

dates=pd.read_csv('Dates_2010.csv',header=None)
dates=dates[dates[0]<='2024-02-31']  ##########!!!!!!!!!!!!!!change date if necessary
dates=dates[0].values.tolist()

Main=pd.DataFrame(index=dates,columns=items)
Main2=pd.DataFrame(index=dates,columns=items)

for item in items:
    
    print(item)
    
    filename=item
    filename=filename+'.h5'
    
    d=pd.read_hdf('MarketDay/'+filename,key='d')
    
    d['OIVolume']=d['VOLUME']+d['OI']*0.1
    
    first=0
    yesterday=0
    for datestr in dates:
        date=datetime.datetime.strptime(datestr,'%Y-%m-%d').date()

        dd=d[d['DATE']==date].copy()

        if len(dd)<=1 or dd['OIVolume'].isna().all()==1:
            continue
        
        dd=dd.sort_values(by='Codes')
        dd=dd.iloc[:-1]

        if first<1:
            first=first+1
            
            large=dd['OIVolume'].nlargest(2)
            
            if large.index[0]==dd.index[-1]:
                Contract1=dd.loc[large.index[1]]['Codes']
                Contract2=dd.loc[large.index[0]]['Codes']
            else:
                Contract1=dd.loc[large.index[0]]['Codes']
                Contract2=dd.loc[large.index[1]]['Codes']

            Main[item][datestr]=Contract1
            Main2[item][datestr]=Contract2

        else:
            
            MainYesterday=Main[item][yesterday]
            Main2Yesterday=Main2[item][yesterday]
            
            if MainYesterday==dd.iloc[-1]['Codes']:
                Contract1=MainYesterday
            else:
                ddd=dd[dd['Codes']>=MainYesterday]
                
                large=ddd['OIVolume'].nlargest(2)
                if MainYesterday in dd['Codes'].values:     
                    if MainYesterday==ddd.loc[large.index[1]]['Codes']:
                        Contract1=ddd.loc[large.index[0]]['Codes']
                    else:
                        if MainYesterday==dd.iloc[0]['Codes']:
                            if large.iloc[0]*Ratio>large.iloc[1]:
                                Contract1=ddd.loc[large.index[0]]['Codes']
                            else:
                                Contract1=ddd.loc[large.index[1]]['Codes']
                        else:
                            if large.iloc[0]>=large.iloc[1]:
                                Contract1=ddd.loc[large.index[0]]['Codes']
                            else:
                                Contract1=ddd.loc[large.index[1]]['Codes']

                else:
                    Contract1=ddd.loc[large.index[0]]['Codes']
                
                    print(datestr,item)
            
            ddd=dd[dd['Codes']!=Contract1]
            large=ddd['OIVolume'].nlargest(2)
            if Main2Yesterday not in ddd['Codes'].values:
                Contract2=ddd.loc[large.index[0]]['Codes']
            elif Main2Yesterday==ddd.loc[large.index[0]]['Codes']:
                Contract2=Main2Yesterday
            elif large.iloc[1]>=large.iloc[0]*Ratio:
                Contract2=Main2Yesterday
            else:        
                Contract2=ddd.loc[large.index[0]]['Codes']

            Main[item][datestr]=Contract1
            Main2[item][datestr]=Contract2
              

        yesterday=date.strftime('%Y-%m-%d')
        
Main.to_hdf('Signal/Main.h5',key='d',mode='w')
Main2.to_hdf('Signal/Main2.h5',key='d',mode='w')



