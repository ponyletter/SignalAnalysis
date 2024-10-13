import datetime
import numpy as np
import pandas as pd
import math
import os

if not os.path.exists('Result'):
    os.mkdir('Result')

#########Parameters#########

DateStart='2016-01-01'
DateMid='2022-07-01'
DateEnd='2024-03-01'

P1s=[13,21,34,55,89,144,233,377]
P2s=[13,21,34,55,89,144,233,377,610]

# P1s=[21]
# P2s=[144]

POne=0
if len(P1s)*len(P2s)==1:
    POne=1

AUM=10000000
TC=0.0005

#########PrepareData#########

Dates=pd.read_csv('Dates_2010.csv',header=None)
Dates=Dates[(Dates[0]>=DateStart) & (Dates[0]<DateEnd)]
Dates=Dates[0].values.tolist()

Items=pd.read_csv('Items.csv',header=None)
Items=Items[0].values.tolist()
Items=[Item[:-4] for Item in Items]

Mains=pd.read_hdf('Signal/Main.h5',header=None)


Returns=pd.read_hdf('Signal/Returns.h5',header=None)
AmtAlls=pd.read_hdf('Signal/AmtAlls.h5',header=None)

TSs=pd.read_hdf('Signal/TSas.h5',header=None) ########将TSas修改为TSbs或TScs可以读取不同的TS信号文件

Results=pd.DataFrame()

#########Backtest#########
for P1 in P1s:
    for P2 in P2s:
        
        if P1>=P2:
            continue
            
        print(P1,P2)
        print(datetime.datetime.now())          
        #########Init All#########

        PNL=pd.DataFrame(index=Dates,columns=['d'])
        PNLItem=pd.DataFrame(index=Dates,columns=Items)
        
        Books=pd.DataFrame(index=Items,columns=['Rtn','Pos','TPos','Signal','Wgt','Open','Contract','TContract'])
        
        for DateStr in Dates:

            #########Init Day#########
            
            Ri=Returns.index.get_loc(DateStr)-1
            Di=Ri-1455
            
            for Item in Items:
                            
                if np.isnan(AmtAlls[Item].iloc[Ri]):
                    Books.at[Item,'Signal']=np.nan
                    Books.at[Item,'Wgt']=np.nan
                elif AmtAlls[Item].iloc[Ri]<10**9:
                    if np.isnan(Books.at[Item,'Pos']):
                        Books.at[Item,'Signal']=np.nan
                        Books.at[Item,'Wgt']=np.nan
                    else:
                        Books.at[Item,'Signal']=0
                        Books.at[Item,'Wgt']=0
                else:
                    
                    Books.at[Item,'Signal']=np.sign(TSs.iloc[Ri-P1:Ri+1][Item].mean()-TSs.iloc[max(Ri-math.ceil(P2),0):Ri+1][Item].mean())
                    Books.at[Item,'Wgt']=0.013/Returns.iloc[Ri-20+1:Ri+1][Item].std()
                

            Books['Rtn']=0.0
            Books['Signal']=Books['Signal']/Books['Signal'].abs().sum()
            Books['TPos']=Books['Signal']*Books['Wgt']

            #########For Loop Minutes#########
            
            for Item in Items:

                if np.isnan(Books.at[Item,'TPos']):
                    continue
                
                
                if np.isnan(Books.at[Item,'Pos']):
                    Books.at[Item,'Rtn']=Books.at[Item,'TPos']*Returns.at[DateStr,Item]
                else:
                    Books.at[Item,'Rtn']=Books.at[Item,'TPos']*Returns.at[DateStr,Item]-abs(Books.at[Item,'TPos']-Books.at[Item,'Pos'])*TC
                
            Books['Pos']=Books['TPos']
                
            #########Close Day#########
            
            if POne==1:
                PNLItem.iloc[Di]=Books['Rtn']

            PNL.iloc[Di]['d']=Books['Rtn'].dropna().sum()           
        
        #########Stats#########
        Result=PNL.mean()/PNL.std()*250**0.5
        Result=Result.to_frame(name='d')
        
        SR2=PNL.loc[PNL.index>=DateMid].mean()/PNL.loc[PNL.index>=DateMid].std()*250**0.5
        
        Result=Result._append(SR2,ignore_index=True)
        
        Result=Result._append(pd.DataFrame({'d':[P1]}),ignore_index=True)
        Result=Result._append(pd.DataFrame({'d':[P2]}),ignore_index=True)

        TR=PNL.cumsum()
        Result=pd.concat([Result,TR],axis=0)

        Results=pd.concat([Results,Result],axis=1)

Results=Results.T

Results.to_excel('Result/Result.xlsx')

if POne==1:
    PNLItem.to_excel('Result/PNLItem.xlsx')
        
                

        