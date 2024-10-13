import datetime
import numpy as np
import pandas as pd
import os

dates=pd.read_csv('Dates_2010.csv',header=None)
dates=dates[dates[0]<='2024-02-31']
dates=dates[0].values.tolist()

items=pd.read_csv('Items.csv',header=None)
items=items[0].values.tolist()
items=[item[:-4] for item in items]


Mains=pd.read_hdf('Signal/Main.h5',header=None)
MainSubs=pd.read_hdf('Signal/Main2.h5',header=None)


#########Init#########

Returns=pd.DataFrame(index=dates,columns=items)

AmtAlls=pd.DataFrame(index=dates,columns=items)

TSas=pd.DataFrame(index=dates,columns=items)########初始化TS信号文件
TSbs=pd.DataFrame(index=dates,columns=items)
TScs=pd.DataFrame(index=dates,columns=items)

#########Returns Amts OIs#########
for item in items:
    filename=item+'.h5'
    
    print(item)
    print(datetime.datetime.now())    
    
    Data=pd.read_hdf('MarketDay/'+filename,header=None)

    date_i=0
    
    yesterday=0
                
    for datestr in dates:

        Main=Mains.at[datestr,item]
        
        if type(Main)!=str:
            continue
        
        date=datetime.datetime.strptime(datestr,'%Y-%m-%d')
        date=datetime.datetime.date(date)
        
        if date_i==0:
            date_i=1
            Open=Data[(Data['DATE']==date) & (Data['Codes']==Main)].iloc[0]['OPEN']
        else:
            if len(Data[(Data['DATE']==yesterday) & (Data['Codes']==Main)])==0:
                Open=Data[(Data['DATE']==date) & (Data['Codes']==Main)].iloc[0]['OPEN']
            else:
                Open=Data[(Data['DATE']==yesterday) & (Data['Codes']==Main)].iloc[0]['CLOSE']
            
        
        DataDay=Data[Data['DATE']==date]  
        DataMain=DataDay[DataDay['Codes']==Main]
        
        Close=DataMain.iloc[0]['CLOSE']

        
        Returns.at[datestr,item]=Close/Open-1
        AmtAlls.at[datestr,item]=DataDay['AMT'].sum()
        
#########TermStructure#########

        MainSub=MainSubs.at[datestr,item]
        
        DataMainSub=DataDay[DataDay['Codes']==MainSub]
        CloseSub=DataMainSub.iloc[0]['CLOSE']
        
        TSas.at[datestr,item]=(Close/CloseSub-1)########TS的定义a：主力合约相对于次主力合约的升水率
        
        DateYear=date.year
        DateMonth=date.month
        
        MainMonth=int(Main[-6:-4])
        MainSubMonth=int(MainSub[-6:-4])
        
        if str.isalpha(Main[-8]):
            if Main[-7]==str(DateYear)[-1]:
                MainYear=DateYear
            else:
                MainYear=DateYear+1
                #print(Main,MainSub,date,MainYear)
        else:
            MainYear=int('20'+Main[-8:-6])
                
        if str.isalpha(MainSub[-8]):
            if MainSub[-7]==str(DateYear)[-1]:
                MainSubYear=DateYear
            else:
                MainSubYear=DateYear+1
        else:
            MainSubYear=int('20'+MainSub[-8:-6])
        
        Delta=0
        if MainYear==MainSubYear:
            Delta=MainMonth-MainSubMonth
        elif MainYear>MainSubYear:
            Delta=MainMonth+12*(MainYear-MainSubYear)-MainSubMonth
        elif MainYear<MainSubYear:
            Delta=MainMonth-12*(MainSubYear-MainYear)-MainSubMonth

        TSbs.at[datestr,item]=(CloseSub/Close-1)*(12/Delta)########TS的定义b：考虑了到期日的主力合约相对于次主力合约的升水率
        
        
        TScs.at[datestr,item]=DataDay['CLOSE'].iloc[0]/DataDay['CLOSE'].iloc[-1]-1########TS的定义c：最晚到期合约相对于最早到期合约的升水率
        
        yesterday=date
        

Returns.to_hdf('Signal/Returns.h5',key='d',mode='w')

AmtAlls.to_hdf('Signal/AmtAlls.h5',key='d',mode='w')

TSas.to_hdf('Signal/TSas.h5',key='d',mode='w')########保存TS信号文件
TSbs.to_hdf('Signal/TSbs.h5',key='d',mode='w')
TScs.to_hdf('Signal/TScs.h5',key='d',mode='w')


        

        