import pandas as pd
import os
import process.processa_dados as processa


#/********************************************************************************************
# FUNÇÕES AUXILIARES
#********************************************************************************************/
def write_DataFrame_csv(df, path):    
    df.to_csv(path, index=False, sep=';', decimal=',', encoding='latin1')    

def read_Dataframe_csv(path):    
    df = pd.read_csv(path, sep=';', decimal=',', encoding='latin1')        
    return df

