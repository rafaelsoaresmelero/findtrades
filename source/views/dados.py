import streamlit as st
from streamlit_option_menu import option_menu
import process.stg_dados as stg
import time
from datetime import datetime, timedelta
import pandas as pd
import os

def createMenu():
    selected = option_menu('DADOS', ['HOME', 'Atualizar Tudo', 'Atualizar Intraday', 'Atualizar Diário'], icons=['home', 'globe', 'water', 'snow'], menu_icon="fire", default_index=0, orientation="horizontal")

    if selected == 'Atualizar Tudo':    
        with st.spinner('Processando...'):
            stg.baixar_Tudo()
        sucesso = st.success('Processado com sucesso!', icon="✅")
        time.sleep(1) 
        sucesso.empty()
    elif selected == 'Atualizar Intraday':    
        with st.spinner('Processando...'):
            stg.baixar_Intraday()
        sucesso = st.success('Processado com sucesso!', icon="✅")
        time.sleep(1) 
        sucesso.empty()
    elif selected == 'Atualizar Diário':
        with st.spinner('Processando...'):
            stg.baixar_Diario()
        sucesso = st.success('Processado com sucesso!', icon="✅")
        time.sleep(1) 
        sucesso.empty()


def gridDados(df_Arquivos_Intraday, df_Arquivos_Diarios):
    
    codneg_selecionado = st.multiselect('Selecione as ações:', df_Arquivos_Intraday['CODNEG'].unique(),placeholder="Escolha as ações")
    
    if not codneg_selecionado:
        df_Arquivos_Intraday = df_Arquivos_Intraday
        df_Arquivos_Diarios = df_Arquivos_Diarios
    else:
        df_Arquivos_Intraday = df_Arquivos_Intraday[df_Arquivos_Intraday['CODNEG'].isin(codneg_selecionado)]   
        df_Arquivos_Diarios = df_Arquivos_Diarios[df_Arquivos_Diarios['CODNEG'].isin(codneg_selecionado)]   
    
    col1, col2 = st.columns(2)    
    
    with col1:        
        st.header('Intraday', divider='rainbow')                
        st.dataframe(data=df_Arquivos_Intraday,
                    hide_index=True
                    )    

    with col2:        
        st.header('Diário', divider='rainbow')
        st.dataframe(data=df_Arquivos_Diarios,
                    hide_index=True
                    )    

def createPage():
    
    ## Leitura dos dados
    #Pegar lista de arquivos Intraday
    files = os.listdir(stg.path_Data_STG_Acoes_Intraday)
    list_Arquivos_Intraday = [(f.replace('.SA.csv', ''), datetime.fromtimestamp(os.path.getmtime(os.path.join(stg.path_Data_STG_Acoes_Intraday, f))))
                for f in os.listdir(stg.path_Data_STG_Acoes_Intraday) if os.path.isfile(os.path.join(stg.path_Data_STG_Acoes_Intraday, f))]

    #Pegar lista de arquivos Diários
    files = os.listdir(stg.path_Data_STG_Acoes_Diario)
    list_Arquivos_Diarios = [(f.replace('.SA.csv', ''), datetime.fromtimestamp(os.path.getmtime(os.path.join(stg.path_Data_STG_Acoes_Diario, f)))) 
                for f in os.listdir(stg.path_Data_STG_Acoes_Diario) if os.path.isfile(os.path.join(stg.path_Data_STG_Acoes_Diario, f))]

    df_Arquivos_Intraday = pd.DataFrame(list_Arquivos_Intraday, columns=['CODNEG', 'Last Modified'])
    df_Arquivos_Intraday.sort_values(by='CODNEG',inplace=True)
    df_Arquivos_Diarios = pd.DataFrame(list_Arquivos_Diarios, columns=['CODNEG', 'Last Modified'])    
    df_Arquivos_Diarios.sort_values(by='CODNEG',inplace=True)
    
    createMenu()

    gridDados(df_Arquivos_Intraday, df_Arquivos_Diarios)    
    