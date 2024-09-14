import streamlit as st
from streamlit_option_menu import option_menu
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import process.stg_dados as stg
import process.auxiliares as aux

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
    
    st.header('Calcular Geral', divider='rainbow')                

    with st.form("form_calculo"):
        # Entrada para valor percentual
        percentual = st.number_input("Informe o valor do percentual (%))", min_value=-100.0, step=0.1)        
        
        valor_moeda = st.number_input("Informe o valor do investimento (R$)", min_value=0.0, step=0.01)        
        
        calcular = st.form_submit_button("Calcular")
        
        if calcular:
            stg.processar_ODS_Diario(percentual, valor_moeda)
            st.success(f'Processado com sucesso! com os parametros: {percentual} e {valor_moeda}')    

    df_dados_diarios_consolidado = aux.read_Dataframe_csv(stg.path_Data_DM_Acoes_Estrategia001_Diario_Consolidado)

    
    st.dataframe(data=df_dados_diarios_consolidado,
                    hide_index=True
                    )    
    
    st.header('Detalhe', divider='rainbow')                

    arquivos_Diario = [(os.path.join(stg.path_Data_ODS_Acoes_Diario, f)) 
                for f in os.listdir(stg.path_Data_ODS_Acoes_Diario) if os.path.isfile(os.path.join(stg.path_Data_ODS_Acoes_Diario, f))]
    arquivos_Diario.sort()    
    
    for arquivo in arquivos_Diario:
        df_dados = aux.read_Dataframe_csv(arquivo)        
        df_dados = df_dados.tail(1)

        #if (df_dados['L/P Valor'] != 0).any():
        
        st.header('', divider='rainbow')                        

        st.dataframe(data=df_dados.tail(20),
                        hide_index=True
                        )    
    

