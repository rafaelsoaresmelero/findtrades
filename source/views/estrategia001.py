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

def processamento_Parametros():
    with st.form("form_calculo"):
        # Entrada para valor percentual
        percentual = st.number_input("Informe o valor do percentual (%))", min_value=-100.0, step=0.1)        
        
        valor_moeda = st.number_input("Informe o valor do investimento (R$)", min_value=0.0, step=0.01)        
        
        calcular = st.form_submit_button("Calcular")
        
        if calcular:
            stg.processar_ODS_Estrategia001_Main(percentual, valor_moeda)
            st.success(f'Processado com sucesso! com os parametros: {percentual} e {valor_moeda}')    

def gridDados(df_dados_diarios_consolidado)    :
    
    st.header('Resultado do Processamento', divider='rainbow')                
    #****************************************************************************
    # Filtros Grid
    #****************************************************************************
    
    aplicar_positivo100d = st.checkbox("Positivo nos últimos 100 dias", value=True)
    aplicar_positivo50d = st.checkbox("Positivo nos últimos 50 dias", value=True)
    aplicar_positivo20d = st.checkbox("Positivo nos últimos 20 dias", value=True)
    
    if aplicar_positivo100d:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado[df_dados_diarios_consolidado['L/P Total 100d'] > 0]
    else:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado    

    if aplicar_positivo50d:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado[df_dados_diarios_consolidado['L/P Total 50d'] > 0]
    else:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado    

    if aplicar_positivo20d:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado[df_dados_diarios_consolidado['L/P Total 20d'] > 0]
    else:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado    

    codneg_selecionado = st.multiselect('Selecione o CODNEG:', sorted(df_dados_diarios_consolidado['CODNEG'].unique(), reverse=True), placeholder="Escolha um codneg")    

    if not codneg_selecionado: 
        df_dados_diarios_consolidado = df_dados_diarios_consolidado
    else:
        df_dados_diarios_consolidado = df_dados_diarios_consolidado[df_dados_diarios_consolidado['CODNEG'].isin(codneg_selecionado)]

    st.dataframe(data=df_dados_diarios_consolidado.style.format(thousands=".", precision=2), hide_index=True)    

    # if codneg_selecionado:         
    #     st.header('Detalhe', divider='rainbow')                
        
    #     arquivos_Diario = [(os.path.join(stg.path_Data_ODS_Acoes_Diario, f)) 
    #                 for f in os.listdir(stg.path_Data_ODS_Acoes_Diario) if (os.path.isfile(os.path.join(stg.path_Data_ODS_Acoes_Diario, f)))]
    #     arquivos_Diario.sort()    
        
    #     for arquivo in arquivos_Diario:
            
    #         if arquivo in codneg_selecionado:
    #             df_dados = aux.read_Dataframe_csv(arquivo)        
    #             df_dados = df_dados.tail(1)
                
    #             st.header('', divider='rainbow')                        

    #             st.dataframe(data=df_dados.tail(20), hide_index=True)            

def createPage():
    
    st.header('Calcular Geral', divider='rainbow')                

    processamento_Parametros()

    #****************************************************************************
    # Leitura de dados
    #****************************************************************************
    df_dados_diarios_consolidado = aux.read_Dataframe_csv(stg.path_Data_DM_Acoes_Estrategia001_Diario_Consolidado)

    gridDados(df_dados_diarios_consolidado)



    

