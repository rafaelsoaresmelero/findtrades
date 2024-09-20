import yfinance as yf
import pandas as pd
import numpy as np
import threading
import concurrent.futures
import os
import logging
import shutil
import process.auxiliares as aux


###PARÂMETROS

#Configurando o log no terminal
logging.basicConfig(
    level=logging.INFO,  # Definir o nível de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato de saída
    force=True
)

#Configurando pastas
path_Data = '../data'
path_Data_STG = path_Data + '/STG'
path_Data_STG_Controle = path_Data_STG + '/controle/listacodnegs.csv'
path_Data_STG_Acoes_Intraday = path_Data_STG + '/acoes_intraday/'
path_Data_STG_Acoes_Diario = path_Data_STG + '/acoes_diario/'

path_Data_ODS = path_Data + '/ODS'
path_Data_ODS_Acoes_Estrategia001_Diario = path_Data_ODS + '/acoes_diario_estrategia001'
path_Data_ODS_Acoes_Estrategia002_Diario = path_Data_ODS + '/acoes_diario_estrategia002'

path_Data_DM = path_Data + '/DM'
path_Data_DM_Acoes_Estrategia001_Diario_Consolidado = path_Data_DM + '/acoes_diario_estrategia001_consolidado.csv'
path_Data_DM_Acoes_Estrategia002_Diario_Consolidado = path_Data_DM + '/acoes_diario_estrategia002_consolidado.csv'

#path_Data_DM_Acoes_Estrategia001001_Diario_Detalhe = path_Data_DM + '/estrategia001_diario_detalhe.csv'


#Maximas threads concorrentes
max_threads = 10
acoes = []
dados_acoes_estrategia001 = []    
dados_acoes_estrategia002 = []    

def limpar_pasta(caminho_pasta):    
    if os.path.exists(caminho_pasta):        
        for arquivo in os.listdir(caminho_pasta):
            caminho_arquivo = os.path.join(caminho_pasta, arquivo)            
            
            if os.path.isfile(caminho_arquivo) or os.path.islink(caminho_arquivo):
                os.unlink(caminho_arquivo)  
            elif os.path.isdir(caminho_arquivo):
                shutil.rmtree(caminho_arquivo)          
    else:
        logging.error(f"A pasta '{caminho_pasta}' não existe.")        

# Função para criar pastas se não existirem
def criar_pasta(caminho):
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        logging.info(f"Pasta '{caminho}' criada.")        
    else:
        limpar_pasta(caminho)
        logging.info(f"Conteúdo da pasta '{caminho}' apagado.")

def baixar_dados_acao(ticker, path_dados, interval):   
    
    path_file = path_dados + '/' + ticker.upper() + '.csv'
    
    acao = yf.Ticker(ticker)
    dados = acao.history(period='2y', interval=interval)  
    
    dados['CODNEG'] = ticker
    
    if len(dados) > 1:
        dados.to_csv(path_file, sep=';', decimal=',')    
    
    logging.info(f"{interval} - Dados de {ticker} finalizados.")    


#********************************************************************************************************
# ESTRATEGIA 001 - INICIO
#********************************************************************************************************
def processar_ODS_Estrategia001(path_arquivo, variacao, valor_investido):   
    
    df_dados = aux.read_Dataframe_csv(path_arquivo)
    df_dados['Date'] = pd.to_datetime(df_dados['Date'])
    df_dados.sort_values(by='Date', inplace=True)
    df_dados['Last Close'] = df_dados['Close'].shift(1)
    
    df_dados['Var da Minima %'] = ((df_dados['Low'] / df_dados['Last Close']) - 1) * 100
    df_dados['Var Fech %'] = ((df_dados['Close'] / df_dados['Last Close']) - 1) * 100
    df_dados['Lucro/Prejuizo %'] = np.where(df_dados['Var da Minima %'] <= variacao, df_dados['Var Fech %'] - variacao, 0)
    df_dados['Valor de Entrada'] = np.where(df_dados['Var da Minima %'] <= variacao, df_dados['Last Close']*(1+(variacao/100)), 0)    
    df_dados['L/P Valor'] = valor_investido * (df_dados['Lucro/Prejuizo %']/100)
    df_dados['Valor de Entrada'] = np.where(df_dados['Var da Minima %'] <= variacao, df_dados['Last Close']*(1+(variacao/100)), 0)

    arquivo_salvar = path_Data_ODS_Acoes_Estrategia001_Diario + '/' + os.path.basename(path_arquivo)

    aux.write_DataFrame_csv(df_dados, arquivo_salvar)   

    #soma periodos específicos
    df_dados_periodo = df_dados.copy()
    
    df_dados100 = df_dados_periodo.tail(100)    
    soma_lp100 = df_dados100['L/P Valor'].sum()    
    qtde_trades_lp100_total = df_dados100[df_dados100['L/P Valor'] != 0]['L/P Valor'].count()    
    qtde_trades_lp100_positivos = (df_dados100['L/P Valor'] > 0).sum()
    qtde_trades_lp100_negativos = (df_dados100['L/P Valor'] < 0).sum()

    df_dados50 = df_dados_periodo.tail(50)
    soma_lp50 = df_dados50['L/P Valor'].sum()    
    qtde_trades_lp50_total = df_dados50[df_dados50['L/P Valor'] != 0]['L/P Valor'].count()
    qtde_trades_lp50_positivos = (df_dados50['L/P Valor'] > 0).sum()
    qtde_trades_lp50_negativos = (df_dados50['L/P Valor'] < 0).sum()

    df_dados20 = df_dados_periodo.tail(20)    
    soma_lp20_total = df_dados20['L/P Valor'].sum()
    qtde_trades_lp20_total = df_dados20[df_dados20['L/P Valor'] != 0]['L/P Valor'].count()        
    qtde_trades_lp20_positivos = (df_dados20['L/P Valor'] > 0).sum()
    qtde_trades_lp20_negativos = (df_dados20['L/P Valor'] < 0).sum()

    df_dados1 = df_dados_periodo.tail(1)
    valor_entrada_nova = float(df_dados1['Close'].iloc[0]*(1+(variacao/100)))
        
    codneg = df_dados['CODNEG'].unique()[0]
    
    dados_acoes_estrategia001.append({'CODNEG': codneg,
                             'Valor Entrada Nova': valor_entrada_nova,
                             'Parametros - Variacao': variacao,                             
                             'Lucro/Prejuizo % - Min': df_dados['Lucro/Prejuizo %'].min(),
                             'Lucro/Prejuizo % - Max': df_dados['Lucro/Prejuizo %'].max(),
                             'Lucro/Prejuizo % - Avg': df_dados['Lucro/Prejuizo %'].mean(),
                             'L/P Valor - Min': df_dados['L/P Valor'].min(),
                             'L/P Valor - Max': df_dados['L/P Valor'].max(),
                             'L/P Valor - Avg': df_dados['L/P Valor'].mean(),
                             'L/P Total 20d': soma_lp20_total,                             
                             'L/P Count 20d': qtde_trades_lp20_total,
                             'L/P Count 20d positivos': qtde_trades_lp20_positivos,
                             'L/P Count 20d negativos': qtde_trades_lp20_negativos,
                             'L/P Total 50d': soma_lp50,
                             'L/P Count 50d': qtde_trades_lp50_total,
                             'L/P Count 50d positivos': qtde_trades_lp50_positivos,
                             'L/P Count 50d negativos': qtde_trades_lp50_negativos,
                             'L/P Total 100d': soma_lp100,
                             'L/P Count 100d': qtde_trades_lp100_total,
                             'L/P Count 100d positivos': qtde_trades_lp100_positivos,
                             'L/P Count 100d negativos': qtde_trades_lp100_negativos,                                                          
                        })    
    
    logging.info(f"Dados de {os.path.basename(path_arquivo)} finalizados.")    

def processar_ODS_Estrategia001_Main(variacao, valor_investido):

    criar_pasta(path_Data_ODS_Acoes_Estrategia001_Diario)

    arquivos_Diario = [(os.path.join(path_Data_STG_Acoes_Diario, f)) 
                for f in os.listdir(path_Data_STG_Acoes_Diario) if os.path.isfile(os.path.join(path_Data_STG_Acoes_Diario, f))]

    threads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(processar_ODS_Estrategia001, arquivo, variacao, valor_investido) for arquivo in arquivos_Diario]

        # Aguardar todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Captura exceções das threads, se houver
            except Exception as e:
                logging.error(f"Erro ao baixar dados da ação: {e}")

    df_acoes_consolidada = pd.DataFrame(dados_acoes_estrategia001)
    
    #Gerar arquivo consolidado
    aux.write_DataFrame_csv(df_acoes_consolidada, path_Data_DM_Acoes_Estrategia001_Diario_Consolidado)   

    logging.info(f"Processamento finalizados.")    
#********************************************************************************************************
# ESTRATEGIA 001 - FIM
#********************************************************************************************************    

#********************************************************************************************************
# ESTRATEGIA 002 - INICIO
#********************************************************************************************************
def processar_ODS_Estrategia002(path_arquivo):
    
    df_dados = aux.read_Dataframe_csv(path_arquivo)
    
    #Salvar Detalhe    
    df_dados['Date'] = pd.to_datetime(df_dados['Date'])
    df_dados.sort_values(by='Date', inplace=True)
    
    df_dados['WeekDay'] = df_dados['Date'].dt.strftime("%A")
    
    df_dados = df_dados[df_dados['WeekDay'].isin(['Friday', 'Monday', 'Thursday', 'Wednesday'])]
    df_dados.reset_index(inplace=True)
    
    indices_para_remover = []
    valor_entrada_nova = 0
    data_entrada_nova = 0    

    for i in range(1, len(df_dados) - 1):
        dia_atual = df_dados['WeekDay'].iloc[i]
        dia_anterior = df_dados['WeekDay'].iloc[i - 1]    
        dia_anterior_2 = df_dados['WeekDay'].iloc[i - 2]    
        dia_anterior_3 = df_dados['WeekDay'].iloc[i - 3]    
        
        if dia_atual == 'Monday' and dia_anterior == 'Friday' and dia_anterior_2 == 'Thursday' and dia_anterior_3 == 'Wednesday':
            df_dados.at[i, 'High Friday'] = df_dados['High'].iloc[i-1]
            df_dados.at[i, 'Low Friday'] = df_dados['Low'].iloc[i-1]
            df_dados.at[i, 'Close Thursday'] = df_dados['Close'].iloc[i-2]
            df_dados.at[i, 'Close Wednesday'] = df_dados['Close'].iloc[i-3]

    #Pegar ultimo fechamento/data
    df_dados1 = df_dados.copy()
    df_dados1 = df_dados1.tail(1)
    valor_entrada_nova = float(df_dados1['Close'].values[0])
    data_entrada_nova = df_dados1['Date'].values[0]

    df_dados = df_dados.drop(indices_para_remover, axis=0).reset_index(drop=True)
    df_dados.dropna(inplace=True)
    
    df_dados['Trade Aconteceu Bol'] = (df_dados['Close Thursday'] >= df_dados['Low Friday']).astype(int)
    df_dados.loc[df_dados['Trade Aconteceu Bol'] == 1, 'Var Close Thursday Close Monday %'] = ((df_dados['Close Thursday'] / df_dados['Close']) - 1) * 100
    
    arquivo_salvar = path_Data_ODS_Acoes_Estrategia002_Diario + '/' + os.path.basename(path_arquivo)
    
    aux.write_DataFrame_csv(df_dados, arquivo_salvar)   
    
    #Salvar Consolidado
    lucro_prejuizo_minimo = df_dados['Var Close Thursday Close Monday %'].min()
    lucro_prejuizo_maximo = df_dados['Var Close Thursday Close Monday %'].max()
    lucro_prejuizo_media = df_dados['Var Close Thursday Close Monday %'].mean()
    rel_lucro_prejuizo = ((abs(lucro_prejuizo_maximo) / lucro_prejuizo_minimo) -1)*-1

    df_dados_periodo = df_dados.copy()
    
    df_dados100 = df_dados_periodo.tail(100)        
    soma_lp100 = float(df_dados100['Var Close Thursday Close Monday %'].sum())
    qtde_trades_lp100_total = int(df_dados100.loc[df_dados100['Trade Aconteceu Bol'] == 1, 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp100_positivos = int(df_dados100.loc[((df_dados100['Trade Aconteceu Bol'] == 1) & (df_dados100['Var Close Thursday Close Monday %'] > 0)), 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp100_negativos = int(df_dados100.loc[((df_dados100['Trade Aconteceu Bol'] == 1) & (df_dados100['Var Close Thursday Close Monday %'] < 0)), 'Var Close Thursday Close Monday %'].count())
    
    df_dados50 = df_dados_periodo.tail(50)
    soma_lp50 = float(df_dados50['Var Close Thursday Close Monday %'].sum())
    qtde_trades_lp50_total = int(df_dados50.loc[df_dados50['Trade Aconteceu Bol'] == 1, 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp50_positivos = int(df_dados50.loc[((df_dados50['Trade Aconteceu Bol'] == 1) & (df_dados50['Var Close Thursday Close Monday %'] > 0)), 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp50_negativos = int(df_dados50.loc[((df_dados50['Trade Aconteceu Bol'] == 1) & (df_dados50['Var Close Thursday Close Monday %'] < 0)), 'Var Close Thursday Close Monday %'].count())
    
    df_dados20 = df_dados_periodo.tail(20)    
    soma_lp20 = float(df_dados50['Var Close Thursday Close Monday %'].sum())
    qtde_trades_lp20_total = int(df_dados20.loc[df_dados20['Trade Aconteceu Bol'] == 1, 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp20_positivos = int(df_dados20.loc[((df_dados20['Trade Aconteceu Bol'] == 1) & (df_dados20['Var Close Thursday Close Monday %'] > 0)), 'Var Close Thursday Close Monday %'].count())
    qtde_trades_lp20_negativos = int(df_dados20.loc[((df_dados20['Trade Aconteceu Bol'] == 1) & (df_dados20['Var Close Thursday Close Monday %'] < 0)), 'Var Close Thursday Close Monday %'].count())
    
    
    
    codneg = df_dados['CODNEG'].unique()[0]
    
    dados_acoes_estrategia002.append({'CODNEG': codneg,
                             'Valor Entrada Nova': valor_entrada_nova,                             
                             'Data Entrada Nova': data_entrada_nova,
                             'Lucro/Prejuizo % - Min': lucro_prejuizo_minimo,
                             'Lucro/Prejuizo % - Max': lucro_prejuizo_maximo,
                             'Lucro/Prejuizo % - Avg': lucro_prejuizo_media,
                             'Rel L/P %': rel_lucro_prejuizo,
                             #'L/P Valor - Min': df_dados['L/P Valor'].min(),
                             #'L/P Valor - Max': df_dados['L/P Valor'].max(),
                             #'L/P Valor - Avg': df_dados['L/P Valor'].mean(),
                             'L/P Total 20d': soma_lp20,                             
                             'L/P Count 20d': qtde_trades_lp20_total,
                             'L/P Count 20d positivos': qtde_trades_lp20_positivos,
                             'L/P Count 20d negativos': qtde_trades_lp20_negativos,
                             'L/P Total 50d': soma_lp50,
                             'L/P Count 50d': qtde_trades_lp50_total,
                             'L/P Count 50d positivos': qtde_trades_lp50_positivos,
                             'L/P Count 50d negativos': qtde_trades_lp50_negativos,
                             'L/P Total 100d': soma_lp100,
                             'L/P Count 100d': qtde_trades_lp100_total,
                             'L/P Count 100d positivos': qtde_trades_lp100_positivos,
                             'L/P Count 100d negativos': qtde_trades_lp100_negativos,                                                          
                        })    
    
    logging.info(f"Dados de {os.path.basename(path_arquivo)} finalizados.")    


def processar_ODS_Estrategia002_Main():

    criar_pasta(path_Data_ODS_Acoes_Estrategia002_Diario)

    arquivos_Diario = [(os.path.join(path_Data_STG_Acoes_Diario, f)) 
                for f in os.listdir(path_Data_STG_Acoes_Diario) if os.path.isfile(os.path.join(path_Data_STG_Acoes_Diario, f))]

    threads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(processar_ODS_Estrategia002, arquivo) for arquivo in arquivos_Diario]

        # Aguardar todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Captura exceções das threads, se houver
            except Exception as e:
                logging.error(f"Erro ao baixar dados da ação: {e}")

    df_acoes_consolidada = pd.DataFrame(dados_acoes_estrategia002)
    
    criar_pasta(path_Data_DM)

    #Gerar arquivo consolidado
    aux.write_DataFrame_csv(df_acoes_consolidada, path_Data_DM_Acoes_Estrategia002_Diario_Consolidado)   

    logging.info(f"Processamento finalizados.")    
#********************************************************************************************************
# ESTRATEGIA 002 - FIM
#********************************************************************************************************


### Chamadas
def baixar_Intraday():
    criar_pasta(path_Data_STG_Acoes_Intraday)

    threads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(baixar_dados_acao, acao, path_Data_STG_Acoes_Intraday, '60m') for acao in acoes]

        # Aguardar todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Captura exceções das threads, se houver
            except Exception as e:
                logging.error(f"Erro ao baixar dados da ação: {e}")


def baixar_Diario():
    criar_pasta(path_Data_STG_Acoes_Diario)

    threads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(baixar_dados_acao, acao, path_Data_STG_Acoes_Diario, '1d') for acao in acoes]

        # Aguardar todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Captura exceções das threads, se houver
            except Exception as e:
                logging.error(f"Erro ao baixar dados da ação: {e}")
    

    
def baixar_Tudo():        
    
    #Montar codigos
    df_codnegs = aux.read_Dataframe_csv(path_Data_STG_Controle)

    for i in df_codnegs['CODNEG'].values:
        acoes.append(i)

    baixar_Intraday()
    
    baixar_Diario()

    logging.info('Download de todas as ações completo.')    

