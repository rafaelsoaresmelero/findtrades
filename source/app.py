import streamlit as st  # pip install streamlit
import time
import locale
from streamlit_option_menu import option_menu
from views import home, dados, estrategia001
   
def pageConfig():
    # Set the locale to the user's default locale for proper thousand separators
    locale.setlocale(locale.LC_ALL, '')

    #Configurações Gerais da Página
    st.set_page_config(page_title="Trades Estratégias", page_icon=":horse:", layout="wide", initial_sidebar_state="expanded")

    # ---- HIDE STREAMLIT STYLE ----
    #hide_st_style = """
    #            <style>
    #            #MainMenu {visibility: hidden;}
    #            footer {visibility: hidden;}
    #            header {visibility: hidden;}            
    #            </style>
    #            """
    #st.markdown(hide_st_style, unsafe_allow_html=True)

def buildSidebar():
    ### MENU PRINCIPAL
    with st.sidebar:
        selected = option_menu(menu_title="DayTrade", options=['Home', 'Dados', 'Estratégia 001'], icons=['house', 'gear'], menu_icon='menu-down', orientation='vertical')

    if selected=='Home':
        home.createPage()        
    elif selected=='Dados':
        dados.createPage()
    elif selected=="Estratégia 001":
        estrategia001.createPage()
    #elif selected=="Benchmark":
    #    benchmark.createPage()
    #elif selected=="Contas":
    #    contas.createPage()
    #elif selected=='Trade Historico':
    #    tradehistorico.createPage()

pageConfig()
buildSidebar()    
