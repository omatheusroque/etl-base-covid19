import gspread
from google.oauth2 import service_account
import pandas as pd
import datetime as datetime
import requests

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

json_file = "credentials.json"

def login():
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    return gc

def executar():

    # Importando CSV da base no SEADE
    df_drs = pd.read_csv('https://raw.githubusercontent.com/seade-R/dados-covid-sp/master/data/plano_sp_leitos_internacoes_serie_nova.csv', sep=';')

    # Gerando um dataframe e filtrando apenas a DRS 03 Araraquara
    drs3 = df_drs[df_drs.nome_drs == "DRS 03 Araraquara"]

    # Gerando dataframe para exportação apenas dos últimos dados para reduzir o tamanho
    drs3 = drs3.tail(30)
    drs_sp = df_drs.tail(18)  
    # Verificando se o relatório já foi atualiazado
    data_relatorio_SEADE = drs_sp['datahora'].iloc[1]
    data_hoje = datetime.datetime.today()
    data_hoje = (f"{data_hoje.strftime('%Y-%m-%d')}")

    # Realizando login na API
    gc = login()

    token = "token" #incluir o token
    chat_id = "chat_id" #incluir o chat_id

    if data_relatorio_SEADE == data_hoje:
        validar_drs_sp = gc.open('dados_drs_sp')
        validar_drs_sp = validar_drs_sp.worksheet("dados_drs_sp")
        validar_drs_sp = validar_drs_sp.get_all_records()
        validar_drs_sp = pd.DataFrame(validar_drs_sp)
        data_relatorio_sheets = validar_drs_sp['datahora'].iloc[-1]

        if data_relatorio_sheets == data_relatorio_SEADE:
            quit()
        else:
            # Substituindo a vírgula pelo ponto no datagrame drs3
            i = 0
            tamanho = len(drs3)

            for i in range(tamanho):
                drs3.loc[:, ('pacientes_uti_mm7d')].iloc[i] = float(drs3.loc[:, ('pacientes_uti_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('total_covid_uti_mm7d')].iloc[i] = float(drs3.loc[:, ('total_covid_uti_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('ocupacao_leitos')].iloc[i] = float(drs3.loc[:, ('ocupacao_leitos')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('leitos_pc')].iloc[i] = float(drs3.loc[:, ('leitos_pc')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('internacoes_28v28')].iloc[i] = float(drs3.loc[:, ('internacoes_28v28')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i] = float(drs3.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('pacientes_enf_mm7d')].iloc[i] = float(drs3.loc[:, ('pacientes_enf_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('total_covid_enf_mm7d')].iloc[i] = float(drs3.loc[:, ('total_covid_enf_mm7d')].iloc[i].replace(',', '.'))
                i = i + 1

            # Substituindo a vírgula pelo ponto no datagrame drs_sp
            i = 0
            tamanho = len(drs_sp)

            for i in range(tamanho):
                drs_sp.loc[:, ('pacientes_uti_mm7d')].iloc[i] = float(drs_sp.loc[:, ('pacientes_uti_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('total_covid_uti_mm7d')].iloc[i] = float(drs_sp.loc[:, ('total_covid_uti_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('ocupacao_leitos')].iloc[i] = float(drs_sp.loc[:, ('ocupacao_leitos')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('leitos_pc')].iloc[i] = float(drs_sp.loc[:, ('leitos_pc')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('internacoes_28v28')].iloc[i] = float(drs_sp.loc[:, ('internacoes_28v28')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i] = float(drs_sp.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('pacientes_enf_mm7d')].iloc[i] = float(drs_sp.loc[:, ('pacientes_enf_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('total_covid_enf_mm7d')].iloc[i] = float(drs_sp.loc[:, ('total_covid_enf_mm7d')].iloc[i].replace(',', '.'))
                i = i + 1

            # Importando as planilhas Google
            dados_drs_sp = gc.open('dados_drs_sp')
            dados_drs3 = gc.open('dados_drs3')
            dados_drs_sp = dados_drs_sp.worksheet("dados_drs_sp")
            dados_drs3 = dados_drs3.worksheet("dados_drs3")

            # Limpando a planilha dados_drs_sp
            dados_drs_sp.delete_rows(2, 19)
            dados_drs_sp.add_rows(1)

            # Importando os dados atualizados para a planilha dados_drs_sp
            i = 0
            for i in range(len(drs_sp)):
                lista_drs_sp = drs_sp.iloc[i].tolist()
                str_lista_drs_sp = [str(_) for _ in lista_drs_sp]
                dados_drs_sp.append_row(str_lista_drs_sp, value_input_option='USER_ENTERED')
                i = i + 1

            # Limpando a planilha dados_drs3
            dados_drs3.delete_rows(2, 31)
            dados_drs3.add_rows(1)

            # Importando os dados atualizados para a planilha dados_drs3
            i = 0
            for i in range(len(drs3)):
                lista_drs3 = drs3.iloc[i].tolist()
                str_lista_drs3 = [str(_) for _ in lista_drs3]
                dados_drs3.append_row(str_lista_drs3, value_input_option='USER_ENTERED')
                i = i + 1

            # Enviando mensagem de notificação para o Telegram
            token = token
            chat_id = chat_id
            mensagem = ("Database Covid: os dados do SEADE foram importados para a sua base de dados.")
            URL = "https://api.telegram.org/bot"+token+"/sendMessage?chat_id="+chat_id+"&text="+mensagem
            resposta = requests.get(URL)

    else:
        validar_drs_sp = gc.open('dados_drs_sp')
        validar_drs_sp = validar_drs_sp.worksheet("dados_drs_sp")
        validar_drs_sp = validar_drs_sp.get_all_records()
        validar_drs_sp = pd.DataFrame(validar_drs_sp)
        data_relatorio_sheets = validar_drs_sp['datahora'].iloc[-1]

        if data_relatorio_sheets == data_relatorio_SEADE:
            quit()

        else:
            # Substituindo a vírgula pelo ponto no datagrame drs3
            i = 0
            tamanho = len(drs3)

            for i in range(tamanho):
                drs3.loc[:, ('pacientes_uti_mm7d')].iloc[i] = float(drs3.loc[:, ('pacientes_uti_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('total_covid_uti_mm7d')].iloc[i] = float(drs3.loc[:, ('total_covid_uti_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('ocupacao_leitos')].iloc[i] = float(drs3.loc[:, ('ocupacao_leitos')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('leitos_pc')].iloc[i] = float(drs3.loc[:, ('leitos_pc')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('internacoes_28v28')].iloc[i] = float(drs3.loc[:, ('internacoes_28v28')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i] = float(drs3.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('pacientes_enf_mm7d')].iloc[i] = float(drs3.loc[:, ('pacientes_enf_mm7d')].iloc[i].replace(',', '.'))
                drs3.loc[:, ('total_covid_enf_mm7d')].iloc[i] = float(drs3.loc[:, ('total_covid_enf_mm7d')].iloc[i].replace(',', '.'))
                i = i + 1
            # Substituindo a vírgula pelo ponto no datagrame drs_sp
            i = 0
            tamanho = len(drs_sp)

            for i in range(tamanho):
                drs_sp.loc[:, ('pacientes_uti_mm7d')].iloc[i] = float(drs_sp.loc[:, ('pacientes_uti_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('total_covid_uti_mm7d')].iloc[i] = float(drs_sp.loc[:, ('total_covid_uti_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('ocupacao_leitos')].iloc[i] = float(drs_sp.loc[:, ('ocupacao_leitos')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('leitos_pc')].iloc[i] = float(drs_sp.loc[:, ('leitos_pc')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('internacoes_28v28')].iloc[i] = float(drs_sp.loc[:, ('internacoes_28v28')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i] = float(drs_sp.loc[:, ('ocupacao_leitos_ultimo_dia')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('pacientes_enf_mm7d')].iloc[i] = float(drs_sp.loc[:, ('pacientes_enf_mm7d')].iloc[i].replace(',', '.'))
                drs_sp.loc[:, ('total_covid_enf_mm7d')].iloc[i] = float(drs_sp.loc[:, ('total_covid_enf_mm7d')].iloc[i].replace(',', '.'))
                i = i + 1

            # Importando as planilhas Google
            dados_drs_sp = gc.open('dados_drs_sp')
            dados_drs3 = gc.open('dados_drs3')
            dados_drs_sp = dados_drs_sp.worksheet("dados_drs_sp")
            dados_drs3 = dados_drs3.worksheet("dados_drs3")

            # Limpando a planilha dados_drs_sp
            dados_drs_sp.delete_rows(2, 19)
            dados_drs_sp.add_rows(1)

            # Importando os dados atualizados para a planilha dados_drs_sp
            i = 0
            for i in range(len(drs_sp)):
                lista_drs_sp = drs_sp.iloc[i].tolist()
                str_lista_drs_sp = [str(_) for _ in lista_drs_sp]
                dados_drs_sp.append_row(str_lista_drs_sp, value_input_option='USER_ENTERED')
                i = i + 1

            # Limpando a planilha dados_drs3
            dados_drs3.delete_rows(2, 31)
            dados_drs3.add_rows(1)

            # Importando os dados atualizados para a planilha dados_drs3
            i = 0
            for i in range(len(drs3)):
                lista_drs3 = drs3.iloc[i].tolist()
                str_lista_drs3 = [str(_) for _ in lista_drs3]
                dados_drs3.append_row(str_lista_drs3, value_input_option='USER_ENTERED')
                i = i + 1

            # Enviando mensagem de notificação para o Telegram
            token = token
            chat_id = chat_id
            mensagem = ("Database Covid: os dados do SEADE foram importados para a sua base de dados.")
            URL = "https://api.telegram.org/bot"+token+"/sendMessage?chat_id="+chat_id+"&text="+mensagem
            resposta = requests.get(URL)

    return

executar()
