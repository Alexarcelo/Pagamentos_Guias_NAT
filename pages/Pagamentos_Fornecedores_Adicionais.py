import streamlit as st
import pandas as pd
import mysql.connector
import decimal
from babel.numbers import format_currency
import gspread 
import requests
from datetime import time, timedelta
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials

def gerar_df_phoenix(vw_name, base_luck):

    # Parametros de Login AWS
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def puxar_dados_phoenix():

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_fornecedores', 'test_phoenix_natal')

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores'

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
    st.session_state.df_cnpj_fornecedores = st.session_state.df_escalas_bruto[~pd.isna(st.session_state.df_escalas_bruto['Fornecedor Motorista'])]\
        [['Fornecedor Motorista', 'CNPJ/CPF Fornecedor Motorista', 'Razao Social/Nome Completo Fornecedor Motorista']].drop_duplicates().reset_index(drop=True)

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def tratar_colunas_df_tarifario():

    for coluna in ['Valor ADT', 'Valor CHD']:

        st.session_state.df_tarifario[coluna] = (st.session_state.df_tarifario[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

        st.session_state.df_tarifario[coluna] = pd.to_numeric(st.session_state.df_tarifario[coluna])

def puxar_tarifario_fornecedores():

    puxar_aba_simples('1tsaBFwE3KS84r_I5-g3YGP7tTROe1lyuCw_UjtxofYI', 'Tarifário Fornecedores (Adicional)', 'df_tarifario')

    tratar_colunas_df_tarifario()

def verificar_tarifarios(df_escalas_group, id_gsheet):

    lista_passeios = df_escalas_group['Servico'].unique().tolist()

    lista_passeios_tarifario = st.session_state.df_tarifario['Servico'].unique().tolist()

    lista_passeios_sem_tarifario = [item for item in lista_passeios if not item in lista_passeios_tarifario]

    if len(lista_passeios_sem_tarifario)>0:

        df_itens_faltantes = pd.DataFrame(lista_passeios_sem_tarifario, columns=['Serviços'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        project_id = "grupoluck"
        secret_id = "cred-luck-aracaju"
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        secret_payload = response.payload.data.decode("UTF-8")
        credentials_info = json.loads(secret_payload)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Tarifário Fornecedores (Adicional)')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os serviços acima não estão tarifados. Eles foram inseridos no final da planilha de tarifários. Por favor, tarife os serviços e tente novamente')

        st.stop()

def gerar_escalas_agrupadas(data_inicial, data_final):

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                             (~pd.isna(st.session_state.df_escalas['adicional'])) & 
                                             (~st.session_state.df_escalas['adicional'].isin(['', 'Água Mineral (Luck Natal)', 'Cadeirinha de bebê  (Luck Natal)', 
                                                                                               'Deslocamento de Hoteis Distante (Luck Natal)'])) & 
                                             (st.session_state.df_escalas['adicional'].str.upper().str.contains('LANCHA|BARCO|JARDINEIRA'))].reset_index(drop=True)

    df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'adicional': transformar_em_string, 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    df_escalas_group = df_escalas_group[~df_escalas_group['adicional'].isin(['', 'Água Mineral (Luck Natal)', 'Cadeirinha de bebê  (Luck Natal)', 'Deslocamento de Hoteis Distante (Luck Natal)'])]\
        .reset_index(drop=True)
    
    return df_escalas_group

def calcular_valor_final(df_escalas_group):

    df_pag_fornecedores = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    df_pag_fornecedores['Valor Final'] = (df_pag_fornecedores['Total ADT'] * df_pag_fornecedores['Valor ADT']) + (df_pag_fornecedores['Total CHD'] * df_pag_fornecedores['Valor CHD'])

    df_pag_fornecedores['Servico'] = df_pag_fornecedores['Servico'].replace({'Passeio à Perobas - Touros ': 'Passeio à Perobas', 'Passeio à Maracajaú - Touros': 'Passeio à Maracajaú'})

    return df_pag_fornecedores

def definir_html(df_ref):

    html=df_ref.to_html(index=False, escape=False)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                text-align: center;  /* Centraliza o texto */
            }}
            table {{
                margin: 0 auto;  /* Centraliza a tabela */
                border-collapse: collapse;  /* Remove espaço entre as bordas da tabela */
            }}
            th, td {{
                padding: 8px;  /* Adiciona espaço ao redor do texto nas células */
                border: 1px solid black;  /* Adiciona bordas às células */
                text-align: center;
            }}
        </style>
    </head>
    <body>
        {html}
    </body>
    </html>
    """

    return html

def criar_output_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{guia}</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def verificar_fornecedor_sem_telefone(id_gsheet, guia, lista_guias_com_telefone):

    if not guia in lista_guias_com_telefone:

        lista_guias = []

        lista_guias.append(guia)

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Guias'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        project_id = "grupoluck"
        secret_id = "cred-luck-aracaju"
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        secret_payload = response.payload.data.decode("UTF-8")
        credentials_info = json.loads(secret_payload)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Telefones Fornecedores')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error(f'O fornecedor {guia} não tem número de telefone cadastrado na planilha. Ele foi inserido no final da lista de fornecedores. Por favor, cadastre o telefone dele e tente novamente')

        st.stop()

    else:

        telefone_guia = st.session_state.df_telefones.loc[st.session_state.df_telefones['Fornecedores']==guia, 'Telefone'].values[0]

    return telefone_guia

st.set_page_config(layout='wide')

if not 'mapa_forn_add_gerado' in st.session_state:

    st.session_state.mapa_forn_add_gerado = 0

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1tsaBFwE3KS84r_I5-g3YGP7tTROe1lyuCw_UjtxofYI'

if not 'id_webhook' in st.session_state:

    st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucknatal"

if not 'df_escalas' in st.session_state or st.session_state.view_phoenix != 'vw_pagamento_fornecedores':

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title('Mapa de Pagamento - Fornecedores (Adicional)')

st.divider()

row1 = st.columns(2)

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_final')

    gerar_mapa = container_datas.button('Gerar Mapa de Pagamentos')

with row1[1]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    container_data_pgto = st.container(border=True)

    container_data_pgto.subheader('Data de Pagamento')

    data_pagamento = container_data_pgto.date_input('Data de Pagamento', value=None ,format='DD/MM/YYYY', key='data_pagamento')

if atualizar_phoenix:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

if gerar_mapa:

    with st.spinner('Puxando tarifários...'):

        puxar_tarifario_fornecedores()

    # Gerando escalas agrupadas

    df_escalas_group = gerar_escalas_agrupadas(data_inicial, data_final)

    # Verificar se ta tudo tarifado

    verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet)

    # Precificar serviços e calcular valor total

    df_pag_fornecedores = calcular_valor_final(df_escalas_group)

    st.session_state.df_pag_final = df_pag_fornecedores[['Data da Escala', 'Escala', 'Servico', 'Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD', 'Valor Final']]

    st.session_state.mapa_forn_add_gerado = 1

if st.session_state.mapa_forn_add_gerado == 1:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_servicos = st.session_state.df_pag_final['Servico'].dropna().unique().tolist()

        servico = st.multiselect('Serviço', sorted(lista_servicos), default=None)

    if servico and data_pagamento and data_inicial and data_final:

        row2_1 = st.columns(4)

        df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Servico'].isin(servico)].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        container_dataframe = st.container()

        container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

        with row2_1[0]:

            total_a_pagar = df_pag_guia['Valor Final'].sum()

            st.subheader(f'Valor Total: R${int(total_a_pagar)}')

        soma_servicos = df_pag_guia['Valor Final'].sum()

        soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

        for item in ['Valor Final', 'Valor ADT', 'Valor CHD']:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        for item in ['Total ADT', 'Total CHD']:

            df_pag_guia[item] = df_pag_guia[item].astype(int)

        html = definir_html(df_pag_guia)

        nome_html = f"{', '.join(servico)}.html"

        criar_output_html(nome_html, html, nome_html, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content = file.read()

        with row2_1[1]:

            st.download_button(
                label="Baixar Arquivo HTML",
                data=html_content,
                file_name=nome_html,
                mime="text/html"
            )

        st.session_state.html_content = html_content

    else:

        row2_1 = st.columns(4)

        with row2_1[0]:

            enviar_informes = st.button(f'Enviar Informes Gerais')

            if enviar_informes:

                puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Fornecedores', 'df_telefones')

                lista_htmls = []

                lista_telefones = []

                for servico_ref in lista_servicos:

                    telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, servico_ref, st.session_state.df_telefones['Fornecedores'].unique().tolist())

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Servico']==servico_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Final'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Final', 'Valor ADT', 'Valor CHD']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    for item in ['Total ADT', 'Total CHD']:

                        df_pag_guia[item] = df_pag_guia[item].astype(int)

                    html = definir_html(df_pag_guia)

                    nome_html = f'{servico_ref}.html'

                    criar_output_html(nome_html, html, servico_ref, soma_servicos)

                    with open(nome_html, "r", encoding="utf-8") as file:

                        html_content_fornecedor_ref = file.read()

                    lista_htmls.append([html_content_fornecedor_ref, telefone_guia])

                payload = {"informe_html": lista_htmls}
                
                response = requests.post(st.session_state.id_webhook, json=payload)
                    
                if response.status_code == 200:
                    
                    st.success(f"Mapas de Pagamentos enviados com sucesso!")
                    
                else:
                    
                    st.error(f"Erro. Favor contactar o suporte")

                    st.error(f"{response}")

        with row2_1[1]:

            enviar_informes_financeiro = st.button('Enviar Informes p/ Financeiro')

            if enviar_informes_financeiro:

                lista_htmls = []

                lista_telefones = []

                for servico_ref in lista_servicos:

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Servico']==servico_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Final'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Final', 'Valor ADT', 'Valor CHD']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    for item in ['Total ADT', 'Total CHD']:

                        df_pag_guia[item] = df_pag_guia[item].astype(int)

                    html = definir_html(df_pag_guia)

                    nome_html = f'{servico_ref}.html'

                    criar_output_html(nome_html, html, servico_ref, soma_servicos)

                    with open(nome_html, "r", encoding="utf-8") as file:

                        html_content_fornecedor_ref = file.read()

                    lista_htmls.append([html_content_fornecedor_ref, '84994001644'])

                payload = {"informe_html": lista_htmls}
                
                response = requests.post(st.session_state.id_webhook, json=payload)
                    
                if response.status_code == 200:
                    
                    st.success(f"Mapas de Pagamentos enviados com sucesso!")
                    
                else:
                    
                    st.error(f"Erro. Favor contactar o suporte")

                    st.error(f"{response}")

if 'html_content' in st.session_state and len(servico)==1:

    with row2_1[2]:

        enviar_informes = st.button(f"Enviar Informes | {', '.join(servico)}")

    if enviar_informes:

        puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Fornecedores', 'df_telefones')

        telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, servico[0], st.session_state.df_telefones['Fornecedores'].unique().tolist())
        
        payload = {"informe_html": st.session_state.html_content, 
                    "telefone": telefone_guia}
        
        response = requests.post(st.session_state.id_webhook, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
