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

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
    st.session_state.df_cnpj_fornecedores = st.session_state.df_escalas_bruto[~pd.isna(st.session_state.df_escalas_bruto['Fornecedor Motorista'])]\
        [['Fornecedor Motorista', 'CNPJ/CPF Fornecedor Motorista', 'Razao Social/Nome Completo Fornecedor Motorista']].drop_duplicates().reset_index(drop=True)

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def tratar_colunas_df_tarifario():

    for coluna in ['Bus', 'Micro', 'Van Alongada', 'Van', 'Utilitario', 'Conjugado Bus', 'Conjugado Micro', 'Conjugado Van Alongada', 'Conjugado Van', 'Conjugado Utilitario', 
                   'Bus Terra Dourada', 'Micro Terra Dourada', 'Van Alongada Terra Dourada', 'Van Terra Dourada', 'Conjugado Bus Terra Dourada', 'Conjugado Micro Terra Dourada', 
                   'Conjugado Van Alongada Terra Dourada', 'Conjugado Van Terra Dourada']:

        st.session_state.df_tarifario[coluna] = (st.session_state.df_tarifario[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

        st.session_state.df_tarifario[coluna] = pd.to_numeric(st.session_state.df_tarifario[coluna])

def puxar_tarifario_fornecedores():

    puxar_aba_simples('1tsaBFwE3KS84r_I5-g3YGP7tTROe1lyuCw_UjtxofYI', 'Tarifário Fornecedores', 'df_tarifario')

    tratar_colunas_df_tarifario()

def inserir_config(df_itens_faltantes, id_gsheet, nome_aba):

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z1000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

def tratar_tipos_veiculos(df_escalas):

    dict_tp_veic = {'Ônibus': 'Bus', 'Sedan': 'Utilitario', '4X4': 'Utilitario', 'Executivo': 'Utilitario', 'Micrão': 'Micro', 'Executivo Blindado': 'Utilitario', 'Monovolume': 'Utilitario'}

    df_escalas['Tipo Veiculo'] = df_escalas['Tipo Veiculo'].replace(dict_tp_veic)

    return df_escalas

def tratar_servicos_in_out(df_escalas):

    dict_tp_veic = {'In Natal - Hotéis Parceiros ': 'IN - Natal ', 'IN Touros - Hotéis Parceiros': 'IN - Touros', 'IN Pipa - Hotéis Parceiros ': 'IN - Pipa', 
                    'OUT Natal - Hotéis Parceiros ': 'OUT - Natal', 'OUT Pipa - Hotéis Parceiros': 'OUT - Pipa', 'OUT Touros - hotéis Parceiros': 'OUT - Touros'}

    df_escalas['Servico'] = df_escalas['Servico'].replace(dict_tp_veic)

    return df_escalas

def verificar_tarifarios(df_escalas_group, id_gsheet):

    lista_passeios = df_escalas_group['Servico'].unique().tolist()

    lista_passeios_tarifario = st.session_state.df_tarifario['Servico'].unique().tolist()

    lista_passeios_sem_tarifario = [item for item in lista_passeios if not item in lista_passeios_tarifario]

    if len(lista_passeios_sem_tarifario)>0:

        df_itens_faltantes = pd.DataFrame(lista_passeios_sem_tarifario, columns=['Serviços'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        # GCP projeto onde está a chave credencial
        project_id = "grupoluck"
    
        # ID da chave credencial do google.
        secret_id = "cred-luck-aracaju"
    
        # Cria o cliente.
        secret_client = secretmanager.SecretManagerServiceClient()
    
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
    
        secret_payload = response.payload.data.decode("UTF-8")
    
        credentials_info = json.loads(secret_payload)
    
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
        # Use the credentials to authorize the gspread client
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Tarifário Fornecedores')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os serviços acima não estão tarifados. Eles foram inseridos no final da planilha de tarifários. Por favor, tarife os serviços e tente novamente')

        st.stop()

def map_regiao(servico):

    for key, value in st.session_state.dict_conjugados.items():

        if key in servico: 

            return value
        
    return None 

def identificar_trf_conjugados(df_escalas_pag):

    st.session_state.dict_conjugados = {'OUT - Pipa': 'Pipa', 'IN - Pipa': 'Pipa', 'OUT - Touros': 'Touros', 'IN - Touros': 'Touros', 'OUT - Natal': 'Natal', 'IN - Natal ': 'Natal', 'OUT - Tripulacao': 'Tripulacao', 
                    'IN - Tripulacao': 'Tripulacao', 'OUT - São Miguel Gostoso': 'Sao Miguel', 'IN - São Miguel Gostoso': 'Sao Miguel'}

    df_escalas_pag['Regiao'] = df_escalas_pag['Servico'].apply(map_regiao)

    df_escalas_pag['Servico Conjugado'] = ''

    df_in_out = df_escalas_pag[df_escalas_pag['Tipo de Servico'].isin(['IN', 'OUT'])].reset_index()

    for veiculo in df_in_out['Veiculo'].unique():

        df_veiculo = df_in_out[(df_in_out['Veiculo']==veiculo)].reset_index(drop=True)

        for data_ref in df_veiculo['Data da Escala'].unique():

            df_data = df_veiculo[df_veiculo['Data da Escala']==data_ref].reset_index(drop=True)

            if len(df_data)>1 and len(df_data['Tipo de Servico'].unique())>1 and df_data['Regiao'].duplicated().any():

                df_ref = df_data.sort_values(by=['Regiao', 'Data | Horario Apresentacao']).reset_index(drop=True)

                for index in range(1, len(df_ref), 2):

                    regiao = df_ref.at[index, 'Regiao']

                    primeiro_trf = df_ref.at[index-1, 'Tipo de Servico']

                    segundo_trf = df_ref.at[index, 'Tipo de Servico']

                    hora_voo_out_nat = pd.to_datetime(df_ref.at[index-1, 'Data | Horario Apresentacao'])

                    data_hora_voo_out_nat = hora_voo_out_nat.date()

                    hora_voo_in_nat = pd.to_datetime(df_ref.at[index, 'Horario Voo'], format='%H:%M:%S').replace(year=data_hora_voo_out_nat.year, month=data_hora_voo_out_nat.month, 
                                                                                                                day=data_hora_voo_out_nat.day)
                    
                    hora_voo_out_outros = pd.to_datetime(df_ref.at[index, 'Data | Horario Apresentacao'])

                    data_hora_voo_out_outros = hora_voo_out_outros.date()

                    hora_voo_in_outros = pd.to_datetime(df_ref.at[index-1, 'Horario Voo'], format='%H:%M:%S').replace(year=data_hora_voo_out_outros.year, month=data_hora_voo_out_outros.month, 
                                                                                                                      day=data_hora_voo_out_outros.day)
                    
                    hora_voo_out_outros_2 = pd.to_datetime(df_ref.at[index-1, 'Data | Horario Apresentacao'])

                    data_hora_voo_out_outros_2 = hora_voo_out_outros_2.date()

                    hora_voo_in_outros_2 = pd.to_datetime(df_ref.at[index, 'Horario Voo'], format='%H:%M:%S').replace(year=data_hora_voo_out_outros_2.year, month=data_hora_voo_out_outros_2.month, 
                                                                                                                    day=data_hora_voo_out_outros_2.day)

                    index_1 = df_ref.at[index-1, 'index']

                    index_2 = df_ref.at[index, 'index']

                    if regiao=='Natal' and ((primeiro_trf=='OUT') and (segundo_trf=='IN')) and (hora_voo_in_nat - hora_voo_out_nat <= timedelta(hours=2, minutes=30)):

                        df_escalas_pag.at[index_1, 'Servico Conjugado'] = 'X'

                        df_escalas_pag.at[index_2, 'Servico Conjugado'] = 'X'

                    elif regiao!='Natal' and ((primeiro_trf=='IN') and (segundo_trf=='OUT')) and (hora_voo_out_outros - hora_voo_in_outros <= timedelta(hours=4)):

                        df_escalas_pag.at[index_1, 'Servico Conjugado'] = 'X'

                        df_escalas_pag.at[index_2, 'Servico Conjugado'] = 'X'

                    elif regiao!='Natal' and ((primeiro_trf=='OUT') and (segundo_trf=='IN')) and (hora_voo_in_outros_2 - hora_voo_out_outros_2 <= timedelta(hours=3, minutes=40)):

                        df_escalas_pag.at[index_1, 'Servico Conjugado'] = 'X'

                        df_escalas_pag.at[index_2, 'Servico Conjugado'] = 'X'

    return df_escalas_pag

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

        file.write(f'<p style="font-size:30px;">CPF / CNPJ: {st.session_state.cnpj}</p>')

        file.write(f'<p style="font-size:30px;">Razão Social / Nome Completo: {st.session_state.razao_social}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def criar_colunas_escala_veiculo_mot_guia(df_apoios):

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace('Escala Auxiliar: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Veículo: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Motorista: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Guia: ', '', regex=False)

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = \
        df_apoios['Apoio'].str.split(',', expand=True)
    
    return df_apoios

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def adicionar_apoios_em_dataframe(df_escalas_group):

    df_escalas_com_apoio = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                       (~pd.isna(st.session_state.df_escalas['Apoio']))].reset_index(drop=True)
    
    df_escalas_com_apoio = df_escalas_com_apoio.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Motorista', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista'])\
        .agg({'Apoio': transformar_em_string, 'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min'}).reset_index()
    
    df_escalas_com_1_apoio = df_escalas_com_apoio[(df_escalas_com_apoio['Apoio']!='') & (~df_escalas_com_apoio['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_escalas_com_1_apoio = criar_colunas_escala_veiculo_mot_guia(df_escalas_com_1_apoio)

    df_escalas_com_1_apoio = df_escalas_com_1_apoio[~(df_escalas_com_1_apoio['Veiculo Apoio'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))]

    df_apoios_group = df_escalas_com_1_apoio.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio', 'Servico'])\
        .agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'first'}).reset_index()

    df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

    df_apoios_group = df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao']]

    df_apoios_group = df_apoios_group[(~df_apoios_group['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))].reset_index(drop=True)

    df_veiculo_tp_veiculo = st.session_state.df_escalas[(st.session_state.df_escalas['Veiculo'].isin(df_apoios_group['Veiculo'].unique())) & 
                                                        (~st.session_state.df_escalas['Fornecedor Motorista'].dropna().str.upper().str.contains('DUPLICIDADE'))]\
                                                            [['Veiculo', 'Tipo Veiculo', 'Fornecedor Motorista']].drop_duplicates()

    df_apoios_group = pd.merge(df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Data | Horario Apresentacao']], df_veiculo_tp_veiculo, on='Veiculo', how='left')

    df_apoios_group[['Servico', 'Tipo de Servico', 'Horario Voo']] = ['APOIO', 'TRANSFER', None]

    df_escalas_pag = pd.concat([df_escalas_group, df_apoios_group], ignore_index=True)

    df_escalas_com_2_apoios = df_escalas_com_apoio[(df_escalas_com_apoio['Apoio']!='') & (df_escalas_com_apoio['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_novo = pd.DataFrame(columns=['Escala', 'Veiculo', 'Data | Horario Apresentacao', 'Data da Escala'])

    for index in range(len(df_escalas_com_2_apoios)):

        data_escala = df_escalas_com_2_apoios.at[index, 'Data da Escala']

        apoio_nome = df_escalas_com_2_apoios.at[index, 'Apoio']

        data_h_apr = df_escalas_com_2_apoios.at[index, 'Data | Horario Apresentacao']

        lista_apoios = apoio_nome.split(' | ')

        for item in lista_apoios:

            dict_replace = {'Escala Auxiliar: ': '', ' Veículo: ': '', ' Motorista: ': '', ' Guia: ': ''}

            for old, new in dict_replace.items():

                item = item.replace(old, new)
                
            lista_insercao = item.split(',')

            contador = len(df_novo)

            df_novo.at[contador, 'Escala'] = lista_insercao[0]

            df_novo.at[contador, 'Veiculo'] = lista_insercao[1]

            df_novo.at[contador, 'Data | Horario Apresentacao'] = data_h_apr

            df_novo.at[contador, 'Data da Escala'] = data_escala

    df_novo = df_novo[(~df_novo['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))].reset_index(drop=True)

    df_veiculo_tp_veiculo = st.session_state.df_escalas[(st.session_state.df_escalas['Veiculo'].isin(df_novo['Veiculo'].unique())) & 
                                                        (~st.session_state.df_escalas['Fornecedor Motorista'].dropna().str.upper().str.contains('DUPLICIDADE'))]\
                                                            [['Veiculo', 'Tipo Veiculo', 'Fornecedor Motorista']].drop_duplicates()

    df_novo = pd.merge(df_novo[['Data da Escala', 'Escala', 'Veiculo', 'Data | Horario Apresentacao']], df_veiculo_tp_veiculo, on='Veiculo', how='left')

    df_novo[['Servico', 'Tipo de Servico', 'Horario Voo']] = ['APOIO', 'TRANSFER', None]

    df_escalas_pag = pd.concat([df_escalas_pag, df_novo], ignore_index=True)

    df_escalas_pag =  tratar_tipos_veiculos(df_escalas_pag)

    return df_escalas_pag

def precificar_apoios_2_em_1(df_escalas_pag):
    
    df_apoios = df_escalas_pag[df_escalas_pag['Servico']=='APOIO'].reset_index()

    df_apoios_group = df_apoios.groupby(['Data da Escala', 'Veiculo'])['Escala'].count().reset_index()

    df_apoios_2_em_1 = df_apoios_group[df_apoios_group['Escala']==2].reset_index()

    for index in range(len(df_apoios_2_em_1)):

        data_ref = df_apoios_2_em_1.at[index, 'Data da Escala']

        veiculo = df_apoios_2_em_1.at[index, 'Veiculo']

        df_ref = df_apoios[(df_apoios['Data da Escala']==data_ref) & (df_apoios['Veiculo']==veiculo)].reset_index(drop=True)

        escala_ref = df_ref['Escala'].iloc[0]

        df_escalas_pag.loc[df_escalas_pag['Escala']==escala_ref, 'Valor Final']=0

    df_apoios_3_ou_mais = df_apoios_group[df_apoios_group['Escala']>2].reset_index()

    for index in range(len(df_apoios_3_ou_mais)):

        data_ref = df_apoios_3_ou_mais.at[index, 'Data da Escala']

        veiculo = df_apoios_3_ou_mais.at[index, 'Veiculo']

        df_ref = df_apoios[(df_apoios['Data da Escala']==data_ref) & (df_apoios['Veiculo']==veiculo)].reset_index(drop=True)

        escalas_ref = df_ref.loc[2:, 'Escala']

        df_escalas_pag.loc[df_escalas_pag['Escala'].isin(escalas_ref), 'Valor Final'] = 0

    return df_escalas_pag

def ajustar_valor_luiz_damiao_pipa(df_escalas_pag):

    df_escalas_damiao_luiz = df_escalas_pag[(df_escalas_pag['Fornecedor Motorista'].isin(['DAMIAO PIPA', 'LUIZ ANTONIO'])) & (df_escalas_pag['Servico']=='APOIO')].reset_index()

    for escala in df_escalas_damiao_luiz['Escala'].unique():

        df_ref = st.session_state.df_escalas[(~pd.isna(st.session_state.df_escalas['Apoio'])) & (st.session_state.df_escalas['Apoio'].str.contains(escala))].reset_index()

        total_paxs = df_ref['Total ADT'].sum() + df_ref['Total CHD'].sum()

        df_escalas_pag.loc[df_escalas_pag['Escala']==escala, 'Valor Final'] = ((total_paxs + 3) // 4) * 20

    return df_escalas_pag

def verificar_fornecedor_sem_telefone(id_gsheet, guia, lista_guias_com_telefone):

    if not guia in lista_guias_com_telefone:

        lista_guias = []

        lista_guias.append(guia)

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Guias'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        # GCP projeto onde está a chave credencial
        project_id = "grupoluck"
    
        # ID da chave credencial do google.
        secret_id = "cred-luck-aracaju"
    
        # Cria o cliente.
        secret_client = secretmanager.SecretManagerServiceClient()
    
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
    
        secret_payload = response.payload.data.decode("UTF-8")
    
        credentials_info = json.loads(secret_payload)
    
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
        # Use the credentials to authorize the gspread client
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Telefones Guias')
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

def identificar_trf_htl_conjugados(df_escalas_pag):

    dict_trf_hotel_conjugado = {'TRF  Pipa/Natal': 1, 'TRF Natal/Pipa ': 2, 'TRF Natal/Touros': 3, 'TRF Touros/Natal': 4, 'TRF Natal/São Miguel': 5, 'TRF São Miguel/Natal': 6}

    lista_servicos = ['TRF  Pipa/Natal', 'TRF Natal/Pipa ', 'TRF Natal/Touros', 'TRF Touros/Natal', 'TRF Natal/São Miguel', 'TRF São Miguel/Natal']

    df_in_out = df_escalas_pag[df_escalas_pag['Servico'].isin(lista_servicos)].reset_index()

    df_in_out['Ajuste'] = df_in_out['Servico'].map(dict_trf_hotel_conjugado)

    df_in_out = df_in_out.sort_values(by=['Data da Escala', 'Fornecedor Motorista', 'Ajuste']).reset_index(drop=True)

    df_data_fornecedor = df_in_out[['Data da Escala', 'Fornecedor Motorista']].drop_duplicates().reset_index()

    for index in range(len(df_data_fornecedor)):

        df_ref = df_in_out[(df_in_out['Data da Escala']==df_data_fornecedor.at[index, 'Data da Escala']) & (df_in_out['Fornecedor Motorista']==df_data_fornecedor.at[index, 'Fornecedor Motorista'])]\
            .reset_index(drop=True)
        
        if len(df_ref)>=2:
        
            servico_1 = df_ref.at[0, 'Servico']

            servico_2 = df_ref.at[1, 'Servico']

            df_ref = df_ref.sort_values(by='Data | Horario Apresentacao').reset_index(drop=True)

            hora_1 = df_ref.at[0, 'Data | Horario Apresentacao']

            hora_2 = df_ref.at[1, 'Data | Horario Apresentacao']

            if (servico_1=='TRF  Pipa/Natal' and servico_2=='TRF Natal/Pipa ') or (servico_1=='TRF Natal/Touros' and servico_2=='TRF Touros/Natal'):

                if hora_2 - hora_1 <= timedelta(hours=3, minutes=30):

                    df_escalas_pag.loc[df_escalas_pag.index.isin(df_ref['index']), 'Servico Conjugado'] = 'X'

            elif servico_1=='TRF Natal/São Miguel' and servico_2=='TRF São Miguel/Natal':

                if hora_2 - hora_1 <= timedelta(hours=4):

                    df_escalas_pag.loc[df_escalas_pag.index.isin(df_ref['index']), 'Servico Conjugado'] = 'X'

    return df_escalas_pag

def verificar_tarifarios_terra_dourada(df_escalas_pag):
    
    df_escalas_terra_dourada = df_escalas_pag[df_escalas_pag['Fornecedor Motorista']=='TERRA DOURADA'].reset_index(drop=True)

    df_escalas_terra_dourada['Valor Final'] = pd.to_numeric(df_escalas_terra_dourada['Valor Final'])

    if df_escalas_terra_dourada['Valor Final'].isnull().any():

        nome_servicos_sem_tarifario_terra_dourada = ', '.join(df_escalas_terra_dourada[pd.isna(df_escalas_terra_dourada['Valor Final'])]['Servico'].unique().tolist())

        st.error(f'Os serviços {nome_servicos_sem_tarifario_terra_dourada} não estão tarifados para o Fornecedor Terra Dourada. Por favor, tarife-os e tente novamente')

        st.stop()

    else:

        st.success('Todos os serviços estão tarifados!')

def ajustar_apoios_bolero_pipa(df_escalas_pag): 

    df_apoios = df_escalas_pag[df_escalas_pag['Servico']=='APOIO'].reset_index()

    valores_escala = '|'.join(df_apoios['Escala'].astype(str).unique())

    df_apoios_2 = st.session_state.df_escalas[(st.session_state.df_escalas['Apoio'].str.upper().str.contains(valores_escala, regex=True, na=False)) & 
                                              (st.session_state.df_escalas['Servico']=='Passeio à João Pessoa com Bolero (PIPA)')][['Escala', 'Servico', 'Apoio']].drop_duplicates().reset_index(drop=True)
    
    for index in range(len(df_apoios_2)):

        apoio_str = df_apoios_2.at[index, 'Apoio']

        if ' | ' in apoio_str:

            lista_apoios = apoio_str.split(' | ')

            for item in lista_apoios:

                dict_replace = {'Escala Auxiliar: ': '', ' Veículo: ': '', ' Motorista: ': '', ' Guia: ': ''}

                for old, new in dict_replace.items():

                    item = item.replace(old, new)

                lista_insercao = item.split(',')

                escala_apoio = lista_insercao[0]

                df_escalas_pag.loc[df_escalas_pag['Escala']==escala_apoio, 'Utilitario'] = 150

        else:

            df_escalas_com_1_apoio = criar_colunas_escala_veiculo_mot_guia(df_escalas_com_1_apoio)

            escala_apoio = df_escalas_com_1_apoio['Escala Apoio'].iloc[0]

            df_escalas_pag.loc[df_escalas_pag['Escala']==escala_apoio, 'Utilitario'] = 150

    return df_escalas_pag

def ajustar_valor_litoral_sul_4x4(df_escalas_pag):

    df_4x4_litoral_sul = df_escalas_pag[df_escalas_pag['Servico']=='Passeio Litoral Sul de 4x4'].reset_index()

    df_4x4_litoral_sul_2 = st.session_state.df_escalas[st.session_state.df_escalas['Escala'].isin(df_4x4_litoral_sul['Escala'].unique())].groupby('Escala')\
        .agg({'Modo': 'first', 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()
    
    df_4x4_litoral_sul_2 = df_4x4_litoral_sul_2[df_4x4_litoral_sul_2['Modo']=='REGULAR'].reset_index(drop=True)

    for index in range(len(df_4x4_litoral_sul_2)):

        total_a_pagar = (df_4x4_litoral_sul_2.at[index, 'Total ADT'] + df_4x4_litoral_sul_2.at[index, 'Total CHD']) * 130

        escala_ref = df_4x4_litoral_sul_2.at[index, 'Escala']

        df_escalas_pag.loc[df_escalas_pag['Escala']==escala_ref, 'Utilitario'] = total_a_pagar

    return df_escalas_pag

st.set_page_config(layout='wide')

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1tsaBFwE3KS84r_I5-g3YGP7tTROe1lyuCw_UjtxofYI'

if not 'id_webhook' in st.session_state:

    st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucknatal"

if not 'mostrar_config' in st.session_state:

        st.session_state.mostrar_config = False

if not 'df_config' in st.session_state:

    with st.spinner('Puxando configurações...'):

        puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title('Mapa de Pagamento - Fornecedores')

st.divider()

st.header('Configurações')

alterar_configuracoes = st.button('Visualizar Configurações')

if alterar_configuracoes:

    if st.session_state.mostrar_config == True:

        st.session_state.mostrar_config = False

    else:

        st.session_state.mostrar_config = True

row01 = st.columns(1)

if st.session_state.mostrar_config == True:

    with row01[0]:

        st.subheader('Excluir Veículos')

        container_frota = st.container(height=300)

        filtrar_frota = container_frota.multiselect('', sorted(st.session_state.df_escalas_bruto['Veiculo'].dropna().unique().tolist()), key='filtrar_frota', 
                                       default=sorted(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))
        
        st.subheader('Excluir Serviços')
        
        filtrar_servicos = st.multiselect('', sorted(st.session_state.df_escalas_bruto['Servico'].dropna().unique().tolist()), key='filtrar_servicos', 
                                          default=sorted(list(filter(lambda x: x != '', st.session_state.df_config['Excluir Servicos'].tolist()))))

    salvar_config = st.button('Salvar Configurações')

    if salvar_config:

        with st.spinner('Salvando Configurações...'):

            lista_escolhas = [filtrar_frota, filtrar_servicos]

            st.session_state.df_config = pd.DataFrame({f'Coluna{i+1}': pd.Series(lista) for i, lista in enumerate(lista_escolhas)})

            st.session_state.df_config = st.session_state.df_config.fillna('')

            inserir_config(st.session_state.df_config, st.session_state.id_gsheet, 'Configurações Fornecedores')

            puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

        st.session_state.mostrar_config = False

        st.rerun()

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

    # Puxando tarifários e tratando colunas de números

    # with st.spinner('Puxando tarifários...'):

    #     puxar_tarifario_fornecedores()

    # Filtrando período solicitado pelo usuário

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                             (~st.session_state.df_escalas['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist())))) & 
                                             (~st.session_state.df_escalas['Servico'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Excluir Servicos'].tolist()))))]\
                                                .reset_index(drop=True)

    # Tratando nomes de tipos de veículos

    df_escalas = tratar_tipos_veiculos(df_escalas)

    # Tratando nomes de serviços IN e OUT

    df_escalas = tratar_servicos_in_out(df_escalas)

    # Agrupando escalas

    df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista'])\
        .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min'}).reset_index()
    
    # Adicionando apoios no dataframe
    
    df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)

    # Verificando se todos os serviços estão tarifados

    verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet)

    # Colocando valores tarifarios
        
    df_escalas_pag = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    # Ajustando valor de apoio a João Pessoa com Bolero (Pipa) p/ 150

    df_escalas_pag = ajustar_apoios_bolero_pipa(df_escalas_pag)

    # Ajustando valor de 4x4 Litoral Sul

    df_escalas_pag = ajustar_valor_litoral_sul_4x4(df_escalas_pag)

    # Identificando transfers conjugados

    df_escalas_pag = identificar_trf_conjugados(df_escalas_pag)

    # Identificando transfers hotel > hotel conjugados

    df_escalas_pag = identificar_trf_htl_conjugados(df_escalas_pag)
    
    # Gerando coluna valor levando em conta o tipo de veículo usado

    df_escalas_pag['Valor Final'] = df_escalas_pag.apply(lambda row: row[f"Conjugado {row['Tipo Veiculo']}"] if row['Servico Conjugado'] == 'X' else row[row['Tipo Veiculo']], axis=1)

    # Gerando coluna valor do fornecedor Terra Dourada
    
    df_escalas_pag['Valor Final'] = df_escalas_pag.apply(lambda row: row[f"Conjugado {row['Tipo Veiculo']} Terra Dourada"] 
                                                         if row['Servico Conjugado'] == 'X' and row['Fornecedor Motorista'] == 'TERRA DOURADA' 
                                                         else row[f"{row['Tipo Veiculo']} Terra Dourada"] 
                                                         if row['Servico Conjugado'] != 'X' and row['Fornecedor Motorista'] == 'TERRA DOURADA' else row['Valor Final'], axis=1)
    
    # Verificando se os serviços prestados pelo fornecedor Terra Dourada estão tarifados
    
    verificar_tarifarios_terra_dourada(df_escalas_pag)
    
    # Precificando 2 apoios como 1 só ou mais de 2 apoios com 2

    df_escalas_pag = precificar_apoios_2_em_1(df_escalas_pag)

    # Ajustando valor de Damiao e Luiz Antonio nos apoios de pipa

    df_escalas_pag = ajustar_valor_luiz_damiao_pipa(df_escalas_pag)

    st.session_state.df_pag_final = df_escalas_pag[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Servico Conjugado', 'Valor Final']]

    st.session_state.df_pag_final['Valor Final'] = st.session_state.df_pag_final['Valor Final'].fillna(0)

if 'df_pag_final' in st.session_state:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_fornecedores = st.session_state.df_pag_final['Fornecedor Motorista'].dropna().unique().tolist()

        fornecedor = st.multiselect('Fornecedores', sorted(lista_fornecedores), default=None)

    if fornecedor and data_pagamento and data_inicial and data_final:

        st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

        st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]\
            ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

        row2_1 = st.columns(4)

        df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Fornecedor Motorista'].isin(fornecedor)].sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

        df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

        container_dataframe = st.container()

        container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

        with row2_1[0]:

            total_a_pagar = df_pag_guia['Valor Final'].sum()

            st.subheader(f'Valor Total: R${int(total_a_pagar)}')

        soma_servicos = df_pag_guia['Valor Final'].sum()

        soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

        for item in ['Valor Final']:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        nome_html = f"{', '.join(fornecedor)}.html"

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

                for fornecedor_ref in lista_fornecedores:

                    st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor_ref]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

                    st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor_ref]\
                        ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

                    telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, fornecedor_ref, st.session_state.df_telefones['Fornecedores'].unique().tolist())

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista'])\
                        .reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

                    df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Final'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Final']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    html = definir_html(df_pag_guia)

                    nome_html = f'{fornecedor_ref}.html'

                    criar_output_html(nome_html, html, fornecedor_ref, soma_servicos)

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

                for fornecedor_ref in lista_fornecedores:

                    st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor_ref]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

                    st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista']==fornecedor_ref]\
                        ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Fornecedor Motorista']==fornecedor_ref].sort_values(by=['Data da Escala', 'Veiculo', 'Motorista'])\
                        .reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

                    df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Final'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Final']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    html = definir_html(df_pag_guia)

                    nome_html = f'{fornecedor_ref}.html'

                    criar_output_html(nome_html, html, fornecedor_ref, soma_servicos)

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

if 'html_content' in st.session_state and fornecedor:

    with row2_1[2]:

        enviar_informes = st.button(f"Enviar Informes | {', '.join(fornecedor)}")

    if enviar_informes:

        puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Fornecedores', 'df_telefones')

        telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, fornecedor, st.session_state.df_telefones['Fornecedores'].unique().tolist())
        
        payload = {"informe_html": st.session_state.html_content, 
                    "telefone": telefone_guia}
        
        response = requests.post(st.session_state.id_webhook, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
