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
from google.oauth2 import service_account

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()
    request_name = f'SELECT * FROM {vw_name}'
    cursor.execute(request_name)
    resultado = cursor.fetchall()
    cabecalho = [desc[0] for desc in cursor.description]
    cursor.close()
    conexao.close()
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    return df

def puxar_dados_phoenix():

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_fornecedores', st.session_state.base_luck)

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores'

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def inserir_config(df_itens_faltantes, id_gsheet, nome_aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z1000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def tratar_colunas_df_tarifario(df, lista_colunas):

    for coluna in lista_colunas:

        df[coluna] = (df[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

        df[coluna] = pd.to_numeric(df[coluna])

def puxar_tarifario_fornecedores():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Fornecedores', 'df_tarifario')

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Lanchas', 'df_tarifario_lanchas')

    puxar_aba_simples(st.session_state.id_gsheet, 'Valores Específicos Lanchas', 'df_tarifario_esp_lanchas')

    tratar_colunas_df_tarifario(st.session_state.df_tarifario, ['Valor ADT', 'Valor CHD'])

    tratar_colunas_df_tarifario(st.session_state.df_tarifario_lanchas, ['Valor Final', 'Qtd. Pax'])

    tratar_colunas_df_tarifario(st.session_state.df_tarifario_esp_lanchas, ['Valor Final'])

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def gerar_df_pag_tpp():

    # Filtrando período solicitado pelo usuário

    lista_servicos_tarifarios_por_pax = ['ACTE MERGULHO BATISMO', 'MERGULHO BATISMO DE PRAIA', 'MERGULHO BATISMO EMBARCADO (MANHÃ)', 'MERGULHO BATISMO EMBARCADO (TARDE)', 
                                        'MERGULHO CREDENCIADO C/ EQUIPAMENTO', 'MERGULHO CREDENCIADO S/ EQUIPAMENTO', 'PASSEIO DE BARCO', 'PASSEIO DE CANOA']

    df_escalas_tarif_por_pax = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                        (st.session_state.df_escalas['Servico'].isin(lista_servicos_tarifarios_por_pax))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_tarif_por_pax.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    # Colocando valores tarifarios
        
    df_escalas_pag_tpp = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    # Calculando Valor Final

    df_escalas_pag_tpp['Valor Final'] = (df_escalas_pag_tpp['Total ADT'] * df_escalas_pag_tpp['Valor ADT']) + (df_escalas_pag_tpp['Total CHD'] * df_escalas_pag_tpp['Valor CHD'])

    return df_escalas_pag_tpp

def gerar_df_pag_entardecer():

    df_escalas_entardecer = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                        (st.session_state.df_escalas['Servico'].isin(['ENTARDECER']))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_entardecer.groupby(['Data da Escala', 'Escala', 'Servico', 'adicional']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    # Colocando valores tarifarios
        
    df_escalas_pag_ent = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

    # Identificando MARINA

    df_escalas_pag_ent['Servico'] = df_escalas_pag_ent.apply(lambda row: f"{row['Servico']} - MARINA" if row['adicional']=='ENTARDECER (MARINA SERVICOS NAUTICOS LTDA)' else row['Servico'], axis=1)

    df_escalas_pag_ent = df_escalas_pag_ent.drop(columns=['adicional'])

    # Agrupando de novo pra tirar repetidos e somar ADT e CHD

    df_escalas_pag_ent = df_escalas_pag_ent.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Total ADT': 'sum', 'Total CHD': 'sum', 'Valor ADT': 'first', 'Valor CHD': 'first'}).reset_index()

    # Calculando Valor Final    

    df_escalas_pag_ent['Valor Final'] = (df_escalas_pag_ent['Total ADT'] * df_escalas_pag_ent['Valor ADT']) + (df_escalas_pag_ent['Total CHD'] * df_escalas_pag_ent['Valor CHD'])

    df_escalas_pag_ent.loc[df_escalas_pag_ent['Servico']=='ENTARDECER - MARINA', 'Valor Final'] = 1500

    return df_escalas_pag_ent

def gerar_df_pag_barco():

    df_escalas_barco = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                   (st.session_state.df_escalas['Servico'].isin(['PASSEIO DE BARCO PRIVATIVO', 'BARCO PRIVATICO PRAIA CONCEICAO / PORTO']))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_barco.groupby(['Data da Escala', 'Escala', 'Servico']).agg({'Data | Horario Apresentacao': 'max'}).reset_index()

    df_escalas_group['Horário'] = df_escalas_group['Data | Horario Apresentacao'].dt.time

    df_escalas_group['Valor Final'] = df_escalas_group['Data | Horario Apresentacao'].apply(lambda x: 3500 if x.time()<=time(12) else 2000)

    df_escalas_group = df_escalas_group.drop(columns=['Data | Horario Apresentacao'])

    return df_escalas_group

def gerar_df_pag_lancha():

    df_escalas_lancha = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                    (st.session_state.df_escalas['Servico'].isin(['LANCHA PRIVATIVA']))].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group = df_escalas_lancha.groupby(['Data da Escala', 'Escala', 'Servico', 'adicional']).agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    df_escalas_group['Servico'] = df_escalas_group.apply(lambda row: f"{row['Servico']} - LANCHA MARESIA" if 'LANCHA MARESIA' in row['adicional'] 
                                                        else f"{row['Servico']} - LANCHA GUARDIÃO" if 'LANCHA GUARDIÃO' in row['adicional'] 
                                                        else f"{row['Servico']} - LANCHA EVANOELE DOMINIC DA SILVA" if 'EVANOELE DOMINIC DA SILVA' in row['adicional']
                                                        else f"{row['Servico']} - LANCHA JOSE MANOEL DA SILVA JUNIOR" if 'JOSE MANOEL DA SILVA JUNIOR' in row['adicional']
                                                        else f"{row['Servico']} - LANCHA SEM FORNECEDOR" if 'sem fornecedor' in row['adicional']
                                                        else f"{row['Servico']} - LANCHA SERGIO LUIZ DO AMARANTE" if 'SERGIO LUIZ DO AMARANTE' in row['adicional'] 
                                                        else f"{row['Servico']} - LANCHA FORNECEDOR NÃO IDENTIFICADO", axis=1)

    df_escalas_group['Qtd. Pax'] = df_escalas_group[['Total ADT', 'Total CHD']].sum(axis=1)

    # Colocando valores tarifarios

    df_escalas_pag_lancha = pd.merge(df_escalas_group, st.session_state.df_tarifario_lanchas, on=['Servico', 'Qtd. Pax'], how='left')

    df_escalas_pag_lancha = df_escalas_pag_lancha.drop(columns=['adicional', 'Qtd. Pax'])

    lista_escalas = st.session_state.df_tarifario_esp_lanchas['Escala'].unique().tolist()

    for escala in lista_escalas:

        df_escalas_pag_lancha.loc[df_escalas_pag_lancha['Escala']==escala, 'Valor Final'] = \
            st.session_state.df_tarifario_esp_lanchas.loc[st.session_state.df_tarifario_esp_lanchas['Escala']==escala, 'Valor Final'].values[0]

    return df_escalas_pag_lancha

def gerar_df_pag_escalas_geral():

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                            (~st.session_state.df_escalas['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist())))) & 
                                            (~st.session_state.df_escalas['Servico'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Excluir Servicos'].tolist()))))]\
                                                .reset_index(drop=True)


    df_escalas['Valor Final'] = pd.to_numeric(st.session_state.df_config['Valor Diária'].iloc[0])

    df_escalas = df_escalas.groupby(['Data da Escala', 'Veiculo']).agg({'Escala': transformar_em_string, 'Servico': transformar_em_string, 'Valor Final': 'first'}).reset_index()

    df_escalas['Servico'] = df_escalas['Veiculo'] + ' - ' + df_escalas['Servico']

    df_escalas = df_escalas[['Data da Escala', 'Escala', 'Servico', 'Valor Final']]

    return df_escalas

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

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
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

if not 'base_luck' in st.session_state:

    st.session_state.base_luck = 'test_phoenix_noronha'

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1aGO6ni3zLJwzAXuhXNUZfIjjZ87japcg3GPsUEReMIs'

if not 'id_webhook' in st.session_state:

    st.session_state.id_webhook = "https://conexao.multiatend.com.br/webhook/pagamentolucknoronha"

if not 'mostrar_config' in st.session_state:

    st.session_state.mostrar_config = False

if not 'df_config' in st.session_state:

    with st.spinner('Puxando configurações...'):

        puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_fornecedores':

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
        
        st.subheader('Valor Diária')
        
        valor_diaria = st.number_input('', value=pd.to_numeric(st.session_state.df_config['Valor Diária'].iloc[0]))

    salvar_config = st.button('Salvar Configurações')

    if salvar_config:

        with st.spinner('Salvando Configurações...'):

            lista_escolhas = [filtrar_frota, filtrar_servicos, valor_diaria]

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

    if not data_pagamento:

        st.warning('Preencha a data de pagamento para ter acesso aos mapas de pagamentos')

if atualizar_phoenix:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

if gerar_mapa:

    # Puxando tarifários e tratando colunas de números

    with st.spinner('Puxando tarifários...'):

        puxar_tarifario_fornecedores()

    # Gerando df_pag de serviços tarifados por pax

    df_escalas_pag_tpp = gerar_df_pag_tpp()

    # Gerando df_pag de Entardecer

    df_escalas_pag_ent = gerar_df_pag_entardecer()

    # Gerando df_pag de Barcos

    df_escalas_pag_barco = gerar_df_pag_barco()

    # Gerando df_pag de Lanchas

    df_escalas_pag_lancha = gerar_df_pag_lancha()

    # Gerando df_pag dos fornecedores que são veículos normais

    df_escalas_pag_normal = gerar_df_pag_escalas_geral()

    st.session_state.df_pag_final_forn = pd.concat([df_escalas_pag_tpp, df_escalas_pag_ent, df_escalas_pag_barco, df_escalas_pag_lancha, df_escalas_pag_normal], ignore_index=True)

    st.session_state.df_pag_final_forn = st.session_state.df_pag_final_forn[['Data da Escala', 'Escala', 'Servico', 'Horário', 'Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD', 'Valor Final']]

    for coluna in ['Total ADT', 'Total CHD', 'Valor ADT', 'Valor CHD']:

        st.session_state.df_pag_final_forn[coluna] = st.session_state.df_pag_final_forn[coluna].fillna(0)

if 'df_pag_final_forn' in st.session_state:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_servicos = st.session_state.df_pag_final_forn['Servico'].dropna().unique().tolist()

        servico = st.multiselect('Serviço', sorted(lista_servicos), default=None)

    if servico and data_pagamento and data_inicial and data_final:

        row2_1 = st.columns(4)

        df_pag_guia = st.session_state.df_pag_final_forn[st.session_state.df_pag_final_forn['Servico'].isin(servico)].sort_values(by=['Data da Escala']).reset_index(drop=True)

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

        container_dataframe = st.container()

        container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

        with row2_1[0]:

            total_a_pagar = df_pag_guia['Valor Final'].sum()

            st.subheader(f'Valor Total: R${int(total_a_pagar)}')

        soma_servicos = df_pag_guia['Valor Final'].sum()

        soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

        for item in ['Valor Final', 'Valor ADT', 'Valor CHD']:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else None)

        for item in ['Total ADT', 'Total CHD']:

            df_pag_guia[item] = df_pag_guia[item].astype(int)

        html = definir_html(df_pag_guia)

        nome_html = f"{', '.join(servico)}.html"

        nome_html = nome_html.replace("/", "-")

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

                    if 'CARRO' in servico_ref:

                        servico_ref = servico_ref.split(' - ')[0]

                    telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, servico_ref, st.session_state.df_telefones['Fornecedores'].unique().tolist())

                    df_pag_guia = st.session_state.df_pag_final_forn[st.session_state.df_pag_final_forn['Servico']==servico_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala']).dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Final'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Final', 'Valor ADT', 'Valor CHD']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else None)

                    for item in ['Total ADT', 'Total CHD']:

                        df_pag_guia[item] = df_pag_guia[item].astype(int)

                    html = definir_html(df_pag_guia)

                    nome_html = f'{servico_ref}.html'

                    nome_html = nome_html.replace("/", "-")

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

if 'html_content' in st.session_state and len(servico)>=1:

    with row2_1[2]:

        enviar_informes = st.button(f"Enviar Informes | {', '.join(servico)}")

    if enviar_informes and len(servico)==1:

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

    elif enviar_informes and len(servico)>1:

        nome_fornecedor = servico[0].split(' - ')[0]

        puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Fornecedores', 'df_telefones')

        telefone_guia = verificar_fornecedor_sem_telefone(st.session_state.id_gsheet, nome_fornecedor, st.session_state.df_telefones['Fornecedores'].unique().tolist())
        
        payload = {"informe_html": st.session_state.html_content, 
                   "telefone": telefone_guia}
        
        response = requests.post(st.session_state.id_webhook, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
