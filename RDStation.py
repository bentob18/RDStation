# %% [markdown]
# # Listar negociações

# # %%
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
# # Configurações iniciais
# token = "67c0c74e0fca36001419b7f4"
# base_url = "https://crm.rdstation.com/api/v1/deals"
# limit = 200  # Máximo por requisição
# rate_limit_pause = 0.1  # pausa de 0.1s para ficar abaixo do limite de 120 req/s

# #primeiro lead de todos data:  2023-06-06T16:13:34.988-03:00

# # Defina o intervalo total que você deseja consultar
# start_date_global = datetime(2023, 6, 6)
# end_date_global = datetime.today()

# all_deals = []

# # Dividindo o período por intervalos mensais (pode ajustar conforme a necessidade)
# current_start = start_date_global
# while current_start < end_date_global:
#     current_end = current_start + timedelta(days=30)
#     # Garante que current_end não ultrapasse o final do período
#     if current_end > end_date_global:
#         current_end = end_date_global

#     # Converte datas para o formato ISO esperado pela API
#     start_date_str = current_start.strftime("%Y-%m-%dT%H:%M:%S")
#     end_date_str = current_end.strftime("%Y-%m-%dT%H:%M:%S")

#     print(f"Consultando de {start_date_str} até {end_date_str}")

#     page = 1
#     while True:
#         params = {
#             "token": token,
#             "limit": limit,
#             "page": page,
#             "start_date": start_date_str,
#             "end_date": end_date_str,
#             "created_at_period": "True"
#         }

#         response = requests.get(base_url, params=params)
#         if response.status_code != 200:
#             print(f"Erro na requisição: {response.status_code}")
#             break

#         data = response.json()
#         deals = data.get("deals", [])

#         if not deals:
#             break  # Sem dados para este intervalo

#         all_deals.extend(deals)
#         print(f"  Página {page} retornou {len(deals)} deals")

#         if not data.get("has_more", False):
#             break  # Não há mais páginas para este intervalo

#         page += 1
#         time.sleep(rate_limit_pause)

#     # Avança para o próximo período
#     current_start = current_end

# # 🔹 Criar um DataFrame do pandas com os dados coletados
# df = pd.DataFrame(all_deals)

# # 🔹 Função para extrair dados aninhados
# def extract_nested_value(data, key):
#     """Pega um valor de um dicionário, se existir"""
#     return data.get(key) if isinstance(data, dict) else None

# # 🔹 Criar colunas separadas para informações aninhadas
# df["user_name"] = df["user"].apply(lambda x: extract_nested_value(x, "name"))
# df["user_email"] = df["user"].apply(lambda x: extract_nested_value(x, "email"))

# # 🔹 Extrair o nome e o ID do estágio diretamente do campo deal_stage
# df["deal_stage_name"] = df["deal_stage"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
# df["stage_id"] = df["deal_stage"].apply(lambda x: x.get("id") if isinstance(x, dict) else None)

# df["deal_source"] = df["deal_source"].apply(lambda x: extract_nested_value(x, "name"))
# df["campaign_name"] = df["campaign"].apply(lambda x: extract_nested_value(x, "name"))

# # 🔹 Extrair informações dos contatos
# df["contact_name"] = df["contacts"].apply(lambda x: x[0]["name"] if isinstance(x, list) and x else None)
# df["contact_email"] = df["contacts"].apply(lambda x: x[0]["emails"][0]["email"] if isinstance(x, list) and x and "emails" in x[0] and x[0]["emails"] else None)
# df["contact_phone"] = df["contacts"].apply(lambda x: x[0]["phones"][0]["phone"] if isinstance(x, list) and x and "phones" in x[0] and x[0]["phones"] else None)

# # 🔹 Extrair informações dos campos personalizados
# def extract_custom_field(custom_fields, label):
#     """Encontra o valor de um campo personalizado pelo nome"""
#     if isinstance(custom_fields, list):
#         for field in custom_fields:
#             if "custom_field" in field and field["custom_field"]["label"] == label:
#                 return field["value"]
#     return None

# df["lead_origin"] = df["deal_custom_fields"].apply(lambda x: extract_custom_field(x, "Página de Origem do LEAD"))
# df["contact_whatsapp"] = df["deal_custom_fields"].apply(lambda x: extract_custom_field(x, "Whatsapp ou Telefone"))

# # 🔸 Novas colunas solicitadas
# df["motivos_nao_aprovacao"] = df["deal_custom_fields"].apply(lambda x: extract_custom_field(x, "MOTIVOS NÃO APROVAÇÃO"))
# df["estado"] = df["deal_custom_fields"].apply(lambda x: extract_custom_field(x, "ESTADO"))

# # 🔹 Selecionar colunas úteis atualizadas
# df = df[[
#     "id", "name", "amount_total", "created_at", "updated_at",
#     "win", "closed_at", "user_name", "user_email", "deal_stage_name",
#     "stage_id", "deal_source", "campaign_name", "contact_name", "contact_email",
#     "contact_phone", "contact_whatsapp", "lead_origin",
#     "motivos_nao_aprovacao", "estado"
# ]]
df = pd.read_excel('df.xlsx')
# 🔹 Exibir as primeiras linhas do DataFrame atualizado

# %% [markdown]
# # Listar funis e suas etapas

# %%
import requests
import pandas as pd

# 🔹 URL da API para buscar os funis e etapas
url = "https://crm.rdstation.com/api/v1/deal_pipelines?token=67c0c74e0fca36001419b7f4"
headers = {"accept": "application/json"}

# 🔹 Faz a requisição à API
response = requests.get(url, headers=headers)

if response.status_code == 200:
    deal_pipelines = response.json()

    # 🔹 Criar lista para armazenar os dados formatados
    pipeline_data = []

    for pipeline in deal_pipelines:
        pipeline_id = pipeline["id"]
        pipeline_name = pipeline["name"]

        for stage in pipeline.get("deal_stages", []):
            pipeline_data.append({
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "stage_id": stage["id"],
                "stage_name": stage["name"],
                "stage_nickname": stage["nickname"],
                "stage_order": stage["order"],
            })

    # 🔹 Criar DataFrame
    df_pipelines = pd.DataFrame(pipeline_data)

# %% [markdown]
# # Merge para o df final

# %%
# Fazer o merge das duas bases de dados
df_final = pd.merge(
    df,  # DataFrame das negociações
    df_pipelines,  # DataFrame dos funis e estágios
    on="stage_id",  # Chave de junção
    how="left"  # Mantém todas as negociações, mesmo que não tenham um estágio correspondente
)

# Exibir as primeiras linhas do DataFrame final

# %% [markdown]
# # Listar as anotações

# %%
# import requests
# import pandas as pd

# # 🔹 URL base da API
# RD_API_URL = "https://crm.rdstation.com/api/v1/activities"

# # 🔹 Cabeçalhos da requisição
# HEADERS = {"accept": "application/json"}

# # 🔹 Parâmetros iniciais
# LIMIT = 200  # Número máximo de atividades por página
# PAGE = 1  # Página inicial

# # 🔹 Lista para armazenar as atividades
# all_activities = []

# while True:
#     params = {
#         "token": "67c0c74e0fca36001419b7f4",  # Substitua pelo seu token
#         "page": PAGE,
#         "limit": LIMIT,
#     }

#     response = requests.get(RD_API_URL, headers=HEADERS, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         activities = data.get("activities", [])

#         if not activities:  # Se não houver mais atividades, parar a coleta
#             break

#         all_activities.extend(activities)
#         PAGE += 1
#     else:
#         print(f"❌ Erro ao buscar atividades: {response.status_code} - {response.text}")
#         break

# # 🔹 Criar um DataFrame do pandas com os dados coletados
# df_activities = pd.DataFrame(all_activities)

# # 🔹 Exibir as primeiras linhas do DataFrame
# df_activities

# %%
# !pip install ftfy

# %%
# import ftfy

# # Aplicar correção automática na coluna 'text'
# df_activities['text'] = df_activities['text'].apply(ftfy.fix_text)
# df_activities

# %%
# # Agrupar as anotações por deal_id e juntar os textos com quebra de linha
# df_aggregated = df_activities.groupby('deal_id')['text'].apply(lambda x: '\n'.join(x.dropna())).reset_index()

# # Exibir o resultado
# df_aggregated

# %%
# if 'text' in df_final.columns:
#     df_final = df_final.drop(columns=['text'])

# # Renomear a coluna deal_id para id no df_aggregated
# df_aggregated.rename(columns={'deal_id': 'id'}, inplace=True)

# # Fazer o merge com df_final usando a coluna 'id'
df_merge = df_final

# %%
# .merge(df_aggregated, on='id', how='left')

# %% [markdown]
# 

# %%
#!pip install dash dash_bootstrap_components

# %% [markdown]
# # Realizando comparações e testes pra bater os dados corretamente

# %%
# # Contar pessoas únicas na coluna 'id' para cada valor em 'pipeline_names'
# pipeline_counts = (
#     df_merge
#     .groupby('pipeline_name')['id']
#     .nunique()
#     .reset_index(name='quantidade_pessoas')
#     .sort_values(by='quantidade_pessoas', ascending=False)
# )

# # Exibir resultado
# print(pipeline_counts)

# %%
# Remover tudo após '|' na coluna deal_source
df_merge['deal_source'] = df_merge['deal_source'].str.split('|').str[0].str.strip()

# %%
# Selecionar colunas relevantes para análise (sem remover as outras)
colunas_relevantes = [
    "stage_name", "pipeline_name", "user_name", "lead_origin",
    "deal_stage_name", "win", "campaign_name","deal_source", "motivos_nao_aprovacao", "user_name", "deal_source"
]

# Filtrar apenas colunas existentes na base para análise
colunas_existentes = [col for col in colunas_relevantes if col in df_merge.columns]

# Verificar se há colunas válidas para análise
if not colunas_existentes:
    print("Nenhuma das colunas relevantes foi encontrada na base de dados.")
else:
    # Gerar saídas únicas por coluna
    saidas = {}
    for col in colunas_existentes:
        saidas[col] = df_merge[col].dropna().astype(str).unique().tolist()

    # Exibir saídas
    #for col, valores in saidas.items():
       # print(f"\nColuna: {col}")
       # print("-" * 40)
        #print(valores)


# %%
# Agrupar e listar estágios por pipeline
estagios_por_pipeline = df_merge.groupby('pipeline_name')['stage_name'].unique().reset_index()

# Exibir resultados de forma clara
#for _, row in estagios_por_pipeline.iterrows():
   # pipeline = row['pipeline_name']
    #estagios = ', '.join(row['stage_name'])
    #print(f'Pipeline: {pipeline}\nEstágios: {estagios}\n{"-"*50}')

# %%
df_merge = df_merge.sort_values(by='created_at', ascending=False)

# Converter a coluna para datetime
df_merge['created_at'] = pd.to_datetime(df_merge['created_at'])

# Criar a coluna de data no formato dd-mm-yyyy
df_merge['data criação'] = df_merge['created_at'].dt.strftime('%d-%m-%Y')

# Criar a coluna de hora no formato hh:mm
df_merge['hora criação'] = df_merge['created_at'].dt.strftime('%H:%M')

df_merge = df_merge.drop(columns=['created_at'])

# Padronizar a saída da coluna motivos_nao_aprovacao
df_merge["motivos_nao_aprovacao"] = df_merge["motivos_nao_aprovacao"].replace(
    "ENVIADOS PARA 2° ANÁLISE", "ENVIADOS PARA 2º ANÁLISE"
)

# %%
# # Contar a frequência de cada valor único na coluna stage_name
# contagem_stage_name = df_merge['stage_name'].value_counts()

# print(contagem_stage_name)

# %%
# Padronizar a saída da coluna motivos_nao_aprovacao
df_merge["motivos_nao_aprovacao"] = df_merge["motivos_nao_aprovacao"].replace(
    "ENVIADOS PARA 2° ANÁLISE", "ENVIADOS PARA 2º ANÁLISE"
)

# %%
import pandas as pd

# Converter 'data criação' para datetime
df_merge['data criação'] = pd.to_datetime(df_merge['data criação'], errors='coerce', dayfirst=True)

# Converter 'hora criação' para inteiro (extraindo apenas a hora)
df_merge['hora criação'] = pd.to_datetime(df_merge['hora criação'], errors='coerce').dt.hour

# %%
import pandas as pd

# Garantir datetime
df_merge['data criação'] = pd.to_datetime(df_merge['data criação'], dayfirst=True)

# Filtro de período (ajustável conforme necessário)
start_date = pd.to_datetime("2023-06-06")
end_date = pd.to_datetime("2025-03-23")
df_filtered = df_merge[(df_merge['data criação'] >= start_date) & (df_merge['data criação'] <= end_date)]

# Explodir a coluna lead_origin mantendo a coluna data
lead_origins_exploded = df_filtered[['data criação', 'lead_origin', 'motivos_nao_aprovacao']].dropna(subset=['lead_origin']).copy()
lead_origins_exploded['lead_origin'] = (
    lead_origins_exploded['lead_origin']
    .astype(str)
    .str.replace(r"[\[\]']", "", regex=True)
    .str.split(',')
)
lead_origins_exploded = lead_origins_exploded.explode('lead_origin')
lead_origins_exploded['lead_origin'] = lead_origins_exploded['lead_origin'].str.strip()

# Remover vazios
lead_origins_exploded = lead_origins_exploded[lead_origins_exploded['lead_origin'] != ""]

# Filtrar os enviados para 2ª análise
segunda_analise = lead_origins_exploded[
    lead_origins_exploded['motivos_nao_aprovacao'] == 'ENVIADOS PARA 2º ANÁLISE'
]

# Contagem agrupada por data e origem
contagem = (
    segunda_analise
    .groupby(['lead_origin', 'data criação'])
    .size()
    .reset_index(name='Enviados para 2ª Análise')
)

# Renomear coluna para manter padrão
contagem = contagem.rename(columns={'lead_origin': 'Origem do Lead'})

# Exibir resultado
# print(f"Total de linhas: {len(contagem)}")
# contagem

# %%
import pandas as pd

# Garantir datetime
df_merge['data criação'] = pd.to_datetime(df_merge['data criação'], dayfirst=True)

# Filtro de período (ajustável)
start_date = pd.to_datetime("2023-06-06")
end_date = pd.to_datetime("2025-03-23")
df_filtered = df_merge[
    (df_merge['data criação'] >= start_date) & (df_merge['data criação'] <= end_date)
]

# Explodir lead_origin mantendo colunas necessárias
lead_origins_exploded = df_filtered[
    ['data criação', 'lead_origin', 'motivos_nao_aprovacao', 'pipeline_name']
].dropna(subset=['lead_origin']).copy()

lead_origins_exploded['lead_origin'] = (
    lead_origins_exploded['lead_origin']
    .astype(str)
    .str.replace(r"[\[\]']", "", regex=True)
    .str.split(',')
)
lead_origins_exploded = lead_origins_exploded.explode('lead_origin')
lead_origins_exploded['lead_origin'] = lead_origins_exploded['lead_origin'].str.strip()

# Remover vazios
lead_origins_exploded = lead_origins_exploded[lead_origins_exploded['lead_origin'] != ""]

# Filtrar apenas os leads enviados para 2ª análise
segunda_analise = lead_origins_exploded[
    lead_origins_exploded['motivos_nao_aprovacao'] == 'ENVIADOS PARA 2º ANÁLISE'
]

# Agrupar e contar
contagem = (
    segunda_analise
    .groupby(['lead_origin', 'data criação'])
    .agg(
        Enviados_para_2_Analise=('pipeline_name', 'count'),
        Comercial=('pipeline_name', lambda x: (x == 'Comercial').sum()),
        Relacionamento=('pipeline_name', lambda x: (x == 'Relacionamento').sum())
    )
    .reset_index()
)

# Renomear para manter padrão
contagem = contagem.rename(columns={
    'lead_origin': 'Origem do Lead',
    'Comercial': 'Qtde Comercial',
    'Relacionamento': 'Qtde Relacionamento',
    'Enviados_para_2_Analise': 'Enviados para 2ª Análise'
})


# %% [markdown]
# # Dash PRINCIPAL

# %%
import plotly.express as px

# Filtro para leads desqualificados
desqualificados = df_merge[df_merge['pipeline_name'] == 'Desqualificação']

# Contagem dos motivos de não aprovação
contagem_motivos = desqualificados['motivos_nao_aprovacao'].value_counts()

# Pegando os 15 principais motivos e agrupando o resto como "Outros"
top_motivos = contagem_motivos.head(15)
outros = pd.Series(contagem_motivos[15:].sum(), index=['Outros'])
motivos_final = pd.concat([top_motivos, outros])

# Criar dataframe para plotagem
df_plot = motivos_final.reset_index()
df_plot.columns = ['Motivo', 'Quantidade']

#fig = px.bar(
    #df_plot[::-1],  # Inverter ordem para ficar de cima para baixo
    #x='Quantidade',
    #y='Motivo',
    #orientation='h',
    #text='Quantidade',
    #labels={'Quantidade': 'Número de Leads', 'Motivo': 'Motivos de Não Aprovação'},
    #title='🛑 Principais Motivos de Não Aprovação dos Leads',
    #color='Quantidade',
    #color_continuous_scale='Blues'
#)

#fig.update_layout(
    #yaxis={'categoryorder': 'total ascending'},
    #template='plotly_white',
    #height=700,
    #coloraxis_showscale=False
#)

#fig.update_traces(
    #texttemplate='%{text}',
    #textposition='outside'
#)

# %%
import pandas as pd
import plotly.express as px
from datetime import timedelta

# Certificar-se de que 'hora criação' é numérica
df_merge['hora criação'] = pd.to_numeric(df_merge['hora criação'], errors='coerce')

# Criar coluna para considerar leads após 18h até 7h do dia seguinte
df_merge['data_dia_seguinte'] = df_merge.apply(
    lambda row: (row['data criação'] + timedelta(days=1)).date() if row['hora criação'] >= 18 else row['data criação'].date(),
    axis=1
)

# Filtrar leads entre 18h e 7h do dia seguinte
late_night_filtered = df_merge[(df_merge['hora criação'] >= 18) | (df_merge['hora criação'] < 7)]
late_night_counts = late_night_filtered.groupby('data_dia_seguinte')['id'].count().reset_index()

# Criar o gráfico
#fig = px.bar(
    #late_night_counts,
    #x='data_dia_seguinte',
    #y='id',
    #labels={'id': 'Leads após 18h até 7h', 'data_dia_seguinte': 'Data'},
    #title='🌙 Leads das 18h até 7h do Dia Seguinte',
    #text_auto=True,
    #color_discrete_sequence=['#dc3545']
#)

#fig.update_traces(textposition='outside')
#fig.update_layout(yaxis_title="Quantidade", xaxis_title="Data", template="plotly_white", bargap=0.2)

# Exibir o gráfico
#fig.show()


# %%
import dash
from flask import Flask, session, redirect, url_for, request
from dash import html
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from datetime import datetime, timedelta

# =================== DADOS =====================
df_merge['data criação'] = pd.to_datetime(df_merge['data criação'], dayfirst=True)
contagem['data criação'] = pd.to_datetime(contagem['data criação'], dayfirst=True)

min_date = df_merge['data criação'].min().date()
max_date = df_merge['data criação'].max().date()
end_default = datetime.today().date()
start_default = end_default - timedelta(days=30)

pipeline_options = [{'label': p, 'value': p} for p in sorted(df_merge['pipeline_name'].unique())]
pipeline_options.insert(0, {'label': 'Nenhum', 'value': 'Nenhum'})

# =================== APP =====================
server = Flask(__name__)
server.secret_key = os.urandom(24).hex()


app = dash.Dash(__name__, server=server, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.FLATLY])
app.config.suppress_callback_exceptions = True

USERS = {
    "admin": "LCbank",
    "gustavo": "LCbank2718"
}

@server.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in USERS and USERS[username] == password:
            session['logged_in'] = True
            return redirect('/home')
        return '''
            <p style="color:red;">Usuário ou senha inválidos</p>
            ''' + login_form()

    if session.get('logged_in'):
        return redirect('/home')
    return login_form()

def login_form():
    return '''
        <h2>Login LCbank</h2>
        <form method="post">
            Usuário: <input type="text" name="username"><br><br>
            Senha: <input type="password" name="password"><br><br>
            <input type="submit" value="Entrar">
        </form>
    '''

@server.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect('/')

# =================== LAYOUT =====================
sidebar = html.Div([
    html.H2("Dashboard", className="display-6", style={"color": "white"}),
    html.Hr(),
    html.P("LCbank Dashboard", className="lead", style={"color": "white"}),
    dbc.Nav([
        dbc.NavLink("Home", href="/home", id="home-button", style={"color": "white"}),
        dbc.NavLink("Relatório", href="/leads", id="leads-button", style={"color": "white"}),
        dbc.NavLink("Atendentes", href="/atendentes", id="atendentes-button", style={"color": "white"}),
        dbc.NavLink("Tabelas", href="/tabelas", id="tabelas-button", style={"color": "white"})
    ], vertical=True, pills=True),
    html.Div([
        html.H5("Filtrar por Pipeline", style={"margin-top": "20px", "color": "white"}),
        dcc.Dropdown(id='pipeline-filter', options=pipeline_options, value='Nenhum', clearable=False, style={"margin-bottom": "20px"}),
        dcc.Checklist(
            id='apply-filter-extra-graphs',
            options=[{'label': 'Aplicar filtro nos gráficos de motivos', 'value': 'filtro'}],
            value=[],
            style={"color": "white"}
        )
    ])
], style={"position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "20%", "padding": "20px", "background-color": "#030257"})

content = html.Div([
    html.H2("Dashboard de Dados LCbank", className="display-4"),
    html.Hr(),
    html.Div([
        html.H5("Selecionar Período de Datas", style={"margin-bottom": "10px"}),
        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=start_default,
            end_date=end_default,
            min_date_allowed=min_date,
            max_date_allowed=max_date,
            display_format='DD/MM/YYYY',
            style={"margin-bottom": "20px"}
        )
    ]),
    html.Div(id="page-content")
], style={"margin-left": "20%", "padding": "20px", "background-color": "#f8f9fa", "text-align": "center"})

app.layout = html.Div([dcc.Location(id='url'), sidebar, content])

# =================== CALLBACK DE ROTEAMENTO =====================
@app.callback(
    Output("page-content", "children"),
    [Input("home-button", "n_clicks"),
     Input("leads-button", "n_clicks"),
     Input("atendentes-button", "n_clicks"),
     Input("tabelas-button", "n_clicks")]
)
def display_page(n_home, n_leads, n_atendentes, n_tabelas):
    ctx = dash.callback_context
    if not session.get('logged_in'):
        return dcc.Location(href='/', id='redirect')
        
    if not ctx.triggered:
        return html.Div([
            html.Div([
                html.H2("🏠 Bem-vindo ao Dashboard LCbank", style={"color": "#2C3E50", "marginBottom": "10px"}),
                html.P(
                    "Use o menu à esquerda para navegar entre as análises de leads, atendentes e tabelas específicas.",
                    style={"fontSize": "16px", "color": "#555"}
                )
            ], style={
                "backgroundColor": "#ffffff",
                "padding": "40px",
                "marginTop": "30px",
                "borderRadius": "8px",
                "boxShadow": "0 4px 10px rgba(0,0,0,0.05)",
                "maxWidth": "800px",
                "margin": "auto"
            })
        ])

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == "home-button":
         return html.Div([
    html.Div([
        html.H2("🏠 Bem-vindo ao Dashboard LCbank", style={"color": "#2C3E50", "marginBottom": "10px"}),
        html.P("Use o menu à esquerda para navegar entre as análises de leads, atendentes e tabelas específicas.",
               style={"fontSize": "16px", "color": "#555"})
    ], style={
        "backgroundColor": "#ffffff",
        "padding": "40px",
        "marginTop": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 4px 10px rgba(0,0,0,0.05)",
        "maxWidth": "800px",
        "margin": "auto"
    })
])


    elif button_id == "leads-button":
        return html.Div(id='leads-content')

    elif button_id == "atendentes-button":
        atendimentos = df_merge['user_name'].value_counts()
        atendentes_filtrados = atendimentos[atendimentos > 100]
        user_options = [{'label': nome, 'value': nome} for nome in atendentes_filtrados.index]
        default_user = user_options[0]['value'] if user_options else None

        return html.Div([
    html.Div([
        html.H4("Selecionar Atendente", style={"color": "#2C3E50"}),
        dcc.Dropdown(
            id='user-selector',
            options=user_options,
            value=default_user,
            clearable=False,
            style={"width": "60%", "margin": "auto", "marginBottom": "20px"}
        )
    ], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
        "textAlign": "center"
    }),

    html.Div([
        dcc.Graph(id='grafico-atendimentos')
    ], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
    }),

    html.Div([
        dcc.Graph(id='grafico-pizza-atendimentos')
    ], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
    }),
], style={"maxWidth": "1200px", "margin": "auto"})


    elif button_id == "tabelas-button":
        return html.Div([
            html.Div([
                html.H4("📅 Selecionar Período", style={"marginBottom": "10px", "color": "#2C3E50"}),
                dcc.Dropdown(
                    id='period-filter-tabelas',
                    options=[
                        {'label': 'Diário', 'value': 'D'},
                        {'label': 'Semanal', 'value': 'W'},
                        {'label': 'Mensal', 'value': 'M'},
                        {'label': 'Anual', 'value': 'Y'},
                        {'label': 'Total', 'value': 'T'}
                    ],
                    value='T',
                    clearable=False,
                    style={"width": "300px", "margin": "auto"}
                ),
            ], style={
                "backgroundColor": "#ffffff",
                "padding": "20px",
                "marginBottom": "30px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 6px rgba(0,0,0,0.05)",
                "textAlign": "center"
            }),

            html.Div([
                html.Div(id="lead-origin-table", style={
                    "backgroundColor": "#ffffff",
                    "padding": "20px",
                    "marginBottom": "30px",
                    "borderRadius": "8px",
                    "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
                }),

                html.Div(id="segunda-tabela", style={
                    "backgroundColor": "#ffffff",
                    "padding": "20px",
                    "marginBottom": "30px",
                    "borderRadius": "8px",
                    "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
                }),

                html.Div(id="tabela-total-geral", style={
                    "backgroundColor": "#ffffff",
                    "padding": "20px",
                    "marginBottom": "30px",
                    "borderRadius": "8px",
                    "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
                }),

                html.Div(id="tabela-pipeline", style={
                    "backgroundColor": "#ffffff",
                    "padding": "20px",
                    "marginBottom": "30px",
                    "borderRadius": "8px",
                    "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
                }),
            ], style={"maxWidth": "1200px", "margin": "auto"})
        ])

# ========== CALLBACK ABA LEADS (GRÁFICOS) ==========
@app.callback(
    Output("leads-content", "children"),
    [Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date"),
     Input("pipeline-filter", "value"),
     Input("apply-filter-extra-graphs", "value")]
)
def update_leads_content(start_date, end_date, selected_pipeline, apply_filter):
    dff = df_merge[(df_merge['data criação'] >= pd.to_datetime(start_date)) & (df_merge['data criação'] <= pd.to_datetime(end_date))]

    dff_pipeline = dff if selected_pipeline == 'Nenhum' else dff[dff['pipeline_name'] == selected_pipeline]

    # Gráfico 1
    registros_diarios = dff_pipeline.groupby(dff_pipeline['data criação'].dt.date)['id'].count().reset_index()
    fig = px.bar(
        registros_diarios,
        x='data criação',
        y='id',
        labels={'id': 'Número de Registros', 'data criação': 'Data'},
        title='📊 Registros Diários',
        color_discrete_sequence=['#007bff']
    )
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside'
    )
    fig.update_layout(
        yaxis_title="Quantidade",
        xaxis_title="Data",
        template="plotly_white",
        bargap=0.2,
        uniformtext_minsize=8,
        uniformtext_mode='hide',
        margin=dict(t=60),  # Aumenta o topo
    )



    # Gráfico 2
    dff['data_dia_seguinte'] = dff.apply(lambda row: (row['data criação'] + timedelta(days=1)).date() if row['hora criação'] >= 18 else row['data criação'].date(), axis=1)
    late = dff[(dff['hora criação'] >= 18) | (dff['hora criação'] < 7)]
    late_counts = late.groupby('data_dia_seguinte')['id'].count().reset_index()
    fig_late = px.bar(
        late_counts,
        x='data_dia_seguinte',
        y='id',
        labels={'id': 'Leads após 18h até 7h', 'data_dia_seguinte': 'Data'},
        title='🌙 Leads das 18h até 7h do Dia Seguinte',
        color_discrete_sequence=['#dc3545']
    )
    fig_late.update_traces(
        texttemplate='%{y}',
        textposition='outside'
    )
    fig_late.update_layout(
        yaxis_title="Quantidade",
        xaxis_title="Data",
        template="plotly_white",
        bargap=0.2,
        uniformtext_minsize=8,
        uniformtext_mode='hide',
        margin=dict(t=60),  # Aumenta o topo
    )


    # Gráfico 3
    if 'filtro' in apply_filter:
        # Filtrar apenas por data (ignorar pipeline)
        dff_extra = dff.copy()
    else:
        # Mostrar todos os dados, sem filtro
        dff_extra = df_merge.copy()


    desqualificados = dff_extra[dff_extra['stage_name'] == 'DESQUALIFICADOS']
    motivos = desqualificados['motivos_nao_aprovacao'].value_counts()

    if len(motivos) > 15:
        top = motivos.head(14)
        outros = pd.Series(motivos[14:].sum(), index=['Outros'])
        motivos_final = pd.concat([top, outros])
    else:
        motivos_final = motivos

    df_motivos = motivos_final.sort_values(ascending=False).reset_index()
    df_motivos.columns = ['Motivo', 'Quantidade']

    fig_motivos = px.bar(df_motivos[::-1], x='Quantidade', y='Motivo', orientation='h', title='Principais Motivos de Não Aprovação (Desqualificação)', text='Quantidade', color='Quantidade', color_continuous_scale='Blues')
    fig_motivos.update_layout(template='plotly_white', height=700, coloraxis_showscale=False)
    fig_motivos.update_traces(texttemplate='%{text}', textposition='auto')
    fig_motivos.update_layout(margin=dict(t=60, r=100))  # Aumenta o espaço à direita



    # Gráfico 4
    total = dff_pipeline['deal_source'].replace({'Desconhecido': 'Sem rastreio', 'Outros': 'Sem rastreio'}).value_counts()
    segunda = dff_pipeline[dff_pipeline['motivos_nao_aprovacao'] == 'ENVIADOS PARA 2º ANÁLISE']['deal_source'].replace({'Desconhecido': 'Sem rastreio', 'Outros': 'Sem rastreio'}).value_counts()
    resultado = pd.DataFrame({'Total de Leads': total, 'Leads ENVIADOS PARA 2ª ANÁLISE': segunda}).fillna(0).astype(int).groupby(level=0).sum()
    resultado = resultado[resultado['Total de Leads'] > 50].sort_values('Total de Leads', ascending=False)

    fig_marketing = go.Figure()
    fig_marketing.add_trace(go.Bar(x=resultado.index, y=resultado['Leads ENVIADOS PARA 2ª ANÁLISE'], name='Enviados p/ 2ª Análise', marker_color='#FF6F61', text=resultado['Leads ENVIADOS PARA 2ª ANÁLISE'], textposition='inside'))
    fig_marketing.add_trace(go.Bar(x=resultado.index, y=resultado['Total de Leads'] - resultado['Leads ENVIADOS PARA 2ª ANÁLISE'], name='Demais Leads', marker_color='#6B5B95', text=resultado['Total de Leads'], textposition='outside'))
    fig_marketing.update_layout(title='Leads por Estratégia de Marketing (com destaque p/ 2ª Análise)', barmode='stack', template='plotly_white',bargap=0.2, uniformtext_minsize=8,uniformtext_mode='hide', margin=dict(t=60))
    return html.Div([
    html.Div([dcc.Graph(figure=fig)], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.05)"
    }),
    html.Div([dcc.Graph(figure=fig_late)], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.05)"
    }),
    html.Div([dcc.Graph(figure=fig_motivos)], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.05)"
    }),
    html.Div([dcc.Graph(figure=fig_marketing)], style={
        "backgroundColor": "#ffffff",
        "padding": "20px",
        "marginBottom": "30px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.05)"
    }),
], style={"maxWidth": "1200px", "margin": "auto"})


# === ATENDENTES - Callback gráfico de barras
@app.callback(
    Output("grafico-atendimentos", "figure"),
    [Input("user-selector", "value"),
     Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date")]
)
def update_grafico_user(selected_user, start_date, end_date):
    if selected_user is None:
        return go.Figure()

    # Filtrar por data
    dff = df_merge[(df_merge['data criação'] >= pd.to_datetime(start_date)) &
                   (df_merge['data criação'] <= pd.to_datetime(end_date))]

    # Filtrar por atendente
    dff_user = dff[dff['user_name'] == selected_user]

    if dff_user.empty:
        return go.Figure(layout={"title": f"Sem dados para {selected_user} no período selecionado"})

    # Total de atendimentos por dia
    atendimentos_diarios = dff_user.groupby(dff_user['data criação'].dt.date)['id'].count().reset_index()
    atendimentos_diarios.columns = ['Data', 'Total Atendimentos']

    # Explodir lead_origin
    lead_exp = dff_user[['data criação', 'lead_origin']].dropna().copy()
    lead_exp['lead_origin'] = lead_exp['lead_origin'].astype(str).str.replace(r"[\[\]']", "", regex=True).str.split(',')
    lead_exp = lead_exp.explode('lead_origin')
    lead_exp['lead_origin'] = lead_exp['lead_origin'].str.strip()

    # Filtrar WhatsApp e contar por dia
    whatsapp = lead_exp[lead_exp['lead_origin'].str.lower() == 'whatsapp']
    whatsapp_diario = whatsapp.groupby(whatsapp['data criação'].dt.date)['lead_origin'].count().reset_index()
    whatsapp_diario.columns = ['Data', 'Atendimentos WhatsApp']

    # Mesclar com total
    resultado_diario = pd.merge(atendimentos_diarios, whatsapp_diario, on='Data', how='left').fillna(0)
    resultado_diario['Atendimentos WhatsApp'] = resultado_diario['Atendimentos WhatsApp'].astype(int)

    # Plot
    fig_regis = go.Figure()
    fig_regis.add_trace(go.Bar(
        x=resultado_diario['Data'],
        y=resultado_diario['Atendimentos WhatsApp'],
        name='WhatsApp',
        marker_color='#FF6F61',
        text=resultado_diario['Atendimentos WhatsApp'],
        textposition='inside'
    ))
    fig_regis.add_trace(go.Bar(
        x=resultado_diario['Data'],
        y=resultado_diario['Total Atendimentos'] - resultado_diario['Atendimentos WhatsApp'],
        name='Outros',
        marker_color='#6B5B95',
        text=resultado_diario['Total Atendimentos'],
        textposition='outside'
    ))
    fig_regis.update_layout(
        title=f'📊 Atendimentos de {selected_user} (WhatsApp vs Total)',
        xaxis_title='Data',
        yaxis_title='Quantidade',
        legend_title='Origem',
        template='plotly_white',
        barmode='stack',
        bargap=0.2,
        uniformtext_minsize=8,
        uniformtext_mode='hide',
        margin=dict(t=60)
    )

    return fig_regis

# === ATENDENTES - Callback gráfico de pizza
@app.callback(
    Output("grafico-pizza-atendimentos", "figure"),
    [Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date")]
)
def update_pizza(start_date, end_date):
    dff = df_merge[(df_merge['data criação'] >= pd.to_datetime(start_date)) & (df_merge['data criação'] <= pd.to_datetime(end_date))]
    valid_users = df_merge['user_name'].value_counts()
    valid_users = valid_users[valid_users > 100].index.tolist()
    dff = dff[dff['user_name'].isin(valid_users)]
    contagem = dff['user_name'].value_counts().reset_index()
    contagem.columns = ['user_name', 'quantidade']
    fig = px.pie(contagem, names='user_name', values='quantidade', title='🍕 Participação de Atendimentos por Atendente', color_discrete_sequence=px.colors.qualitative.Set3)
    fig.update_layout(template='plotly_white')
    return fig

# ========== CALLBACK TABELAS (4 Tabelas) ==========
@app.callback(
    [Output("lead-origin-table", "children"),
     Output("segunda-tabela", "children"),
     Output("tabela-total-geral", "children"),
     Output("tabela-pipeline", "children")],
    [Input("period-filter-tabelas", "value")]
)
def update_tabelas(period):
    hoje = df_merge['data criação'].max().date()

    def filtrar_por_periodo(df):
        dff = df.copy()
        if period == 'D':
            dff = dff[dff['data criação'].dt.date == hoje]
        elif period == 'W':
            dff = dff[dff['data criação'].dt.date >= hoje - timedelta(days=7)]
        elif period == 'M':
            dff = dff[dff['data criação'].dt.date >= hoje - timedelta(days=30)]
        elif period == 'Y':
            dff = dff[dff['data criação'].dt.date >= hoje - timedelta(days=365)]
        return dff

    dff_merge = filtrar_por_periodo(df_merge)
    dff_contagem = filtrar_por_periodo(contagem)

    common_style = {
        'textAlign': 'center',
        'fontSize': '14px',
        'padding': '5px',
        'backgroundColor': '#f9f9f9',
        'color': 'black',
    }

    highlight_top3 = [
        {"if": {"row_index": 0}, "backgroundColor": "#2ECC71", "color": "white"},
        {"if": {"row_index": 1}, "backgroundColor": "#3498DB", "color": "white"},
        {"if": {"row_index": 2}, "backgroundColor": "#F1C40F", "color": "black"},
        {"if": {"state": "active"}, "backgroundColor": "#D5DBDB", "color": "black"},
    ]

    # TABELA 1
    lead_origins_exploded = (
        dff_merge['lead_origin'].dropna().astype(str)
        .str.replace(r"[\[\]']", "", regex=True)
        .str.split(',').explode().str.strip()
    )
    lead_origins_exploded = lead_origins_exploded[lead_origins_exploded != ""]
    lead_counts = lead_origins_exploded.value_counts().reset_index()
    lead_counts.columns = ['Origem do Lead', 'Quantidade']
    if len(lead_counts) > 23:
        outros = pd.DataFrame({'Origem do Lead': ['Outros'], 'Quantidade': [lead_counts[23:]['Quantidade'].sum()]})
        lead_counts = pd.concat([lead_counts[:23], outros])
    tabela_1 = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in lead_counts.columns],
        data=lead_counts.to_dict('records'),
        style_table={'width': '60%', 'margin': 'auto'},
        style_header={
            'backgroundColor': 'black',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_cell=common_style,
        style_data_conditional=highlight_top3
    )

    # TABELA 2
    contagem_agg = dff_contagem.groupby('Origem do Lead', as_index=False)['Enviados para 2ª Análise'].sum()
    contagem_agg = contagem_agg.sort_values(by='Enviados para 2ª Análise', ascending=False)
    if len(contagem_agg) > 23:
        outros = pd.DataFrame({'Origem do Lead': ['Outros'], 'Enviados para 2ª Análise': [contagem_agg[23:]['Enviados para 2ª Análise'].sum()]})
        contagem_agg = pd.concat([contagem_agg[:23], outros])
    tabela_2 = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in contagem_agg.columns],
        data=contagem_agg.to_dict('records'),
        style_table={'width': '100%'},
        style_header={
            'backgroundColor': '#34495E',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_cell=common_style,
        style_data_conditional=highlight_top3
    )

    # TABELA 3
    total_leads_base = dff_merge.shape[0]
    enviados_2_analise = dff_merge[dff_merge['motivos_nao_aprovacao'] == 'ENVIADOS PARA 2º ANÁLISE']
    total_enviados = enviados_2_analise.shape[0]
    proc_aprovado = enviados_2_analise[enviados_2_analise['stage_name'] == 'Proc. Aprovado Cliente'].shape[0]
    novos = enviados_2_analise[enviados_2_analise['stage_name'] == 'Novos'].shape[0]
    porcent_proc_aprovado = (proc_aprovado / total_enviados) * 100 if total_enviados else 0
    porcent_novos = (novos / total_enviados) * 100 if total_enviados else 0
    total_comercial = dff_contagem['Qtde Comercial'].sum()
    total_relacionamento = dff_contagem['Qtde Relacionamento'].sum()
    pct_comercial = (total_comercial / total_enviados) * 100 if total_enviados else 0
    pct_relacionamento = (total_relacionamento / total_enviados) * 100 if total_enviados else 0

    total_geral_vertical = pd.DataFrame([
        {"Métrica": "Total na Base", "Valor": total_leads_base},
        {"Métrica": "Enviados para 2ª Análise", "Valor": total_enviados},
        {"Métrica": "Proc. Aprovado Cliente", "Valor": f"{proc_aprovado} ({porcent_proc_aprovado:.2f}%)"},
        {"Métrica": "Novos", "Valor": f"{novos} ({porcent_novos:.2f}%)"},
        {"Métrica": "Total no Pipeline Comercial", "Valor": total_comercial},
        {"Métrica": "Total no Pipeline Relacionamento", "Valor": total_relacionamento},
        {"Métrica": "% Comercial", "Valor": f"{pct_comercial:.2f}%"},
        {"Métrica": "% Relacionamento", "Valor": f"{pct_relacionamento:.2f}%"},
    ])
    tabela_total = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in total_geral_vertical.columns],
        data=total_geral_vertical.to_dict('records'),
        style_table={
            'width': '100%',
            'margin': 'auto',
            'border': '1px solid #ccc',
            'borderRadius': '6px',
            'boxShadow': '0px 0px 5px rgba(0,0,0,0.1)'
        },
        style_header={
            'backgroundColor': '#2C3E50',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_cell={
            'textAlign': 'left',
            'fontSize': '14px',
            'padding': '8px',
            'backgroundColor': '#ECF0F1',
            'color': '#2C3E50',
        },
        style_data_conditional=[
            {
                "if": {"filter_query": '{Métrica} contains "%"'},
                "backgroundColor": "#FDEDEC",
                "color": "#C0392B",
                "fontWeight": "bold"
            }
        ]
    )

    # TABELA 4
    pipeline_counts = dff_merge['pipeline_name'].value_counts().reset_index()
    pipeline_counts.columns = ['Pipeline', 'Quantidade']
    pipeline_counts = pipeline_counts.sort_values(by='Quantidade', ascending=False)
    tabela_pipeline = dash_table.DataTable(
        columns=[{"name": col, "id": col} for col in pipeline_counts.columns],
        data=pipeline_counts.to_dict('records'),
        style_table={
            'width': '100%',
            'marginTop': '20px',
            'border': '1px solid #ccc',
            'borderRadius': '6px',
            'boxShadow': '0px 0px 5px rgba(0,0,0,0.1)'
        },
        style_header={
            'backgroundColor': '#1F618D',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_cell=common_style,
        style_data_conditional=highlight_top3
    )

    return tabela_1, tabela_2, tabela_total, tabela_pipeline

# ========== RUN ==========
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)

# %%



