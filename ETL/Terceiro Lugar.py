# Terceiro Lugar

import pandas as pd
import datetime as dt
import os
from sqlalchemy import create_engine
from datetime import datetime

# extrair data de hoje
hoje = dt.date.today()
ano_atual = hoje.year

# engine que conecta no banco
engine = create_engine('postgresql+psycopg2://admin:123456@localhost:5432/postgresql_db')


print('start loop')
# loop que extrai todas as tabelas da copa do mundo da Wiki, de todos os anos
for ano in range(1930,ano_atual,4):
   if ano not in(1942, 1946):
    globals()[f"link{ano}"] = f"https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_{ano}"
    globals()[f"tabelas{ano}"] = pd.read_html(globals()[f"link{ano}"])
    globals()[f"qtd_tabelas{ano}"] = len(globals()[f"tabelas{ano}"])
    for x in range(0, globals()[f"qtd_tabelas{ano}"]):
      #x é o número da tabela
      for z in globals()[f"tabelas{ano}"]:
        globals()[f"pd{ano}{x:02}"] = pd.DataFrame(globals()[f"tabelas{ano}"][x])
        globals()[f"pd{ano}{x:02}"]["Ano"] = ano
        globals()[f"pd{ano}{x:02}"]["MatchNum"] = x

   else:
     print(ano)
print('end loop')
# Extraído do arquivo listasDF
# contém todos os dataframes que são da fase correspondente na copa do mundo
df_3l = [pd193419, pd193821, pd195455, pd195842, 
 pd196262, pd196640, pd197039, pd197446, 
 pd197848, pd198266, pd198664, pd199066, 
 pd199466, pd199878, pd200281, pd200685, 
 pd201072, pd201446, pd201845, pd202245]

# tratamento 3º Lugar
# cria dataframe com dados dos dataframes da disputa pelo 3º lugar e adiciona uma coluna indicando o grupo
df3l = pd.DataFrame()
for i in range(0,len(df_3l)): 
  df_3l[i]['Grupo'] = 'Third Place'
  df_3l[i]['Stage'] = 'Knockout Stage'
  df3l = pd.concat([df3l, df_3l[i]])
df3l.reset_index(inplace=True, drop=True)

df3l['Date'] = df3l[0].map(str)
df3l['Team1'] = df3l[1].map(str)
df3l['Score'] = df3l[2].map(str)
df3l['Team2'] = df3l[3].map(str)
df3l['estadios'] = df3l[4].map(str)

# limpeza dos dados
df3l = df3l.drop(columns = [0, 1, 2, 3, 4])
df3l = df3l.loc[(df3l['Score'] !=  'nan' )]
df3l = df3l.loc[(df3l['Score'] !=  'Relatório' )] 
df3l = df3l.loc[(df3l['Score'] !=  '(Report)' )]
df3l.loc[df3l['Score'] == '4 – 2 (Prorrogação)', 'Score'] = '4 – 2'
df3l = df3l.apply(lambda x: x.astype(str).str.replace("(horário local)", ""))
df3l = df3l.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))

sepDate = df3l["Date"].str.split(" ", expand=True)
df3l['day'] = sepDate[0]

# Dicionário para mapear nomes de meses para números
meses_para_numeros = {
    'janeiro': 1,
    'fevereiro': 2,
    'março': 3,
    'abril': 4,
    'maio': 5,
    'junho': 6,
    'julho': 7,
    'agosto': 8,
    'setembro': 9,
    'outubro': 10,
    'novembro': 11,
    'dezembro': 12
    }

# Aplicar a transformação no DataFrame
df3l['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
df3l['Date'] = df3l.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = df3l["Score"].str.split("–", n=1, expand=True)
df3l['gols time 1'] = Score[0]
df3l['gols time 2'] = Score[1]
###########################################################################################

df3l['Match'] = df3l['Date'].astype(str) + df3l['Team1'] + df3l['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
df3l['Match-1'] = df3l['Match'].shift(1)

# Comparar registros com a linha anterior
df3l['Match'] = df3l['Match'] == df3l['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = df3l['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
df3l['Validação'] = match + match.shift(1)

df3l['Validação'].fillna('FalseFalse', inplace=True)
mask = df3l['Validação'] == 'TrueFalse'
mask2 = df3l['Validação'] == 'FalseTrue'
mask3 = df3l['Validação'] == 'FalseFalse'
df3l.loc[mask, 'Check1'] = df3l.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
df3l.loc[mask2, 'Check2'] = df3l.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = df3l['Check1'].max()
df3l.loc[mask3, 'Check3'] = df3l.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

df3l['Partidas'] = df3l['Check1'].fillna(0) + df3l['Check2'].fillna(0) + df3l['Check3'].fillna(0)
df3l = df3l.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
df3l.to_sql("ThirdPlace", con=engine, schema = "WorldCup", if_exists='replace')
print('end')