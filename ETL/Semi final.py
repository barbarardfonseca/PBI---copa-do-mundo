# Semi final

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

f_sf = [pd193031, pd193032, pd193417, pd193418, 
 pd193819, pd193820, pd195027, pd195028, 
 pd195029, pd195030, pd195031, pd195032, 
 pd195453, pd195454, pd195840, pd195841, 
 pd196260, pd196261, pd196638, pd196639, 
 pd197037, pd197038, pd198263, pd198264, 
 pd198265, pd198662, pd198663, pd199062, 
 pd199063, pd199064, pd199065, pd199464, 
 pd199465, pd199875, pd199876, pd199877, 
 pd200279, pd200280, pd200683, pd200684, 
 pd201070, pd201071, pd201443, pd201444, 
 pd201445, pd201843, pd201844, pd202243, 
 pd202244]

# tratamento 3º Lugar
# cria dataframe com dados dos dataframes da disputa pelo 3º lugar e adiciona uma coluna indicando o grupo
sf = pd.DataFrame()
for i in range(0,len(f_sf)): 
  f_sf[i]['Grupo'] = 'SemiFinal'
  f_sf[i]['Stage'] = 'Knockout Stage'
  sf = pd.concat([sf, f_sf[i]])
sf.reset_index(inplace=True, drop=True)

sf['Date'] = sf[0].map(str)
sf['Team1'] = sf[1].map(str)
sf['Score'] = sf[2].map(str)
sf['Team2'] = sf[3].map(str)
sf['estadios'] = sf[4].map(str)

# limpeza dos dados
sf = sf.drop(columns = [0, 1, 2, 3, 4])
sf = sf.loc[(sf['Score'] !=  'nan' )]
sf = sf.loc[(sf['Score'] !=  'Relatório' )] 
sf = sf.loc[(sf['Score'] !=  'relatório' )] 
sf = sf.loc[(sf['Team2'] !=  "Silvestre 71'" )]
sf = sf.loc[(sf['Score'] !=  '(Report)' )]
sf = sf.loc[(sf['Score'] !=  'Penalidades' )]
sf = sf.loc[(sf['Score'] !=  'Relatório[3]' )]
sf = sf.apply(lambda x: x.astype(str).str.replace("(", ""))
sf = sf.apply(lambda x: x.astype(str).str.replace(")", ""))
sf = sf.apply(lambda x: x.astype(str).str.replace("Prorrogação", ""))
sf = sf.apply(lambda x: x.astype(str).str.replace(" 4–2 Disputa por pênaltis", ""))
sf.loc[sf['Score'] == '4 – 2 Prorrogação', 'Score'] = '4 – 2'
sf = sf.apply(lambda x: x.astype(str).str.replace("pro", ""))
sf = sf.apply(lambda x: x.astype(str).str.replace(".", ""))

sepDate = sf["Date"].str.split(" ", expand=True)
sf['day'] = sepDate[0]

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
sf['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
sf['Date'] = sf.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = sf["Score"].str.split("–", n=1, expand=True)
sf['gols time 1'] = Score[0]
sf['gols time 2'] = Score[1]

###########################################################################################

sf['Match'] = sf['Date'].astype(str) + sf['Team1'] + sf['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
sf['Match-1'] = sf['Match'].shift(1)

# Comparar registros com a linha anterior
sf['Match'] = sf['Match'] == sf['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = sf['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
sf['Validação'] = match + match.shift(1)

sf['Validação'].fillna('FalseFalse', inplace=True)
mask = sf['Validação'] == 'TrueFalse'
mask2 = sf['Validação'] == 'FalseTrue'
mask3 = sf['Validação'] == 'FalseFalse'
sf.loc[mask, 'Check1'] = sf.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
sf.loc[mask2, 'Check2'] = sf.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = sf['Check1'].max()
if pd.isnull(MaxDupMatches):
    MaxDupMatches = 0
sf.loc[mask3, 'Check3'] = sf.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

sf['Partidas'] = sf['Check1'].fillna(0) + sf['Check2'].fillna(0) + sf['Check3'].fillna(0)
sf = sf.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
sf.to_sql("SemiFinal", con=engine, schema = "WorldCup", if_exists='replace')