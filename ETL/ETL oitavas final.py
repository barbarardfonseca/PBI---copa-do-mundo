
import pandas as pd
import datetime as dt
import os
from sqlalchemy import create_engine
from datetime import datetime
# aaa
# falta 2010

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
        # insere todas tabelas no excel, usado pra documentação
        # globals()[f"pd{x}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{x}.xlsx")
        # globals()[f"pd{x}"].to_excel(f"C:/Users/Barbara.rohr/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{x}.xlsx")

   else:
     print(ano)
print('end loop')

listgp81 = [pd198247, pd198248, pd198249, pd198653, pd199055, pd199452, pd199453, pd199864, pd200270, pd200668, pd200669, pd201056, pd201057, pd201428, pd201429, pd201430, pd201827, pd201828, pd202227, pd202228]
listgp82 = [pd198251, pd198252, pd198253, pd198654, pd199054, pd199454, pd199455, pd199868, pd200273, pd200672, pd200673, pd201060, pd201061, pd201434, pd201435, pd201833, pd201834, pd202231, pd202232, pd202233]
listgp83 = [pd198255, pd198256, pd198257, pd198651, pd199051, pd199457, pd199863, pd200271, pd200670, pd200671, pd201058, pd201059, pd201431, pd201432, pd201829, pd201830, pd201831, pd202229, pd202230]
listgp84 = [pd198259, pd198260, pd198261, pd198652, pd199051, pd199457, pd199863, pd200271, pd200670, pd200671, pd201058, pd201059, pd201431, pd201432, pd201829, pd201830, pd201831, pd202229, pd202230]


oitfinal = pd.DataFrame()

for value in range(81,85,1):
  for i in range(0,len(globals()[f"listgp{value}"])): 
    globals()[f"listgp{value}"][i]['Stage'] = 'RoundOf16'
    globals()[f"listgp{value}"][i]['Group'] = value - 80
    oitfinal = pd.concat([oitfinal, globals()[f"listgp{value}"][i]])
oitfinal.reset_index(inplace=True, drop=True)
oitfinal['Date'] = oitfinal[0].map(str)
oitfinal['Team1'] = oitfinal[1].map(str)
oitfinal['Score'] = oitfinal[2].map(str)
oitfinal['Team2'] = oitfinal[3].map(str)
oitfinal = oitfinal.drop(columns = [0, 1, 2, 3])
oitfinal = oitfinal.loc[(oitfinal['Score'] !=  'nan' )]
oitfinal = oitfinal.loc[(oitfinal['Score'] !=  'Relatório' )]
oitfinal = oitfinal.loc[(oitfinal['Score'] !=  'Penalidades' )]
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("(", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace(")", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("pro.", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("pro", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace(" Prorrogação4–3 Disputa por pênaltis", ""))

sepDate = oitfinal["Date"].str.split(" ", expand=True)
oitfinal['day'] = sepDate[0]

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
oitfinal['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
oitfinal['Date'] = oitfinal.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = oitfinal["Score"].str.split("–", n=1, expand=True)
oitfinal['gols time 1'] = Score[0]
oitfinal['gols time 2'] = Score[1]

###########################################################################################

oitfinal['Match'] = oitfinal['Date'].astype(str) + oitfinal['Team1'] + oitfinal['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
oitfinal['Match-1'] = oitfinal['Match'].shift(1)

# Comparar registros com a linha anterior
oitfinal['Match'] = oitfinal['Match'] == oitfinal['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = oitfinal['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
oitfinal['Validação'] = match + match.shift(1)

oitfinal['Validação'].fillna('FalseFalse', inplace=True)
mask = oitfinal['Validação'] == 'TrueFalse'
mask2 = oitfinal['Validação'] == 'FalseTrue'
mask3 = oitfinal['Validação'] == 'FalseFalse'
oitfinal.loc[mask, 'Check1'] = oitfinal.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
oitfinal.loc[mask2, 'Check2'] = oitfinal.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = oitfinal['Check1'].max()
oitfinal.loc[mask3, 'Check3'] = oitfinal.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

oitfinal['Partidas'] = oitfinal['Check1'].fillna(0) + oitfinal['Check2'].fillna(0) + oitfinal['Check3'].fillna(0)
oitfinal = oitfinal.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

oitfinal.to_sql("RoundOf16", con=engine, schema = "WorldCup", if_exists='replace')