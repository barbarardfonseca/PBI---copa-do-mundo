# ETL group F

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


# Extraído do arquivo listasDF
# contém todos os dataframes que são do grupo correspondente na copa do mundo
dfgpF =[pd198240, pd198241, pd198242, pd198243, pd198244, pd198245, pd198640, pd198641, pd198642, pd198643, pd198644, pd198645, pd199040, pd199041, pd199042, pd199043, pd199044, pd199045, pd199442, pd199443, pd199444, pd199445, pd199446, pd199447, pd199840, pd199841, pd199842, pd199843, pd199844, pd199845, pd200244, pd200245, pd200246, pd200247, pd200248, pd200249, pd201422, pd201821, pd202221]

# tratamento gpF
# cria dataframe com dados dos dataframes do grupo E e adiciona uma coluna indicando o grupo
dfF = pd.DataFrame()
for i in range(0,len(dfgpF)): 
  dfgpF[i]['Grupo'] = 'F'
  dfgpF[i]['Stage'] = 'GroupStage'
  dfF = pd.concat([dfF, dfgpF[i]])
dfF.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo E para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfF
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfF['a'] = newdata['Unnamed: 0']
dfF['b'] = newdata['Unnamed: 1']
dfF['c'] = newdata['Unnamed: 2']
dfF['d'] = newdata['Unnamed: 3']

dfF['Date'] = dfF['Unnamed: 0'].map(str) + dfF[0].map(str)
dfF['Team1'] = dfF['a'].map(str) + dfF[1].map(str)
dfF['Score'] = dfF['b'].map(str) + dfF[2].map(str)
dfF['Team2'] = dfF['c'].map(str) + dfF[3].map(str)
dfF['estadios'] = dfF['d'].map(str) + dfF[4].map(str)

# limpeza dos dados
dfF = dfF.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfF = dfF.loc[(dfF['Score'] !=  'nannan' )]
dfF = dfF.loc[(dfF['Score'] !=  'nanRelatório' )]
dfF = dfF.loc[(dfF['Score'] !=  'nanRelatório[3]' )] 

dfF = dfF.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("21:15", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("14:30", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("17:30", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("21:00", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("09:30", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("12:30", ""))
# dfF = dfF.apply(lambda x: x.astype(str).str.replace("17:30()/12:30()", ""))


dfF = dfF.apply(lambda x: x.astype(str).str.replace("16:00", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("Histórico", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("(horário local)", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("()/()", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("(", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace(")", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))
dfF = dfF.apply(lambda x: x.astype(str).str.replace(".º", ""))

dfF.loc[dfF.Date=='Argentian','Date']='21 de junho'
dfF.loc[dfF.Date=='Nigéria','Date']='25 de junho'
dfF.loc[dfF.Date=='Coreia do Sul','Date']='23 de junho'
dfF.loc[dfF.Date=='México','Date']='27 de junho'
dfF.loc[dfF.Date=='Marrocos','Date']='23 de novembro'
dfF.loc[dfF.Date=='Bélgica','Date']='27 de novembro'
dfF.loc[dfF.Date=='Croácia','Date']='1 de dezembro'

sepDate = dfF["Date"].str.split(" ", expand=True)
dfF['day'] = sepDate[0]

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
dfF['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
dfF['Date'] = dfF.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = dfF["Score"].str.split("–", n=1, expand=True)
dfF['gols time 1'] = Score[0]
dfF['gols time 2'] = Score[1]
print(Score)
print(Score[0])
print(Score[1])

# formatar gols em número
dfF['gols time 1'] = dfF['gols time 1'].astype(int)
dfF['gols time 2'] = dfF['gols time 2'].astype(int)
# end tratamento gpF

# ############################################################################################
dfF['Match'] = dfF['Date'].astype(str) + dfF['Team1'] + dfF['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfF['Match-1'] = dfF['Match'].shift(1)

# Comparar registros com a linha anterior
dfF['Match'] = dfF['Match'] == dfF['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfF['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfF['Validação'] = match + match.shift(1)

dfF['Validação'].fillna('FalseFalse', inplace=True)
mask = dfF['Validação'] == 'TrueFalse'
mask2 = dfF['Validação'] == 'FalseTrue'
mask3 = dfF['Validação'] == 'FalseFalse'
dfF.loc[mask, 'Check1'] = dfF.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfF.loc[mask2, 'Check2'] = dfF.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfF['Check1'].max()
if pd.isnull(MaxDupMatches):
    MaxDupMatches = 0
dfF.loc[mask3, 'Check3'] = dfF.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfF['Partidas'] = dfF['Check1'].fillna(0) + dfF['Check2'].fillna(0) + dfF['Check3'].fillna(0)
dfF = dfF.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco

dfF.to_sql("GroupF", con=engine, schema = "WorldCup", if_exists='replace')
print('end')