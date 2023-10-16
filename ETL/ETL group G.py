# ETL group G

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
dfgpG = [pd199840, pd199841, pd199842, pd199843, pd199844, pd199845, pd200244, pd200245, pd200246, pd200247, pd200248, pd200249, pd201422, pd201821, pd202221]

# tratamento gpF
# cria dataframe com dados dos dataframes do grupo E e adiciona uma coluna indicando o grupo
dfG = pd.DataFrame()
for i in range(0,len(dfgpG)): 
  dfgpG[i]['Grupo'] = 'G'
  dfgpG[i]['Stage'] = 'GroupStage'
  dfG = pd.concat([dfG, dfgpG[i]])
dfG.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo E para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfG
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfG['a'] = newdata['Unnamed: 0']
dfG['b'] = newdata['Unnamed: 1']
dfG['c'] = newdata['Unnamed: 2']
dfG['d'] = newdata['Unnamed: 3']

dfG['Date'] = dfG['Unnamed: 0'].map(str) + dfG[0].map(str)
dfG['Team1'] = dfG['a'].map(str) + dfG[1].map(str)
dfG['Score'] = dfG['b'].map(str) + dfG[2].map(str)
dfG['Team2'] = dfG['c'].map(str) + dfG[3].map(str)
dfG['estadios'] = dfG['d'].map(str) + dfG[4].map(str)

# limpeza dos dados
dfG = dfG.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfG = dfG.loc[(dfG['Score'] !=  'nannan' )]
dfG = dfG.loc[(dfG['Score'] !=  'nanRelatório' )]
dfG = dfG.loc[(dfG['Score'] !=  'nanRelatório[3]' )] 

dfG = dfG.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("21:15", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("14:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("17:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("21:00", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("09:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("12:30", ""))
# dfG = dfG.apply(lambda x: x.astype(str).str.replace("17:30()/12:30()", ""))


dfG = dfG.apply(lambda x: x.astype(str).str.replace("16:00", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("Histórico", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário local)", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("()/()", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace(")", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace(".º", ""))




dfG.loc[dfG.Date=='Argentian','Date']='21 de junho'
dfG.loc[dfG.Date=='Nigéria','Date']='25 de junho'
dfG.loc[dfG.Date=='Coreia do Sul','Date']='23 de junho'
dfG.loc[dfG.Date=='México','Date']='27 de junho'
dfG.loc[dfG.Date=='Marrocos','Date']='23 de novembro'
dfG.loc[dfG.Date=='Bélgica','Date']='27 de novembro'
dfG.loc[dfG.Date=='Croácia','Date']='1 de dezembro'

sepDate = dfG["Date"].str.split(" ", expand=True)
dfG['day'] = sepDate[0]

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
dfG['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
dfG['Date'] = dfG.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = dfG["Score"].str.split("–", n=1, expand=True)
dfG['gols time 1'] = Score[0]
dfG['gols time 2'] = Score[1]

# formatar gols em número
dfG['gols time 1'] = dfG['gols time 1'].astype(int)
dfG['gols time 2'] = dfG['gols time 2'].astype(int)
# dfG.to_excel("dfG.xlsx") 
# end tratamento gpF

# ############################################################################################
dfG['Match'] = dfG['Date'].astype(str) + dfG['Team1'] + dfG['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfG['Match-1'] = dfG['Match'].shift(1)

# Comparar registros com a linha anterior
dfG['Match'] = dfG['Match'] == dfG['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfG['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfG['Validação'] = match + match.shift(1)

dfG['Validação'].fillna('FalseFalse', inplace=True)
mask = dfG['Validação'] == 'TrueFalse'
mask2 = dfG['Validação'] == 'FalseTrue'
mask3 = dfG['Validação'] == 'FalseFalse'
dfG.loc[mask, 'Check1'] = dfG.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfG.loc[mask2, 'Check2'] = dfG.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfG['Check1'].max()
dfG.loc[mask3, 'Check3'] = dfG.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfG['Partidas'] = dfG['Check1'].fillna(0) + dfG['Check2'].fillna(0) + dfG['Check3'].fillna(0)
dfG = dfG.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco

dfG.to_sql("GroupG", con=engine, schema = "WorldCup", if_exists='replace')
print('end')