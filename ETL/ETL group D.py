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
dfgpD = [pd193028, pd193410, pd193411,  pd193813, pd195025, pd195443,  pd195444, pd195445, pd195446,  pd195447, pd195828, pd195829,  pd195830, pd195831, pd195832,  pd195833, pd195834, pd196249,  pd196250, pd196251, pd196252,  pd196253, pd196254, pd196627,  pd196628, pd196629, pd196630,  pd196631, pd196632, pd197026,  pd197027, pd197028, pd197029,  pd197030, pd197031, pd197425,  pd197426, pd197427, pd197428,  pd197429, pd197430, pd197431,  pd197827, pd197828, pd197829,  pd197830, pd197831, pd197832,  pd198226, pd198227, pd198228,  pd198229, pd198230,  pd200232, pd200233, pd200234, pd200235, pd200660, pd201418, pd201817, pd202217]

# tratamento gpD
# cria dataframe com dados dos dataframes do grupo D e adiciona uma coluna indicando o grupo
dfD = pd.DataFrame()
for i in range(0,len(dfgpD)): 
  dfgpD[i]['Grupo'] = 'D'
  dfgpD[i]['Stage'] = 'GroupStage'
  dfD = pd.concat([dfD, dfgpD[i]])
dfD.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo D para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfD
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfD['a'] = newdata['Unnamed: 0']
dfD['b'] = newdata['Unnamed: 1']
dfD['c'] = newdata['Unnamed: 2']
dfD['d'] = newdata['Unnamed: 3']

dfD['Date'] = dfD['Unnamed: 0'].map(str) + dfD[0].map(str)
dfD['Team1'] = dfD['a'].map(str) + dfD[1].map(str)
dfD['Score'] = dfD['b'].map(str) + dfD[2].map(str)
dfD['Team2'] = dfD['c'].map(str) + dfD[3].map(str)
dfD['estadios'] = dfD['d'].map(str) + dfD[4].map(str)

# limpeza dos dados
dfD = dfD.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])
dfD = dfD.drop(columns = ['Pos',	'Seleção',	'Pts',	'J',	'V',	'E',	'D',	'GM',	'GS',	'DG'])
dfD.loc[dfD.Score=='nan3 - 1','Score']='3–1'

dfD.loc[dfD.Score=='nan4 – 4 (pro.)','Score']='4–4'
dfD.loc[dfD.Score=='nan3 – 0 (pro.)','Score']='3–0'
dfD = dfD.loc[(dfD['Score'] !=  'nannan' )]
dfD = dfD.loc[(dfD['Score'] !=  'nanRelatório' )]
dfD = dfD.loc[(dfD['Score'] !=  'nanRelatório[3]' )] 
dfD = dfD.loc[(dfD['Score'] !=  'nan(Report)' )]

dfD = dfD.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfD = dfD.apply(lambda x: x.astype(str).str.replace("15:00", ""))

dfD.loc[dfD.Date=='México','Date']='11 de junho'
dfD.loc[dfD.index==154,'Date']='26 de novembro' # repetido
dfD.loc[dfD.index==157,'Date']='30 de novembro' # repetido
dfD.loc[dfD.Date=='Portugal','Date']='21 de junho'
dfD.loc[dfD.Date=='Uruguai','Date']='14 de junho'
dfD.loc[dfD.Date=='Itália','Date']='24 de junho'
dfD.loc[dfD.Date=='Argentian','Date']='16 de junho'
dfD.loc[dfD.Date=='Nigéria','Date']='26 de junho'
dfD.loc[dfD.Date=='Dinamarca','Date']='22 de novembro'
sepDate = dfD["Date"].str.split(" ", expand=True)
dfD['day'] = sepDate[0]

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
dfD['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
dfD['Date'] = dfD.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = dfD["Score"].str.split("–", n=1, expand=True)
dfD['gols time 1'] = Score[0]
dfD['gols time 2'] = Score[1]
print(Score)
print(Score[0])
print(Score[1])

# formatar gols em número
dfD['gols time 1'] = dfD['gols time 1'].astype(int)
dfD['gols time 2'] = dfD['gols time 2'].astype(int)
# end tratamento gpD

###########################################################################################

dfD['Match'] = dfD['Date'].astype(str) + dfD['Team1'] + dfD['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfD['Match-1'] = dfD['Match'].shift(1)

# Comparar registros com a linha anterior
dfD['Match'] = dfD['Match'] == dfD['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfD['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfD['Validação'] = match + match.shift(1)

dfD['Validação'].fillna('FalseFalse', inplace=True)
mask = dfD['Validação'] == 'TrueFalse'
mask2 = dfD['Validação'] == 'FalseTrue'
mask3 = dfD['Validação'] == 'FalseFalse'
dfD.loc[mask, 'Check1'] = dfD.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfD.loc[mask2, 'Check2'] = dfD.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfD['Check1'].max()
if pd.isnull(MaxDupMatches):
    MaxDupMatches = 0
dfD.loc[mask3, 'Check3'] = dfD.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfD['Partidas'] = dfD['Check1'].fillna(0) + dfD['Check2'].fillna(0) + dfD['Check3'].fillna(0)
dfD = dfD.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

dfD.to_sql("GroupD", con=engine, schema = "WorldCup", if_exists='replace')
print('end')
print(datetime.now())

