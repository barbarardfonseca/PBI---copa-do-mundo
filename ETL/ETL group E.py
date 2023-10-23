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
dfgpE = [pd198233, pd198234, pd198235, pd198236, pd198237, pd198238, pd198633, pd198634, pd198635, pd198636, pd198637, pd198638, pd199033, pd199034, pd199035, pd199036, pd199037, pd199038, pd199435, pd199436, pd199437, pd199438, pd199439, pd199440, pd199833, pd199834, pd199835, pd199836, pd199837, pd199838, pd200237, pd200238, pd200239, pd200240, pd200241, pd200242, pd200662, pd201420, pd201819, pd202219]

# tratamento gpE
# cria dataframe com dados dos dataframes do grupo E e adiciona uma coluna indicando o grupo
dfE = pd.DataFrame()
for i in range(0,len(dfgpE)): 
  dfgpE[i]['Grupo'] = 'E'
  dfgpE[i]['Stage'] = 'GroupStage'
  dfE = pd.concat([dfE, dfgpE[i]])
dfE.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo E para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfE
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfE['a'] = newdata['Unnamed: 0']
dfE['b'] = newdata['Unnamed: 1']
dfE['c'] = newdata['Unnamed: 2']
dfE['d'] = newdata['Unnamed: 3']

dfE['Date'] = dfE['Unnamed: 0'].map(str) + dfE[0].map(str)
dfE['Team1'] = dfE['a'].map(str) + dfE[1].map(str)
dfE['Score'] = dfE['b'].map(str) + dfE[2].map(str)
dfE['Team2'] = dfE['c'].map(str) + dfE[3].map(str)
dfE['estadios'] = dfE['d'].map(str) + dfE[4].map(str)

# limpeza dos dados
dfE = dfE.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfE = dfE.loc[(dfE['Score'] !=  'nannan' )]
dfE = dfE.loc[(dfE['Score'] !=  'nanRelatório' )]
dfE = dfE.loc[(dfE['Score'] !=  'nanRelatório[3]' )] 
dfE = dfE.loc[(dfE['Score'] !=  'nan(Report)' )]

dfE = dfE.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfE = dfE.apply(lambda x: x.astype(str).str.replace(".º", ""))

dfE.loc[dfE.Date=='Estados Unidos','Date']='12 de junho'
dfE.loc[dfE.index==76,'Date']='17 de junho' # repetido
dfE.loc[dfE.index==79,'Date']='22 de junho' # repetido
dfE.loc[dfE.index==82,'Date']='15 de junho' # repetido
dfE.loc[dfE.index==85,'Date']='20 de junho' # repetido
dfE.loc[dfE.Date=='Honduras','Date']='25 de junho'
dfE.loc[dfE.Date=='Costa Rica','Date']='17 de junho'
dfE.loc[dfE.Date=='Brasil','Date']='22 de junho'
dfE.loc[dfE.Date=='Sérvia','Date']='27 de junho'
dfE.loc[dfE.Date=='Alemanha','Date']='23 de novembro'
dfE.loc[dfE.index==103,'Date']='27 de novembro' # repetido
dfE.loc[dfE.index==106,'Date']='1 de dezembro' # repetido

sepDate = dfE["Date"].str.split(" ", expand=True)
dfE['day'] = sepDate[0]

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
dfE['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
dfE['Date'] = dfE.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)

# separar gols time 1 x time 2
Score = dfE["Score"].str.split("–", n=1, expand=True)
dfE['gols time 1'] = Score[0]
dfE['gols time 2'] = Score[1]
print(Score)
print(Score[0])
print(Score[1])


# formatar gols em número
dfE['gols time 1'] = dfE['gols time 1'].astype(int)
dfE['gols time 2'] = dfE['gols time 2'].astype(int)
dfE.to_excel("dfE.xlsx") 
# end tratamento gpE

############################################################################################
dfE['Match'] = dfE['Date'].astype(str) + dfE['Team1'] + dfE['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfE['Match-1'] = dfE['Match'].shift(1)

# Comparar registros com a linha anterior
dfE['Match'] = dfE['Match'] == dfE['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfE['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfE['Validação'] = match + match.shift(1)

dfE['Validação'].fillna('FalseFalse', inplace=True)
mask = dfE['Validação'] == 'TrueFalse'
mask2 = dfE['Validação'] == 'FalseTrue'
mask3 = dfE['Validação'] == 'FalseFalse'
dfE.loc[mask, 'Check1'] = dfE.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfE.loc[mask2, 'Check2'] = dfE.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfE['Check1'].max()
if pd.isnull(MaxDupMatches):
    MaxDupMatches = 0
dfE.loc[mask3, 'Check3'] = dfE.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfE['Partidas'] = dfE['Check1'].fillna(0) + dfE['Check2'].fillna(0) + dfE['Check3'].fillna(0)
dfE = dfE.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

print('end')
print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco

dfE.to_sql("GroupE", con=engine, schema = "WorldCup", if_exists='replace')