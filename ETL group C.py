import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

# get current date
Today = dt.date.today()
CurrentYear = Today.year

# engine for the database
Engine = create_engine('postgresql+psycopg2://admin:123456@localhost:5432/postgresql_db')

# extract tables from Wikipedia
for Year in range(1930,CurrentYear,4):
   if Year not in(1942, 1946):
    globals()[f"Link{Year}"] = f"https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_{Year}"
    globals()[f"Tables{Year}"] = pd.read_html(globals()[f"Link{Year}"])
    globals()[f"QtTables{Year}"] = len(globals()[f"Tables{Year}"])
    for TableNum in range(0, globals()[f"QtTables{Year}"]):
      for z in globals()[f"Tables{Year}"]:
        globals()[f"pd{Year}{TableNum:02}"] = pd.DataFrame(globals()[f"Tables{Year}"][TableNum])
        globals()[f"pd{Year}{TableNum:02}"]["Year"] = Year
        globals()[f"pd{Year}{TableNum:02}"]["MatchNum"] = TableNum
        # insert all tables into excel, used for documentation
        # globals()[f"pd{TableNum}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{TableNum}.xlsx")
        # globals()[f"pd{TableNum}"].to_excel(f"C:/Users/Barbara.rohr/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{TableNum}.xlsx")

# Extracted from the DfLists file
# contains all dataframes that are from the corresponding group in the world cup
ListGroupC = [pd193026,pd193408,pd193409,pd193804,pd193812,pd195021,pd195022,pd195023,pd195438,pd195439,pd195440,pd195441,pd195820,pd195821,pd195822,pd195823,pd195824,pd195825,pd195826,pd196242,pd196243,pd196244,pd196245,pd196246,pd196247,pd196620,pd196621,pd196622,pd196623,pd196624,pd196625,pd197019,pd197020,pd197021,pd197022,pd197023,pd197024,pd197419,pd197420,pd197421,pd197422,pd197423,pd197424,pd197820,pd197821,pd197822,pd197823,pd197824,pd197825,pd198219,pd198220,pd198221,pd198222,pd198223,pd198224,pd198619,pd198620,pd198621,pd198622,pd198623,pd198624,pd199019,pd199020,pd199021,pd199022,pd199023,pd199024,pd199421,pd199422,pd199423,pd199424,pd199425,pd199426,pd199819,pd199820,pd199821,pd199822,pd199823,pd199824,pd200223,pd200224,pd200225,pd200226,pd200227,pd200228,pd201416,pd201815,pd202215]

##################################################################################
# Data preparation
# Creates dataframe with data from previously generated tables and adds a column with the group
dfC = pd.DataFrame()
for i in range(0,len(ListGroupC)): 
  ListGroupC[i]['Group'] = 'C'
  ListGroupC[i]['Stage'] = 'GroupStage'
  dfC = pd.concat([dfC, ListGroupC[i]])
dfC.reset_index(inplace=True, drop=True)
######################
NewData = dfC
NewData = NewData[NewData.index>0]
NewData.reset_index(inplace=True, drop=True)

dfC['a'] = NewData['Unnamed: 0']
dfC['b'] = NewData['Unnamed: 1']
dfC['c'] = NewData['Unnamed: 2']
dfC['d'] = NewData['Unnamed: 3']

# Concatenate columns
dfC['Date'] = dfC['Unnamed: 0'].map(str) + dfC[0].map(str)
dfC['Team1'] = dfC['a'].map(str) + dfC[1].map(str)
dfC['Score'] = dfC['b'].map(str) + dfC[2].map(str)
dfC['Team2'] = dfC['c'].map(str) + dfC[3].map(str)
dfC['estadios'] = dfC['d'].map(str) + dfC[4].map(str)

# Cleaning data
dfC = dfC.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 
                          0,'a', 1,'b', 2,'c', 3,'d', 4,
                          ])
######################
dfC = dfC.loc[(dfC['Score'] !=  'nannan' )]
dfC = dfC.loc[(dfC['Score'] !=  'nanRelatório' )]
dfC = dfC.loc[(dfC['Score'] !=  'nanRelatório[3]' )] 
dfC = dfC.loc[(dfC['Score'] !=  'nan(Report)' )]

dfC = dfC.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfC = dfC.apply(lambda x: x.astype(str).str.replace("(", ""))
dfC = dfC.apply(lambda x: x.astype(str).str.replace(")", ""))
dfC = dfC.apply(lambda x: x.astype(str).str.replace(" pro.", ""))

dfC.loc[dfC.Date=='Argentian','Date']='22 de novembro'
dfC.loc[dfC.index==175,'Date']='14 de junho' # repetido
dfC.loc[dfC.index==178,'Date']='19 de junho' # repetido
dfC.loc[dfC.index==184,'Date']='16 de junho'
dfC.loc[dfC.Date=='Japão','Date']='24 de junho'
dfC.loc[dfC.index==187,'Date']='21 de junho' # repetido
dfC.loc[dfC.index==190,'Date']='26 de junho' # repetido
dfC.loc[dfC.index==196,'Date']='26 de novembro ' # repetido
dfC.loc[dfC.index==199,'Date']='30 de novembro' # repetido
sepDate = dfC["Date"].str.split(" ", expand=True)
dfC['day'] = sepDate[0]

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
dfC['NumericMonth'] = sepDate[2].map(meses_para_numeros)
dfC['NumericMonth'] = dfC['NumericMonth'].fillna(0).astype(int)
dfC['Date'] = dfC.apply(lambda row: '-'.join(row[['Year', 'NumericMonth','day']].astype(str)), axis=1)

# separar gols time 1 x time 2
Score = dfC["Score"].str.split("–", n=1, expand=True)
dfC['gols time 1'] = Score[0]
dfC['gols time 2'] = Score[1]
print(Score)
print(Score[0])
print(Score[1])
# formatar gols em número
dfC['gols time 1'] = dfC['gols time 1'].astype(int)
dfC['gols time 2'] = dfC['gols time 2'].astype(int)
# end tratamento gpC

############################################################################################

dfC['Match'] = dfC['Date'].astype(str) + dfC['Team1'] + dfC['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfC['Match-1'] = dfC['Match'].shift(1)

# Comparar registros com a linha anterior
dfC['Match'] = dfC['Match'] == dfC['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfC['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfC['Validação'] = match + match.shift(1)

dfC['Validação'].fillna('FalseFalse', inplace=True)
mask = dfC['Validação'] == 'TrueFalse'
mask2 = dfC['Validação'] == 'FalseTrue'
mask3 = dfC['Validação'] == 'FalseFalse'
dfC.loc[mask, 'Check1'] = dfC.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfC.loc[mask2, 'Check2'] = dfC.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfC['Check1'].max()
dfC.loc[mask3, 'Check3'] = dfC.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfC['Partidas'] = dfC['Check1'].fillna(0) + dfC['Check2'].fillna(0) + dfC['Check3'].fillna(0)
dfC = dfC.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

dfC.to_sql("GroupC", con=Engine, schema = "WorldCup", if_exists='replace')

print('end')

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
