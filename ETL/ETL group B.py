import pandas as pd
import datetime as dt
import numpy as np
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
ListGroupB = [pd193024, pd193406, pd193407, pd193810, pd193811, pd195013, pd195014, pd195015, pd195016, pd195017, pd195018, pd195019, pd195432, pd195433, pd195434, pd195435, pd195436, pd195813, pd195814, pd195815, pd195816, pd195817, pd195818, pd196235, pd196236, pd196237, pd196238, pd196239, pd196240, pd196613, pd196614, pd196615, pd196616, pd196617, pd196618, pd197012, pd197013, pd197014, pd197015, pd197016, pd197017, pd197412, pd197413, pd197414, pd197415, pd197416, pd197417, pd197813, pd197814, pd197815, pd197816, pd197817, pd197818, pd198212, pd198213, pd198214, pd198215, pd198216, pd198217, pd198612, pd198613, pd198614, pd198615, pd198616, pd198617, pd199012, pd199013, pd199014, pd199015, pd199016, pd199017, pd199414, pd199415, pd199416, pd199417, pd199418, pd199419, pd199812, pd199813, pd199814, pd199815, pd199816, pd199817, pd200216, pd200217, pd200218, pd200219, pd200220, pd200221, pd201414, pd201813, pd202213]

# Data preparation
# Creates dataframe with data from previously generated tables and adds a column with the group
dfB = pd.DataFrame()
for i in range(0,len(ListGroupB)): 
  ListGroupB[i]['Group'] = 'B'
  ListGroupB[i]['Stage'] = 'GroupStage'
  dfB = pd.concat([dfB, ListGroupB[i]])
dfB.reset_index(inplace=True, drop=True)
######################
NewData = dfB
NewData = NewData[NewData.index>0]
NewData.reset_index(inplace=True, drop=True)
dfB['a'] = NewData['Unnamed: 0']
dfB['b'] = NewData['Unnamed: 1']
dfB['c'] = NewData['Unnamed: 2']
dfB['d'] = NewData['Unnamed: 3']

# Concatenate columns
dfB['Date'] = dfB['Unnamed: 0'].map(str) + dfB[0].map(str)
dfB['Team1'] = dfB['a'].map(str) + dfB[1].map(str)
dfB['Score'] = dfB['b'].map(str) + dfB[2].map(str)
dfB['Team2'] = dfB['c'].map(str) + dfB[3].map(str)
dfB['Stadiums'] = dfB['d'].map(str) + dfB[4].map(str)

# Cleaning data
dfB = dfB.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 
                          0,'a', 1,'b', 2,'c', 3,'d', 4, 
                          'Pos.',	'Seleção',	'Pts',	'J',	'V',	'E',	'D',	'GP',	'GC',	'SG'])
######################
dfB.loc[dfB.Score=='nan3 – 2 (pro)','Score']='3–2'
dfB.loc[dfB.Score=='nan6 – 5 (pro.)','Score']='6–5'
dfB.loc[dfB.Score=='nan3 – 0 (pro.)','Score']='3–0'
dfB = dfB.loc[(dfB['Score'] !=  'nannan' )]
dfB = dfB.loc[(dfB['Score'] !=  'nanRelatório' )]
dfB = dfB.loc[(dfB['Score'] !=  'nanRelatório[3]' )] 
dfB = dfB.loc[(dfB['Score'] !=  'nan(Report)' )]
######################
dfB = dfB.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfB = dfB.apply(lambda x: x.astype(str).str.replace("15:00", ""))
dfB.loc[dfB.Date=='Espanha','Date']='13 de junho'
dfB.loc[dfB.index==188,'Date']='18 de junho' # repetido
dfB.loc[dfB.index==191,'Date']='23 de junho' # repetido
dfB.loc[dfB.Date=='Marrocos','Date']='15 de junho'
dfB.loc[dfB.Date=='Portugal','Date']='20 de junho'
dfB.loc[dfB.Date=='Irã','Date']='25 de junho'
dfB.loc[dfB.Date=='Inglaterra','Date']='21 de novembro'
dfB.loc[dfB.index==206,'Date']='25 de novembro' # repetido
dfB.loc[dfB.index==209,'Date']='29 de novembro' # repetido

sepDate = dfB["Date"].str.split(" ", expand=True)
dfB['day'] = sepDate[0]

# Dictionary to map month names to numbers
MonthsToNumbers = {
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

dfB['NumericMonth'] = sepDate[2].map(MonthsToNumbers)
# Substituir valores NA por zero
dfB['NumericMonth'] = dfB['NumericMonth'].fillna(0)

# Substituir infinitos por zero, se necessário (dependendo dos seus dados)
dfB['NumericMonth'] = dfB['NumericMonth'].replace([np.inf, -np.inf], 0)

# Converter os valores de 'NumericMonth' para inteiros
dfB['NumericMonth'] = dfB['NumericMonth'].astype(int)
# Concatenate date components
dfB['Date'] = dfB.apply(lambda row: '-'.join(row[['Year', 'NumericMonth','day']].astype(str)), axis=1)

# Split goals from score
Score = dfB["Score"].str.split("–", n=1, expand=True)
dfB['Team1Goals'] = Score[0]
dfB['Team2Goals'] = Score[1]

dfB['Team1Goals'] = dfB['Team1Goals'].astype(int)
dfB['Team2Goals'] = dfB['Team2Goals'].astype(int) 
############################################################################################

dfB['Match'] = dfB['Date'].astype(str) + dfB['Team1'] + dfB['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfB['Match-1'] = dfB['Match'].shift(1)

# Comparar registros com a linha anterior
dfB['Match'] = dfB['Match'] == dfB['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfB['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfB['Validação'] = match + match.shift(1)

dfB['Validação'].fillna('FalseFalse', inplace=True)
mask = dfB['Validação'] == 'TrueFalse'
mask2 = dfB['Validação'] == 'FalseTrue'
mask3 = dfB['Validação'] == 'FalseFalse'
dfB.loc[mask, 'Check1'] = dfB.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfB.loc[mask2, 'Check2'] = dfB.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfB['Check1'].max()
dfB.loc[mask3, 'Check3'] = dfB.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfB['Partidas'] = dfB['Check1'].fillna(0) + dfB['Check2'].fillna(0) + dfB['Check3'].fillna(0)
dfB = dfB.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])
# 
# Insert on database
dfB.to_sql("GroupB", con=Engine, schema = "WorldCup", if_exists='replace')
print('end')