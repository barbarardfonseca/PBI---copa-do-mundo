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
   if Year  not in(1942, 1946):
    
    globals()[f"Link{Year}"] = f"https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_{Year}"
    globals()[f"Tables{Year}"] = pd.read_html(globals()[f"Link{Year}"])
    globals()[f"QtTables{Year}"] = len(globals()[f"Tables{Year}"])
    for TableNum in range(0, globals()[f"QtTables{Year}"]): 
      cont = 0
      for z in globals()[f"Tables{Year}"]:
     
            globals()[f"pd{Year}{TableNum:02}"] = pd.DataFrame(globals()[f"Tables{Year}"][TableNum])
            globals()[f"pd{Year}{TableNum:02}"]["Year"] = Year
            globals()[f"pd{Year}{TableNum:02}"]["TableNum"] = str(TableNum)
        # insert all tables into excel, used for documentation
        # globals()[f"pd{TableNum}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{TableNum}.xlsx")
        # globals()[f"pd{TableNum}"].to_excel(f"C:/Users/Barbara.rohr/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{TableNum}.xlsx")

# Extracted from the DfLists file
# contains all dataframes that are from the corresponding group in the world cup
ListGroupA = [pd193022, pd193404, pd193405, pd193808, pd193809, pd195007, pd195008, pd195009, pd195010, pd195011, pd195012, pd195427, pd195428, pd195429, pd195430, pd195813, pd195814, pd195815, pd195816, pd195817, pd195818, pd196228, pd196229, pd196230, pd196231, pd196232, pd196233, pd196613, pd196614, pd196615, pd196616, pd196617, pd196618, pd197012, pd197013, pd197014, pd197015, pd197016, pd197017, pd197405, pd197406, pd197407, pd197408, pd197409, pd197410, pd197806, pd197807, pd197808, pd197809, pd197810, pd197811, pd198205, pd198206, pd198207, pd198208, pd198209, pd198210, pd198605, pd198606, pd198607, pd198608, pd198609, pd198610, pd199005, pd199006, pd199007, pd199008, pd199009, pd199010, pd199407, pd199408, pd199409, pd199410, pd199411, pd199412, pd199805, pd199806, pd199807, pd199808, pd199809, pd199810, pd200209, pd200210, pd200211, pd200212, pd200213, pd200214, pd200656, pd201412, pd201811, pd202211]

##################################################################################
# Data preparation
# Creates dataframe with data from previously generated tables and adds a column with the group
dfA = pd.DataFrame()
for i in range(0,len(ListGroupA)): 
  ListGroupA[i]['Group'] = 'A'
  ListGroupA[i]['Stage'] = 'GroupStage'
  dfA = pd.concat([dfA, ListGroupA[i]])
dfA.reset_index(inplace=True, drop=True)
######################
NewData = dfA
NewData = NewData[NewData.index>0]
NewData.reset_index(inplace=True, drop=True)
dfA['a'] = NewData['Unnamed: 0']
dfA['b'] = NewData['Unnamed: 1']
dfA['c'] = NewData['Unnamed: 2']
dfA['d'] = NewData['Unnamed: 3']

# Concatenate columns
dfA['Date'] = dfA['Unnamed: 0'].map(str) + dfA[0].map(str)
dfA['Team1'] = dfA['a'].map(str) + dfA[1].map(str)
dfA['Score'] = dfA['b'].map(str) + dfA[2].map(str)
dfA['Team2'] = dfA['c'].map(str) + dfA[3].map(str)
dfA['Stadiums'] = dfA['d'].map(str) + dfA[4].map(str)

# Cleaning data
dfA = dfA.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 
                          0,'a', 1,'b', 2,'c', 3,'d', 4                      ])

######################
dfA.loc[dfA.Score=='nan2 – 1 (pro.)','Score']='2–1'
dfA = dfA.loc[(dfA['Score'] !=  'nannan' )]
dfA = dfA.loc[(dfA['Score'] !=  'nanRelatório' )] 
dfA = dfA.loc[(dfA['Score'] !=  'nan(Report)' )]
dfA.loc[dfA.Score=='1 (pro.)','Score']='1'
######################
dfA = dfA.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfA.loc[dfA.Date=='Chile','Date']='19 de julho'
dfA.loc[dfA.Date=='Alemanha','Date']='9 de junho'
dfA.loc[dfA.Date=='Equador','Date']='20 de junho'
dfA.loc[dfA.Date=='Camarões','Date']='23 de junho'
dfA.loc[dfA.Date=='Uruguai','Date']='25 de junho'
dfA.loc[dfA.Date=='Catar','Date']='25 de novembro'
dfA.loc[dfA.Date=='Países Baixos','Date']='29 de novembro'
######################

dfA['Date'] = dfA['Date'].str.replace('15:00', '')
SepDate = dfA["Date"].str.split(" ", expand=True)
dfA['day'] = SepDate[0]

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

dfA['NumericMonth'] = SepDate[2].map(MonthsToNumbers)

# Concatenate date components
dfA['Date'] = dfA.apply(lambda row: '-'.join(row[['Year', 'NumericMonth','day']].astype(str)), axis=1)

# Split goals from score
Score = dfA["Score"].str.split("–", n=1, expand=True)
dfA['Team1Goals'] = Score[0]
dfA['Team2Goals'] = Score[1]

dfA['Team1Goals'] = dfA['Team1Goals'].astype(int)
dfA['Team2Goals'] = dfA['Team2Goals'].astype(int)

##################################################################################
dfA['Match'] = dfA['Date'].astype(str) + dfA['Team1'] + dfA['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
dfA['Match-1'] = dfA['Match'].shift(1)

# Comparar registros com a linha anterior
dfA['Match'] = dfA['Match'] == dfA['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = dfA['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
dfA['Validação'] = match + match.shift(1)

dfA['Validação'].fillna('FalseFalse', inplace=True)
mask = dfA['Validação'] == 'TrueFalse'
mask2 = dfA['Validação'] == 'FalseTrue'
mask3 = dfA['Validação'] == 'FalseFalse'
dfA.loc[mask, 'Check1'] = dfA.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
dfA.loc[mask2, 'Check2'] = dfA.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = dfA['Check1'].max()
dfA.loc[mask3, 'Check3'] = dfA.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

dfA['Partidas'] = dfA['Check1'].fillna(0) + dfA['Check2'].fillna(0) + dfA['Check3'].fillna(0)
dfA = dfA.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

# Insert on database
dfA.to_sql("GroupA", con=Engine, schema = "WorldCup", if_exists='replace')
print('end')