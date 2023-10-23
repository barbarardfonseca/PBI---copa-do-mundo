# Final
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
listFinal = [pd193033, pd193420, pd193822, pd195456, 
 pd195843, pd196263, pd196641, pd197040, 
 pd197447, pd197849, pd198267, pd198665, 
 pd199067, pd199467, pd199468, pd199879, 
 pd200282, pd200686, pd200687, pd201073, 
 pd201447, pd201846, pd202246]

Final = pd.DataFrame()
for i in range(0,len(listFinal)): 
  listFinal[i]['Grupo'] = 'Final'
  listFinal[i]['Stage'] = 'Knockout Stage'
  Final = pd.concat([Final, listFinal[i]])

Final.reset_index(inplace=True, drop=True)
Final['Date'] = Final[0].map(str)
Final['Team1'] = Final[1].map(str)
Final['Score'] = Final[2].map(str)
Final['Team2'] = Final[3].map(str)
Final = Final.drop(columns = [0, 1, 2, 3])

Final = Final.loc[(Final['Score'] !=  'Relatório' )]
Final = Final.loc[(Final['Score'] !=  'Penalidades' )]
Final = Final.loc[(Final['Score'] !=  'nan' )]
Final = Final.apply(lambda x: x.astype(str).str.replace("(", ""))
Final = Final.apply(lambda x: x.astype(str).str.replace(")", ""))
Final = Final.apply(lambda x: x.astype(str).str.replace("pro", ""))

sepDate = Final["Date"].str.split(" ", expand=True)
Final['day'] = sepDate[0]

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
Final['NumericMonth'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
Final['Date'] = Final.apply(lambda row: '-'.join(row[['Ano', 'NumericMonth','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
Score = Final["Score"].str.split("–", n=1, expand=True)
Final['gols time 1'] = Score[0]
Final['gols time 2'] = Score[1]
newdata = Final
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)
###########################################################################################

Final['Match'] = Final['Date'].astype(str) + Final['Team1'] + Final['Team2']
# Crie uma nova coluna 'MatchNum_Anterior' com os valores de 'MatchNum' deslocados uma linha para cima
Final['Match-1'] = Final['Match'].shift(1)

# Comparar registros com a linha anterior
Final['Match'] = Final['Match'] == Final['Match-1']
# Variável temporária para armazenar o valor da coluna `MatchNum`
match = Final['Match']

match = match.replace(True, 'True')
match = match.replace(False, 'False')
# Validação das partidas iguais
Final['Validação'] = match + match.shift(1)

Final['Validação'].fillna('FalseFalse', inplace=True)
mask = Final['Validação'] == 'TrueFalse'
mask2 = Final['Validação'] == 'FalseTrue'
mask3 = Final['Validação'] == 'FalseFalse'
Final.loc[mask, 'Check1'] = Final.loc[mask, 'Validação'].eq('TrueFalse').cumsum()
Final.loc[mask2, 'Check2'] = Final.loc[mask2, 'Validação'].eq('FalseTrue').cumsum()
MaxDupMatches = Final['Check1'].max()
if pd.isnull(MaxDupMatches):
    MaxDupMatches = 0
Final.loc[mask3, 'Check3'] = Final.loc[mask3, 'Validação'].eq('FalseFalse').cumsum() + MaxDupMatches

Final['Partidas'] = Final['Check1'].fillna(0) + Final['Check2'].fillna(0) + Final['Check3'].fillna(0)
Final = Final.drop(columns = ['Score','NumericMonth','day','Validação', 'Check1', 'Check2', 'Check3', 'Match', 'Match-1'])

Final.to_sql("Final", con=engine, schema = "WorldCup", if_exists='replace')
