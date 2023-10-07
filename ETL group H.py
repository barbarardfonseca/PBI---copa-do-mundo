# ETL group H

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
dfgpH = [pd199854, pd199855, pd199856, pd199857, pd199858, pd199859, pd200258, pd200259, pd200260, pd200261, pd200262, pd200263, pd200666, pd201426, pd201825, pd202225]

# tratamento gpH
# cria dataframe com dados dos dataframes do grupo E e adiciona uma coluna indicando o grupo
dfH = pd.DataFrame()
for i in range(0,len(dfgpH)): 
  dfgpH[i]['Grupo'] = 'H'
  dfgpH[i]['Stage'] = 'GroupStage'
  dfH = pd.concat([dfH, dfgpH[i]])
dfH.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo E para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfH
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfH['a'] = newdata['Unnamed: 0']
dfH['b'] = newdata['Unnamed: 1']
dfH['c'] = newdata['Unnamed: 2']
dfH['d'] = newdata['Unnamed: 3']

dfH['datas'] = dfH['Unnamed: 0'].map(str) + dfH[0].map(str)
dfH['Team1'] = dfH['a'].map(str) + dfH[1].map(str)
dfH['placar'] = dfH['b'].map(str) + dfH[2].map(str)
dfH['Team2'] = dfH['c'].map(str) + dfH[3].map(str)
dfH['estadios'] = dfH['d'].map(str) + dfH[4].map(str)

# limpeza dos dados
dfH = dfH.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfH = dfH.loc[(dfH['placar'] !=  'nannan' )]
dfH = dfH.loc[(dfH['placar'] !=  'nanRelatório[3]' )] 

dfH = dfH.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("16:00", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("14:30", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("17:30", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("09:30", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("21:00", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("11:00", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("(horário local)", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))

# fazer a tipagem correta dos campos

dfH = dfH.apply(lambda x: x.astype(str).str.replace("/", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace("(", ""))
dfH = dfH.apply(lambda x: x.astype(str).str.replace(")", ""))

    
dfH.loc[dfH.datas=='Espanha','datas']='14 de junho'
dfH.loc[dfH.datas=='Colômbia','datas']='19 de junho'
dfH.loc[dfH.datas=='Japão','datas']='24 de junho'
dfH.loc[dfH.datas=='Senegal','datas']='28 de junho'
dfH.loc[dfH.datas=='Uruguai','datas']='24 de novembro'


dfH.loc[dfH.index==28,'datas']='19 de junho' # repetido
dfH.loc[dfH.index==31,'datas']='23 de junho' # repetido

dfH.loc[dfH.index==34,'datas']='17 de junho' # repetido
dfH.loc[dfH.index==37,'datas']='22 de junho' # repetido

dfH.loc[dfH.index==40,'datas']='26 de junho' # repetido
dfH.loc[dfH.index==55,'datas']='28 de novembro' # repetido

dfH.loc[dfH.index==28,'datas']='19 de junho' # repetido
dfH.loc[dfH.index==58,'datas']='02 de dezembro' # repetido

sepDate = dfH["datas"].str.split(" ", expand=True)
dfH['day'] = sepDate[0]

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
dfH['Mês_Numérico'] = sepDate[2].map(meses_para_numeros)
# Use a função apply para concatenar as colunas "teste1" a "teste5" com um espaço entre elas
dfH['datas'] = dfH.apply(lambda row: '-'.join(row[['Ano', 'Mês_Numérico','day']].astype(str)), axis=1)


# separar gols time 1 x time 2
placar = dfH["placar"].str.split("–", n=1, expand=True)
dfH['gols time 1'] = placar[0]
dfH['gols time 2'] = placar[1]
dfH = dfH.drop(columns = ['placar'])

# formatar gols em número
dfH['gols time 1'] = dfH['gols time 1'].astype(int)
dfH['gols time 2'] = dfH['gols time 2'].astype(int)


# end tratamento gpH

# ############################################################################################

# print('end')
# print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco

dfH.to_sql("GroupH", con=engine, schema = "WorldCup", if_exists='replace')
print('end')