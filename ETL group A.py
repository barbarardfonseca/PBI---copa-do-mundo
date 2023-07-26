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
        # insere todas tabelas no excel, usado pra documentação
        # globals()[f"pd{x}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{x}.xlsx")
        # globals()[f"pd{x}"].to_excel(f"C:/Users/Barbara.rohr/OneDrive/Documentos/estudos/PBI - copa do mundo/tabelas_wiki/pd{x}.xlsx")

   else:
     print(ano)
print('end loop')

# Extraído do arquivo listasDF
# contém todos os dataframes que são do grupo correspondente na copa do mundo
dfgpA = [pd193023, pd193404, pd193405, pd193808, pd193809, pd195007, pd195008, pd195009, pd195010, pd195011, pd195012, pd195427, pd195428, pd195429, pd195430, pd195813, pd195814, pd195815, pd195816, pd195817, pd195818, pd196228, pd196229, pd196230, pd196231, pd196232, pd196233, pd196613, pd196614, pd196615, pd196616, pd196617, pd196618, pd197012, pd197013, pd197014, pd197015, pd197016, pd197017, pd197405, pd197406, pd197407, pd197408, pd197409, pd197410, pd197806, pd197807, pd197808, pd197809, pd197810, pd197811, pd198205, pd198206, pd198207, pd198208, pd198209, pd198210, pd198605, pd198606, pd198607, pd198608, pd198609, pd198610, pd199005, pd199006, pd199007, pd199008, pd199009, pd199010, pd199407, pd199408, pd199409, pd199410, pd199411, pd199412, pd199805, pd199806, pd199807, pd199808, pd199809, pd199810, pd200209, pd200210, pd200211, pd200212, pd200213, pd200214, pd200656, pd201412, pd201811, pd202211]

# tratamento gpA
# cria dataframe com dados dos dataframes do grupo A e adiciona uma coluna indicando o grupo
dfA = pd.DataFrame()
for i in range(0,len(dfgpA)): 
  dfgpA[i]['Grupo'] = 'A'
  dfA = pd.concat([dfA, dfgpA[i]])
dfA.reset_index(inplace=True, drop=True)

# tratamento de dataframe do grupo A para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfA
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfA['a'] = newdata['Unnamed: 0']
dfA['b'] = newdata['Unnamed: 1']
dfA['c'] = newdata['Unnamed: 2']
dfA['d'] = newdata['Unnamed: 3']

dfA['datas'] = dfA['Unnamed: 0'].map(str) + dfA[0].map(str)
dfA['pais1'] = dfA['a'].map(str) + dfA[1].map(str)
dfA['placar'] = dfA['b'].map(str) + dfA[2].map(str)
dfA['pais2'] = dfA['c'].map(str) + dfA[3].map(str)
dfA['estadios'] = dfA['d'].map(str) + dfA[4].map(str)
# limpeza dos dados
dfA = dfA.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])
dfA.loc[dfA.placar=='nan2 – 1 (pro.)','placar']='2–1'
dfA = dfA.loc[(dfA['placar'] !=  'nannan' )]
dfA = dfA.loc[(dfA['placar'] !=  'nanRelatório' )] 
dfA = dfA.loc[(dfA['placar'] !=  'nan(Report)' )]
dfA = dfA.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfA.loc[dfA.placar=='1 (pro.)','placar']='1'
dfA.loc[dfA.datas=='Chile','datas']='19 de julho'
dfA.loc[dfA.datas=='Alemanha','datas']='9 de junho'
dfA.loc[dfA.datas=='Equador','datas']='20 de junho'
dfA.loc[dfA.datas=='Camarões','datas']='23 de junho'
dfA.loc[dfA.datas=='Uruguai','datas']='25 de junho'
dfA.loc[dfA.datas=='Catar','datas']='25 de novembro'
dfA.loc[dfA.datas=='Países Baixos','datas']='29 de novembro'
# separar gols time 1 x time 2
placar = dfA["placar"].str.split("–", n=1, expand=True)
dfA['gols time 1'] = placar[0]
dfA['gols time 2'] = placar[1]
dfA = dfA.drop(columns = ['placar'])
# formatar gols em número
dfA['gols time 1'] = dfA['gols time 1'].astype(int)
dfA['gols time 2'] = dfA['gols time 2'].astype(int)
# print(dfA)
# print(dfA['index'])

dfA.to_excel("dfA.xlsx")
# end tratamento gpA

##################################################################################

print('end')
print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
dfA.to_sql("group A", con=engine, schema = "world_cup", if_exists='replace')