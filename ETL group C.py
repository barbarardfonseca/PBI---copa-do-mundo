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
dfgpC = [pd193027,pd193408,pd193409,pd193812,pd195021,pd195022,pd195023,pd195438,pd195439,pd195440,pd195441,pd195820,pd195821,pd195822,pd195823,pd195824,pd195825,pd195826,pd196242,pd196243,pd196244,pd196245,pd196246,pd196247,pd196620,pd196621,pd196622,pd196623,pd196624,pd196625,pd197019,pd197020,pd197021,pd197022,pd197023,pd197024,pd197419,pd197420,pd197421,pd197422,pd197423,pd197424,pd197820,pd197821,pd197822,pd197823,pd197824,pd197825,pd198219,pd198220,pd198221,pd198222,pd198223,pd198224,pd198619,pd198620,pd198621,pd198622,pd198623,pd198624,pd199019,pd199020,pd199021,pd199022,pd199023,pd199024,pd199421,pd199422,pd199423,pd199424,pd199425,pd199426,pd199819,pd199820,pd199821,pd199822,pd199823,pd199824,pd200223,pd200224,pd200225,pd200226,pd200227,pd200228,pd201416,pd201815,pd202215]

# tratamento gpC
# cria dataframe com dados dos dataframes do grupo C e adiciona uma coluna indicando o grupo
dfC = pd.DataFrame()
for i in range(0,len(dfgpC)): 
  dfgpC[i]['Grupo'] = 'C'
  dfC = pd.concat([dfC, dfgpC[i]])
dfC.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo C para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfC
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfC['a'] = newdata['Unnamed: 0']
dfC['b'] = newdata['Unnamed: 1']
dfC['c'] = newdata['Unnamed: 2']
dfC['d'] = newdata['Unnamed: 3']

dfC['datas'] = dfC['Unnamed: 0'].map(str) + dfC[0].map(str)
dfC['pais1'] = dfC['a'].map(str) + dfC[1].map(str)
dfC['placar'] = dfC['b'].map(str) + dfC[2].map(str)
dfC['pais2'] = dfC['c'].map(str) + dfC[3].map(str)
dfC['estadios'] = dfC['d'].map(str) + dfC[4].map(str)

# limpeza dos dados
dfC = dfC.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfC = dfC.loc[(dfC['placar'] !=  'nannan' )]
dfC = dfC.loc[(dfC['placar'] !=  'nanRelatório' )]
dfC = dfC.loc[(dfC['placar'] !=  'nanRelatório[3]' )] 
dfC = dfC.loc[(dfC['placar'] !=  'nan(Report)' )]

dfC = dfC.apply(lambda x: x.astype(str).str.replace("nan", ""))

dfC.loc[dfC.datas=='Argentian','datas']='22 de novembro'
dfC.loc[dfC.index==173,'datas']='14 de junho' # repetido
dfC.loc[dfC.index==176,'datas']='19 de junho' # repetido
dfC.loc[dfC.datas=='França','datas']='16 de junho'
dfC.loc[dfC.datas=='Japão','datas']='24 de junho'
dfC.loc[dfC.index==185,'datas']='21 de junho' # repetido
dfC.loc[dfC.index==188,'datas']='26 de junho' # repetido
dfC.loc[dfC.index==194,'datas']='26 de novembro ' # repetido
dfC.loc[dfC.index==197,'datas']='30 de novembro' # repetido
# separar gols time 1 x time 2
placar = dfC["placar"].str.split("–", n=1, expand=True)
dfC['gols time 1'] = placar[0]
dfC['gols time 2'] = placar[1]
print(placar)
print(placar[0])
print(placar[1])
dfC = dfC.drop(columns = ['placar'])
# formatar gols em número
dfC['gols time 1'] = dfC['gols time 1'].astype(int)
dfC['gols time 2'] = dfC['gols time 2'].astype(int)
dfC.to_excel("dfC.xlsx") 
# end tratamento gpC

############################################################################################

dfC.to_sql("group C", con=engine, schema = "world_cup", if_exists='replace')

print('end')
print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
