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
dfgpD = [pd193029, pd193410, pd193411,  pd193813, pd195025, pd195443,  pd195444, pd195445, pd195446,  pd195447, pd195828, pd195829,  pd195830, pd195831, pd195832,  pd195833, pd195834, pd196249,  pd196250, pd196251, pd196252,  pd196253, pd196254, pd196627,  pd196628, pd196629, pd196630,  pd196631, pd196632, pd197026,  pd197027, pd197028, pd197029,  pd197030, pd197031, pd197425,  pd197426, pd197427, pd197428,  pd197429, pd197430, pd197431,  pd197827, pd197828, pd197829,  pd197830, pd197831, pd197832,  pd198226, pd198227, pd198228,  pd198229, pd198230,  pd200232, pd200233, pd200234, pd200235, pd200660, pd201418, pd201817, pd202217]

# tratamento gpD
# cria dataframe com dados dos dataframes do grupo D e adiciona uma coluna indicando o grupo
dfD = pd.DataFrame()
for i in range(0,len(dfgpD)): 
  dfgpD[i]['Grupo'] = 'D'
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

dfD['datas'] = dfD['Unnamed: 0'].map(str) + dfD[0].map(str)
dfD['pais1'] = dfD['a'].map(str) + dfD[1].map(str)
dfD['placar'] = dfD['b'].map(str) + dfD[2].map(str)
dfD['pais2'] = dfD['c'].map(str) + dfD[3].map(str)
dfD['estadios'] = dfD['d'].map(str) + dfD[4].map(str)

# limpeza dos dados
dfD = dfD.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])
dfD = dfD.drop(columns = ['Pos',	'Seleção',	'Pts',	'J',	'V',	'E',	'D',	'GM',	'GS',	'DG'])
dfD.loc[dfD.placar=='nan3 - 1','placar']='3–1'

dfD.loc[dfD.placar=='nan4 – 4 (pro.)','placar']='4–4'
# dfD.loc[dfD.placar=='nan3 – 0 (pro.)','placar']='3–0'
dfD = dfD.loc[(dfD['placar'] !=  'nannan' )]
dfD = dfD.loc[(dfD['placar'] !=  'nanRelatório' )]
dfD = dfD.loc[(dfD['placar'] !=  'nanRelatório[3]' )] 
dfD = dfD.loc[(dfD['placar'] !=  'nan(Report)' )]

dfD = dfD.apply(lambda x: x.astype(str).str.replace("nan", ""))

dfD.loc[dfD.datas=='México','datas']='11 de junho'
dfD.loc[dfD.index==154,'datas']='26 de novembro' # repetido
dfD.loc[dfD.index==157,'datas']='30 de novembro' # repetido
dfD.loc[dfD.datas=='Portugal','datas']='21 de junho'
dfD.loc[dfD.datas=='Uruguai','datas']='14 de junho'
dfD.loc[dfD.datas=='Itália','datas']='24 de junho'
dfD.loc[dfD.datas=='Argentian','datas']='16 de junho'
dfD.loc[dfD.datas=='Nigéria','datas']='26 de junho'
dfD.loc[dfD.datas=='Dinamarca','datas']='22 de novembro'
# separar gols time 1 x time 2
placar = dfD["placar"].str.split("–", n=1, expand=True)
dfD['gols time 1'] = placar[0]
dfD['gols time 2'] = placar[1]
print(placar)
print(placar[0])
print(placar[1])
dfD = dfD.drop(columns = ['placar'])

# formatar gols em número
dfD['gols time 1'] = dfD['gols time 1'].astype(int)
dfD['gols time 2'] = dfD['gols time 2'].astype(int)
dfD.to_excel("dfD.xlsx") 
# end tratamento gpD

###########################################################################################
dfD.to_sql("group D", con=engine, schema = "world_cup", if_exists='replace')
print('end')
print(datetime.now())

