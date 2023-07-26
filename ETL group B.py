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
dfgpB = [pd193025, pd193406, pd193407, pd193810, pd193811, pd195013, pd195014, pd195015, pd195016, pd195017, pd195018, pd195019, pd195432, pd195433, pd195434, pd195435, pd195436, pd195813, pd195814, pd195815, pd195816, pd195817, pd195818, pd196235, pd196236, pd196237, pd196238, pd196239, pd196240, pd196613, pd196614, pd196615, pd196616, pd196617, pd196618, pd197012, pd197013, pd197014, pd197015, pd197016, pd197017, pd197412, pd197413, pd197414, pd197415, pd197416, pd197417, pd197813, pd197814, pd197815, pd197816, pd197817, pd197818, pd198212, pd198213, pd198214, pd198215, pd198216, pd198217, pd198612, pd198613, pd198614, pd198615, pd198616, pd198617, pd199012, pd199013, pd199014, pd199015, pd199016, pd199017, pd199414, pd199415, pd199416, pd199417, pd199418, pd199419, pd199812, pd199813, pd199814, pd199815, pd199816, pd199817, pd200216, pd200217, pd200218, pd200219, pd200220, pd200221, pd201414, pd201813, pd202213]

# tratamento gpB
# cria dataframe com dados dos dataframes do grupo B e adiciona uma coluna indicando o grupo
dfB = pd.DataFrame()
for i in range(0,len(dfgpB)): 
  dfgpB[i]['Grupo'] = 'B'
  dfB = pd.concat([dfB, dfgpB[i]])
dfB.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo B para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfB
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfB['a'] = newdata['Unnamed: 0']
dfB['b'] = newdata['Unnamed: 1']
dfB['c'] = newdata['Unnamed: 2']
dfB['d'] = newdata['Unnamed: 3']

dfB['datas'] = dfB['Unnamed: 0'].map(str) + dfB[0].map(str)
dfB['pais1'] = dfB['a'].map(str) + dfB[1].map(str)
dfB['placar'] = dfB['b'].map(str) + dfB[2].map(str)
dfB['pais2'] = dfB['c'].map(str) + dfB[3].map(str)
dfB['estadios'] = dfB['d'].map(str) + dfB[4].map(str)

# limpeza dos dados
dfB = dfB.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])
dfB = dfB.drop(columns = ['Pos.',	'Seleção',	'Pts',	'J',	'V',	'E',	'D',	'GP',	'GC',	'SG'])

dfB.loc[dfB.placar=='nan3 – 2 (pro)','placar']='3–2'
dfB.loc[dfB.placar=='nan6 – 5 (pro.)','placar']='6–5'
dfB.loc[dfB.placar=='nan3 – 0 (pro.)','placar']='3–0'
dfB = dfB.loc[(dfB['placar'] !=  'nannan' )]
dfB = dfB.loc[(dfB['placar'] !=  'nanRelatório' )]
dfB = dfB.loc[(dfB['placar'] !=  'nanRelatório[3]' )] 
dfB = dfB.loc[(dfB['placar'] !=  'nan(Report)' )]

dfB = dfB.apply(lambda x: x.astype(str).str.replace("nan", ""))

dfB.loc[dfB.datas=='Espanha','datas']='13 de junho'
dfB.loc[dfB.index==188,'datas']='18 de junho' # repetido
dfB.loc[dfB.index==191,'datas']='23 de junho' # repetido
dfB.loc[dfB.datas=='Marrocos','datas']='15 de junho'
dfB.loc[dfB.datas=='Portugal','datas']='20 de junho'
dfB.loc[dfB.datas=='Irã','datas']='25 de junho'
dfB.loc[dfB.datas=='Inglaterra','datas']='21 de novembro'
dfB.loc[dfB.index==206,'datas']='25 de novembro' # repetido
dfB.loc[dfB.index==209,'datas']='29 de novembro' # repetido
# separar gols time 1 x time 2
placar = dfB["placar"].str.split("–", n=1, expand=True)
dfB['gols time 1'] = placar[0]
dfB['gols time 2'] = placar[1]
print(placar)
print(placar[0])
print(placar[1])
dfB = dfB.drop(columns = ['placar'])
# formatar gols em número
dfB['gols time 1'] = dfB['gols time 1'].astype(int)
dfB['gols time 2'] = dfB['gols time 2'].astype(int)
dfB.to_excel("dfB.xlsx") 
# end tratamento gpB

############################################################################################

print('end')
print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco
dfB.to_sql("group B", con=engine, schema = "world_cup", if_exists='replace')