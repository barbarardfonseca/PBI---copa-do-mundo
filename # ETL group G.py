# ETL group G

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
dfgpG = [pd199840, pd199841, pd199842, pd199843, pd199844, pd199845, pd200244, pd200245, pd200246, pd200247, pd200248, pd200249, pd201422, pd201821, pd202221]

# tratamento gpF
# cria dataframe com dados dos dataframes do grupo E e adiciona uma coluna indicando o grupo
dfG = pd.DataFrame()
for i in range(0,len(dfgpG)): 
  dfgpG[i]['Grupo'] = 'G'
  dfG = pd.concat([dfG, dfgpG[i]])
dfG.reset_index(inplace=True, drop=True)
# tratamento de dataframe do grupo E para a data da partida aparecer na mesma linha dos dados da partida

newdata = dfG
newdata = newdata[newdata.index>0]
newdata.reset_index(inplace=True, drop=True)

dfG['a'] = newdata['Unnamed: 0']
dfG['b'] = newdata['Unnamed: 1']
dfG['c'] = newdata['Unnamed: 2']
dfG['d'] = newdata['Unnamed: 3']

dfG['datas'] = dfG['Unnamed: 0'].map(str) + dfG[0].map(str)
dfG['pais1'] = dfG['a'].map(str) + dfG[1].map(str)
dfG['placar'] = dfG['b'].map(str) + dfG[2].map(str)
dfG['pais2'] = dfG['c'].map(str) + dfG[3].map(str)
dfG['estadios'] = dfG['d'].map(str) + dfG[4].map(str)

# limpeza dos dados
dfG = dfG.drop(columns = ['Unnamed: 0', 'Unnamed: 1','Unnamed: 2','Unnamed: 3', 0,'a', 1,'b', 2,'c', 3,'d', 4])

dfG = dfG.loc[(dfG['placar'] !=  'nannan' )]
dfG = dfG.loc[(dfG['placar'] !=  'nanRelatório' )]
dfG = dfG.loc[(dfG['placar'] !=  'nanRelatório[3]' )] 

dfG = dfG.apply(lambda x: x.astype(str).str.replace("nan", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("21:15", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("14:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("17:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("21:00", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("09:30", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("12:30", ""))
# dfG = dfG.apply(lambda x: x.astype(str).str.replace("17:30()/12:30()", ""))


dfG = dfG.apply(lambda x: x.astype(str).str.replace("16:00", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("Histórico", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário local)", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("()/()", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace(")", ""))
dfG = dfG.apply(lambda x: x.astype(str).str.replace("(horário do Brasil)", ""))





dfG.loc[dfG.datas=='Argentian','datas']='21 de junho'
dfG.loc[dfG.datas=='Nigéria','datas']='25 de junho'
dfG.loc[dfG.datas=='Coreia do Sul','datas']='23 de junho'
dfG.loc[dfG.datas=='México','datas']='27 de junho'
dfG.loc[dfG.datas=='Marrocos','datas']='23 de novembro'
dfG.loc[dfG.datas=='Bélgica','datas']='27 de novembro'
dfG.loc[dfG.datas=='Croácia','datas']='1.º de dezembro'

# separar gols time 1 x time 2
placar = dfG["placar"].str.split("–", n=1, expand=True)
dfG['gols time 1'] = placar[0]
dfG['gols time 2'] = placar[1]
dfG = dfG.drop(columns = ['placar'])

# formatar gols em número
dfG['gols time 1'] = dfG['gols time 1'].astype(int)
dfG['gols time 2'] = dfG['gols time 2'].astype(int)
# dfG.to_excel("dfG.xlsx") 
# end tratamento gpF

# ############################################################################################

# print('end')
# print(datetime.now())

# depois de tudo pronto talvez eu junte todos os grupos. Inserir os dataframes resultantes no banco

dfG.to_sql("group G", con=engine, schema = "world_cup", if_exists='replace')
print('end')