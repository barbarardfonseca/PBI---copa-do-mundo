import pandas as pd
import datetime as dt
import os
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine('postgresql+psycopg2://admin:123456@localhost:5432/postgresql_db')

# aaa
# falta 2010
tabelas20101 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_A')
tabelas20102 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_B')
tabelas20103 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_C')
tabelas20104 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_D')
tabelas20105 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_E')
tabelas20106 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_F')
tabelas20107 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_G')
tabelas20108 = pd.read_html('https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA_de_2010_%E2%80%93_Grupo_H')
df2010 = pd.DataFrame()

# print('aaaa')
for gr in range(1,9,1):
  qtd_tabelas2010 = len(globals()[f"tabelas2010{gr}"])
  globals()[f"qtd_tabelas2010{gr}"] = len(globals()[f"tabelas2010{gr}"])
  
  for x in range(0, len(globals()[f"tabelas2010{gr}"])):
      #x é o número da tabela
      # print(f"pd2010{gr}{x}")
      # print(len(globals()[f"tabelas2010{gr}"]))
      globals()[f"pd2010{gr}{x}"] = pd.DataFrame(globals()[f"tabelas2010{gr}"][x])
      globals()[f"pd2010{gr}{x}"]['Grupo'] = gr
      # print(globals()[f"pd2010{gr}{x}"])

      # globals()[f"pd2010{gr}{x}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/test2010/pd2010-{gr}-{x}.xlsx")

dfy2010 = [pd201011, pd201016, pd2010111, pd2010116, pd2010121, pd2010126, 
           pd201021, pd201026, pd2010211, pd2010216, pd2010221, pd2010226, 
           pd201031, pd201036, pd2010311, pd2010316, pd2010321, pd2010326, 
           pd201041, pd201046, pd2010411, pd2010416, pd2010421, pd2010426, 
           pd201052, pd201057, pd2010512, pd2010517, pd2010522, pd2010527, 
           pd201061, pd201066, pd2010611, pd2010616, pd2010621, pd2010626, 
           pd201071, pd201076, pd2010611, pd2010616, pd2010721, pd2010726, 
           pd201081, pd201086, pd2010811, pd2010816, pd2010821, pd2010826
           ]
# print(pd201011)
df2010 = pd.DataFrame()
for i in range(0,len(dfy2010)): 
     # print(i)
     # print(dfy2010[i])
     # df2010[i]['Grupo'] = 'E'
     df2010 = pd.concat([df2010, dfy2010[i]])
df2010['datas'] = df2010[0].map(str)
df2010['pais1'] = df2010[1].map(str)
df2010['placar'] = df2010[2].map(str)
df2010['pais2'] = df2010[3].map(str)
df2010['estadios'] = df2010[4].map(str)
df2010 = df2010.loc[(df2010['placar'] !=  'Relátorio' )] 
df2010 = df2010.loc[(df2010['placar'] !=  'Relatório' )] 
# separar gols time 1 x time 2
placar = df2010["placar"].str.split("–", n=1, expand=True)
df2010['gols time 1'] = placar[0]
df2010['gols time 2'] = placar[1]
df2010 = df2010.drop(columns = [0, 1, 2, 3, 4, 'placar'])
df2010.loc[df2010.Grupo==1,'Grupo']='A'
df2010.loc[df2010.Grupo==2,'Grupo']='B'
df2010.loc[df2010.Grupo==3,'Grupo']='C'
df2010.loc[df2010.Grupo==4,'Grupo']='D'
df2010.loc[df2010.Grupo==5,'Grupo']='E'
df2010.loc[df2010.Grupo==6,'Grupo']='F'
df2010.loc[df2010.Grupo==7,'Grupo']='G'
df2010.loc[df2010.Grupo==8,'Grupo']='H'
df2010.reset_index(inplace=True, drop=True)
df2010.to_sql("2010", con=engine, schema = "world_cup", if_exists='replace')
# df2010.to_excel("C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/test2010/df2010.xlsx")

#print(df2010)