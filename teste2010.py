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

for gr in range(1,9,1):
  qtd_tabelas2010 = len(f"tabelas2010{gr}")
  globals()[f"qtd_tabelas2010{gr}"] = len(globals()[f"tabelas2010{gr}"])
  for x in range(0, len(globals()[f"tabelas2010{gr}"])):
      #x é o número da tabela
      print(x)
      print(len(globals()[f"tabelas2010{gr}"]))
      globals()[f"pd2010{gr}{x}"] = pd.DataFrame(globals()[f"tabelas2010{gr}"][x])
      print(globals()[f"pd2010{gr}{x}"])
      df2010 = pd.concat([df2010, dfy2010[i]])

      globals()[f"pd2010{gr}{x}"].to_excel(f"C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/test2010/pd2010-{gr}-{x}.xlsx")

dfy2010 = [201011,201018,2010115,2010122,2010129,2010136,201021,201028,2010215,2010222,2010229,2010236,201031,201038,2010315,2010322,2010329,2010336,201041,201048,2010415,2010422,2010429,2010436,201052,201059,2010516,2010523,2010530,2010537,201061,201068,2010615,2010622,2010629,2010636,201071,201078,2010715,2010722,2010729,2010736,]

df2010 = pd.DataFrame()
for i in range(0,len(dfy2010)): 
  # df2010[i]['Grupo'] = 'E'
  df2010 = pd.concat(df2010[0], dfy2010[i])
# df2010.reset_index(inplace=True, drop=True)
df2010.to_sql("2010", con=engine, schema = "world_cup", if_exists='replace')
print(df2010)
# 2010-1-1
# 2010-1-8
# 2010-1-15
# 2010-1-22
# 2010-1-29
# 2010-1-36
# 2010-2-1
# 2010-2-8
# 2010-2-15
# 2010-2-22
# 2010-2-29
# 2010-2-36
# 2010-3-1
# 2010-3-8
# 2010-3-15
# 2010-3-22
# 2010-3-29
# 2010-3-36
# 2010-4-1
# 2010-4-8
# 2010-4-15
# 2010-4-22
# 2010-4-29
# 2010-4-36
# 2010-5-2
# 2010-5-9
# 2010-5-16
# 2010-5-23
# 2010-5-30
# 2010-5-37
# 2010-6-1
# 2010-6-8
# 2010-6-15
# 2010-6-22
# 2010-6-29
# 2010-6-36
# 2010-7-1
# 2010-7-8
# 2010-7-15
# 2010-7-22
# 2010-7-29
# 2010-7-36

# 2010-8-1
# 2010-8-8
# 2010-8-15
# 2010-8-22
# 2010-8-29
# 2010-8-36
