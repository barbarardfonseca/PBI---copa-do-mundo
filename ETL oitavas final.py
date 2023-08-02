
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

dfgp81 = [pd198247, pd198248, pd198249, pd198653, pd199055, pd199452, pd199453, pd199864, pd200270, pd200668, pd200669, pd201056, pd201057, pd201428, pd201429, pd201430, pd201827, pd201828, pd202227, pd202228]
dfgp82 = [pd198251, pd198252, pd198253, pd198654, pd199054, pd199454, pd199455, pd199868, pd200273, pd200672, pd200673, pd201060, pd201061, pd201434, pd201435, pd201833, pd201834, pd202231, pd202232, pd202233]
dfgp83 = [pd198255, pd198256, pd198257, pd198651, pd199051, pd199457, pd199863, pd200271, pd200670, pd200671, pd201058, pd201059, pd201431, pd201432, pd201829, pd201830, pd201831, pd202229, pd202230]
dfgp84 = [pd198259, pd198260, pd198261, pd198652, pd199051, pd199457, pd199863, pd200271, pd200670, pd200671, pd201058, pd201059, pd201431, pd201432, pd201829, pd201830, pd201831, pd202229, pd202230]


oitfinal = pd.DataFrame()

for df in range(81,85,1):
  print(df)
  for i in range(0,len(globals()[f"dfgp{df}"])):
    
    globals()[f"dfgp{df}"][i]['Fase'] = '8ª de final'
    print('a')
    globals()[f"dfgp{df}"][i]['Grupo'] = df - 80
    oitfinal = pd.concat([oitfinal, globals()[f"dfgp{df}"][i]])
oitfinal.reset_index(inplace=True, drop=True)
oitfinal['datas'] = oitfinal[0].map(str)
oitfinal['país1'] = oitfinal[1].map(str)
oitfinal['placar'] = oitfinal[2].map(str)
oitfinal['país2'] = oitfinal[3].map(str)
oitfinal = oitfinal.drop(columns = [0, 1, 2, 3])
oitfinal = oitfinal.loc[(oitfinal['placar'] !=  'nan' )]
oitfinal = oitfinal.loc[(oitfinal['datas'] !=  'nan' )]
oitfinal = oitfinal.loc[(oitfinal['placar'] !=  'Relatório' )]
oitfinal = oitfinal.loc[(oitfinal['placar'] !=  'Penalidades' )]
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("(", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace(")", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("pro.", ""))
oitfinal = oitfinal.apply(lambda x: x.astype(str).str.replace("pro", ""))
# separar gols time 1 x time 2
placar = oitfinal["placar"].str.split("–", n=1, expand=True)
oitfinal['gols time 1'] = placar[0]
oitfinal['gols time 2'] = placar[1]
oitfinal = oitfinal.drop(columns = ['placar'])

print(oitfinal)
oitfinal.to_sql("Oitavas de Final", con=engine, schema = "world_cup", if_exists='replace')