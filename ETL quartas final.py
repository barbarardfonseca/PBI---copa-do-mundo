# aaa
# falta 2010
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
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

List4F1 = [pd193416, pd193816, pd193817, pd193818, pd195452, pd195838, 
           pd196258, pd196637, pd197035, pd197036, pd197433, pd197434,
           pd197435, pd197436, pd197437, pd197438, pd197835, pd197836,
           pd197837, pd197838, pd197839, pd197840, pd198659, pd198660, 
           pd198661, pd199057, pd199058, pd199059, pd199462, pd199463, 
           pd199872, pd199873, pd200275, pd200276, pd200277, pd200677, 
           pd200678, pd200679, pd201065, pd201066, pd201067, pd201438, 
           pd201439, pd201838, pd201839, pd202237, pd202238, pd202239]

List4F2 = [pd193414, pd193415, pd193814, pd193815, pd195450, pd195451, 
           pd195839, pd196259, pd196635, pd196636, pd197033, pd197034, 
           pd197440, pd197441, pd197442, pd197443, pd197444, pd197445, 
           pd197842, pd197843, pd197844, pd197845, pd197846, pd197847, 
           pd198655, pd198656, pd198657, pd198658, pd199060, pd199061, 
           pd199461, pd199874, pd200278, pd200680, pd200681, pd200682, 
           pd201068, pd201069, pd201440, pd201441, pd201442, pd201840, 
           pd201841, pd201842, pd202241, pd202242] 

QuartFinal = pd.DataFrame()

for value in range(1,2,1):
  print(value)
  for i in range(0,len(globals()[f"List4F{value}"])):
    
    globals()[f"List4F{value}"][i]['Fase'] = '4ª de final'
    print('a')
    globals()[f"List4F{value}"][i]['Grupo'] = value
    QuartFinal = pd.concat([QuartFinal, globals()[f"List4F{value}"][i]])
QuartFinal.reset_index(inplace=True, drop=True)
# QuartFinal['datas'] = QuartFinal[0].map(str)
# QuartFinal['país1'] = QuartFinal[1].map(str)
# QuartFinal['placar'] = QuartFinal[2].map(str)
# QuartFinal['país2'] = QuartFinal[3].map(str)
# QuartFinal = QuartFinal.drop(columns = [0, 1, 2, 3])
# QuartFinal = QuartFinal.loc[(QuartFinal['placar'] !=  'nan' )]
# QuartFinal = QuartFinal.loc[(QuartFinal['datas'] !=  'nan' )]
# QuartFinal = QuartFinal.loc[(QuartFinal['placar'] !=  'Relatório' )]
# QuartFinal = QuartFinal.loc[(QuartFinal['placar'] !=  'Penalidades' )]
# QuartFinal = QuartFinal.apply(lambda x: x.astype(str).str.replace("(", ""))
# QuartFinal = QuartFinal.apply(lambda x: x.astype(str).str.replace(")", ""))
# QuartFinal = QuartFinal.apply(lambda x: x.astype(str).str.replace("pro.", ""))
# QuartFinal = QuartFinal.apply(lambda x: x.astype(str).str.replace("pro", ""))
# separar gols time 1 x time 2
# placar = QuartFinal["placar"].str.split("–", n=1, expand=True)
# QuartFinal['gols time 1'] = placar[0]
# QuartFinal['gols time 2'] = placar[1]
# QuartFinal = QuartFinal.drop(columns = ['placar'])

print(QuartFinal)
QuartFinal.to_sql("Quartas de Final", con=engine, schema = "world_cup", if_exists='replace')