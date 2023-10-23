import pandas as pd
from sqlalchemy import create_engine

# engine for the database
Engine = create_engine('postgresql+psycopg2://admin:123456@localhost:5432/postgresql_db')

url = 'https://pt.wikipedia.org/wiki/Copa_do_Mundo_FIFA'
tables = pd.read_html(url)
for i, table in enumerate(tables):
    # Reset columns to a single level
    # table.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in table.columns]
    # tabela 3 tem as sedes
    if i == 3:
        Sedes = pd.DataFrame(table)
        Sedes.columns = Sedes.iloc[0]
        # Skip the first row (header) when creating the DataFrame
        Sedes = Sedes.iloc[2:]
        
        Sedes = Sedes.dropna(axis=1)
        Sedes = Sedes.drop(columns = ['Final','Semifinalistas'])
        Sedes['Ano'] = Sedes['Ano'].str.replace('Detalhes', '')
        Sedes['Ano'] = Sedes['Ano'].str.replace('nota 3', '')
        Sedes['Ano'] = Sedes['Ano'].str.replace('[[]]', '')
        # Sedes['Sede'] = Sedes['Sede'].str.replace('Estados Unidos', 'EstadosUnidos')
        # Sedes['Sede'] = Sedes['Sede'].str.replace('Coreia do Sul', 'CoreiadoSul')
        Sedes = Sedes.loc[(Sedes['Sede'] !=  'Edições canceladas devido à Segunda Guerra Mundial.' )] 
        # Sedes = Sedes.assign(Sede=Sedes['Sede'].str.split('\n')).explode('Sede').drop_duplicates()
        
        # # Create a new DataFrame to hold the duplicated row
        # duplicated_row = Sedes.iloc[22:25].copy()
        # duplicated_row2 = Sedes.iloc[22:25].copy()
        # duplicated_row3 = Sedes.iloc[22:25].copy()
        # duplicated_row4 = Sedes.iloc[16:17].copy()
        # duplicated_row5 = Sedes.iloc[16:17].copy()

        # duplicated_row['Sede'] = duplicated_row['Sede'].str.split().str[0]
        # duplicated_row2['Sede'] = duplicated_row2['Sede'].str.split().str[1]
        # duplicated_row3['Sede'] = duplicated_row3['Sede'].str.split().str[2]
        # duplicated_row4['Sede'] = duplicated_row4['Sede'].str.split().str[0]
        # duplicated_row5['Sede'] = duplicated_row5['Sede'].str.split().str[1]
        # Sedes = pd.concat([Sedes, duplicated_row, duplicated_row2, duplicated_row3, duplicated_row4, duplicated_row5], ignore_index=True)
        # Sedes = Sedes.drop(Sedes.index[22:25])
        # Sedes = Sedes.drop(Sedes.index[16])
        # Sedes['Sede'] = Sedes['Sede'].str.replace('EstadosUnidos', 'Estados Unidos')
        # Sedes['Sede'] = Sedes['Sede'].str.replace('CoreiadoSul', 'Coreia do Sul')
        # print(Sedes)
        # ,
        # Insert on database
        Sedes.to_sql("Sedes", con=Engine, schema = "WorldCup", if_exists='replace')
