import pandas as pd

df_dict = pd.read_excel('data.xlsx', sheet_name=None)

sheets = ['homologacion_pais','homologacion_rating','rating_empresa','rating_soberano']

for sheet in sheets:
    df = df_dict.get(sheet)
    df.to_csv('./data/'+sheet+'.csv', sep='|')

print('Check them out!')