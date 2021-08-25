# pip install pyodbc
# pip install pandas
# pip install xlrd

import pyodbc
import pandas as pd

txjuros_file = 'https://www.bcb.gov.br/conteudo/txcred/Documents/taxascredito.xls' # Arquivo Fonte

df = pd.read_excel(txjuros_file, sheet_name=0, header=4, skiprows=[5], keep_default_na=False)  # Pessoa Fisica Taxas Diarias
df2 = pd.read_excel(txjuros_file, sheet_name=1, header=4, skiprows=[5], keep_default_na=False)  # Pessoa Fisica Taxas Mensais
df3 = pd.read_excel(txjuros_file, sheet_name=2, header=4, skiprows=[5], keep_default_na=False)  # Pessoa Juridica Taxas Mensais

# Renomeando as colunas de cada sheet do nosso arquivo
df.rename(columns={"POSIÇÃO": "POSICAO", "INSTITUIÇÃO FINANCEIRA": "INSTITUICAO_FINANCEIRA", "TAXAS MÉDIAS": "TAXAS_MEDIAS", "TAXAS MÉDIAS.1": "TAXAS_MEDIAS2"}, inplace=True)
df2.rename(columns={"POSIÇÃO ": "POSICAO", "INSTITUIÇÃO FINANCEIRA": "INSTITUICAO_FINANCEIRA", "TAXAS MÉDIAS": "TAXAS_MEDIAS", "TAXAS MÉDIAS.1": "TAXAS_MEDIAS2"}, inplace=True)
df3.rename(columns={"POSIÇÃO": "POSICAO", "INSTITUIÇÃO FINANCEIRA": "INSTITUICAO_FINANCEIRA", "TAXAS MÉDIAS": "TAXAS_MEDIAS", "TAXAS MÉDIAS.1": "TAXAS_MEDIAS2"}, inplace=True)

""" 1. DATA CLEAN - MODALIDADES """
modalidades_tipo1_bruto = pd.DataFrame(df['MODALIDADE'])  # Modalidades em PF-TaxasDiarias
modalidades_tipo2_bruto = pd.DataFrame(df2['MODALIDADE'])  # Modalidades em PF-TaxasMensais
modalidades_tipo3_bruto = pd.DataFrame(df3['MODALIDADE'])  # Modalidades em PJ-TaxasMensais


nan_value = float("NaN")

# Dropando linhas com string vazia e resetando o index
modalidades_tipo1_bruto.replace(' ', nan_value, inplace=True)
modalidades_tipo1_bruto.replace('', nan_value, inplace=True)
modalidades_tipo1_bruto.dropna(how='any', inplace=True)
modalidades_tipo1_bruto.reset_index(level=None, inplace=True)
modalidades_tipo1_bruto['TIPO'] = 'PF-TaxasDiarias'

modalidades_tipo2_bruto.replace(' ', nan_value, inplace=True)
modalidades_tipo2_bruto.replace('', nan_value, inplace=True)
modalidades_tipo2_bruto.dropna(how='any', inplace=True)
modalidades_tipo2_bruto.reset_index(level=None, inplace=True)
modalidades_tipo2_bruto['TIPO'] = 'PF-TaxasMensais'

modalidades_tipo3_bruto.replace(' ', nan_value, inplace=True)
modalidades_tipo3_bruto.replace('', nan_value, inplace=True)
modalidades_tipo3_bruto.dropna(how='any', inplace=True)
modalidades_tipo3_bruto.reset_index(level=None, inplace=True)
modalidades_tipo3_bruto['TIPO'] = 'PJ-TaxasMensais'

# Unindo as modalidades de todos os sheets do arquivo
merged_df = pd.concat([modalidades_tipo1_bruto, modalidades_tipo2_bruto, modalidades_tipo3_bruto])
merged_df.reset_index(level=None, inplace=True)
merged_df['ID'] = merged_df.index + 1

columns = ['ID', 'MODALIDADE', 'TIPO']

full_mod_list = merged_df[columns]
mod_data = full_mod_list.values.tolist() # -> lista final de modalidades que será enviada para o servidor

""" 1.2 DATA CLEAN - INSTITUIÇOES FINANCEIRAS """
instituicoes1 = pd.DataFrame(df['INSTITUICAO_FINANCEIRA'])  # Instituicoes
instituicoes2 = pd.DataFrame(df2['INSTITUICAO_FINANCEIRA'])  # Instituicoes
instituicoes3 = pd.DataFrame(df3['INSTITUICAO_FINANCEIRA'])  # Instituicoes

# Dropando linhas com string vazia
instituicoes1.replace(' ', nan_value, inplace=True)
instituicoes1.replace('', nan_value, inplace=True)
instituicoes1.dropna(how='any', inplace=True)

instituicoes2.replace(' ', nan_value, inplace=True)
instituicoes2.replace('', nan_value, inplace=True)
instituicoes2.dropna(how='any', inplace=True)

instituicoes3.replace(' ', nan_value, inplace=True)
instituicoes3.replace('', nan_value, inplace=True)
instituicoes3.dropna(how='any', inplace=True)

# Unindo as instituições de todos os sheets do nosso arquivo
inst_merge = pd.concat([instituicoes1, instituicoes2, instituicoes3])
inst_merge.drop_duplicates(subset=['INSTITUICAO_FINANCEIRA'], ignore_index=False, inplace=True)
inst_merge.reset_index(level=None, inplace=True)
inst_merge['ID'] = inst_merge.index + 1

inst_col = ['ID', 'INSTITUICAO_FINANCEIRA']
inst_data = inst_merge[inst_col].values.tolist() # -> lista final que será enviada ao servidor

""" 1.2 DATA CLEAN - TAXAS DE JUROS """
txcol = ["MODALIDADE", "INSTITUICAO_FINANCEIRA", "TAXAS_MEDIAS", "TAXAS_MEDIAS2"]
taxas1 = pd.DataFrame(df[txcol])  # Taxas
taxas2 = pd.DataFrame(df2[txcol])  # Taxas
taxas3 = pd.DataFrame(df3[txcol])  # Taxas

# Preenchendo linhas com colunas vazias
taxas_merge = pd.concat([taxas1, taxas2, taxas3])
taxas_merge.replace(' ', nan_value, inplace=True)
taxas_merge.replace('', nan_value, inplace=True)
taxas_merge.fillna(method='ffill', inplace=True)
taxas_merge.reset_index(level=None, inplace=True)
taxas_merge['PAIR ID'] = taxas_merge.index + 1

tx_final_col = ["MODALIDADE", "INSTITUICAO_FINANCEIRA", "TAXAS_MEDIAS", "TAXAS_MEDIAS2", "PAIR ID"]
taxas_data = taxas_merge[tx_final_col].values.tolist() # -> lista final que será enviada ao servidor


""" 2. IMPORTANDO DADOS """

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=<nomedoserver>;' # -> pode ser obtido através do comando "Select @@ServerName" no SQL Server
                      'Database=<nomedodatabase>;' 
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

# Primeira tabela, tabela de modalidades, valores são (ID da Modalidade, Modalidade, Tipo de Modalidade)
sql_insert_mod = '''
     INSERT INTO dbo.modalidades 
     VALUES (?, ?, ?)
'''
# Segunda tabela, tabela de instituicoes, valores são (ID da instituicao e nome da instituicao)
sql_insert_inst = '''
     INSERT INTO dbo.instituicoes
     VALUES (?, ?)
'''
# Terceira tabela, tabela de taxas, valores são (Nome da Modalidade, Nome da Instituicao, Taxa Anual, Taxa Mental
# e ID do Par)
sql_insert_tx = '''
     INSERT INTO dbo.taxas
     VALUES (?, ?, ?, ?, ?)
'''

cursor.executemany(sql_insert_mod, mod_data)
cursor.executemany(sql_insert_inst, inst_data)
cursor.executemany(sql_insert_tx, taxas_data)
conn.commit() # -> commit para enviar os dados para o servidor





