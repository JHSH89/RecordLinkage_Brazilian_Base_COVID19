import pandas as pd
import datetime
from datetime import datetime
from recordlinkage.preprocessing import clean, phonetic
from recordlinkage.index import SortedNeighbourhood
from recordlinkage.index import Block
from recordlinkage.index import Full
import recordlinkage
import recordlinkage as rl

# Função days between
def days_between(d1, d2):
    return abs((d2 - d1).days)

#Os dados do SIVEP-Gripe são lidos de um arquivo CSV
data_sivep = pd.read_csv('C:/Arquivos/Data linkage/data_sivep.csv')

#Pré Processamento

s= pd.Series(data_sivep['fuzzy_nome'])
data_sivep['fuzzy_nome'] = clean(data_sivep['fuzzy_nome'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')
s= pd.Series(data_sivep['fuzzy_nome_mae'])
data_sivep['fuzzy_nome_mae'] = clean(data_sivep['fuzzy_nome_mae'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')

#Indexação
indexer_neigh = recordlinkage.Index()
indexer_neigh.sortedneighbourhood(left_on='dt_nasc')
candidates_neigh = indexer_neigh.index(data_sivep)
print ('Número de pares candidatos:', len(candidates_neigh))


#Conmparação
comp = recordlinkage.Compare()

# initialise similarity measurement algorithms
comp.string('fuzzy_nome','fuzzy_nome', method='jarowinkler')
comp.string('fuzzy_nome_mae','fuzzy_nome_mae', method='jarowinkler', missing_value=0.85)
comp.exact('nu_cpf','nu_cpf',missing_value=0.85)
# the method .compute() returns the DataFrame with the feature vectors.
features =  comp.compute(candidates_neigh, data_sivep)
print(features.info)
matches = features[features.sum(axis=1) > 2.4].reset_index()
print(matches.info)
print("len(matches)")
print(len(matches))


#Construção do DataFrame com dados dos pares
data_sivep = data_sivep.reset_index()
data_sivep_select = data_sivep[['index','nu_cpf','nm_pacient','nm_mae_pac','id_mn_resi','cs_sexo','dt_nasc','dt_sin_pri', 'dt_interna','hospital', 'evolucao', 'dt_evoluca', 'pcr_sars2', 'classi_fin', 'criterio','nu_notific','co_mun_not']]
sivep_merge = matches.merge(data_sivep_select, how='left', left_on='level_0', right_on ='index' )
final_merge8 = sivep_merge.merge(data_sivep_select, how='left', left_on='level_1', right_on = 'index')
final_merge8 = final_merge8.rename(columns={"0": "similaridade"})
print(len(final_merge8))
matches8 = matches[['level_0','level_1']]

#Classificar os pares com base nos cortes analiosados
#Percorre os pares para classificação e verificação das inconsistências
for index, fm_row in final_merge8.iterrows():
    #testa se nome acima de 0,9 e soma acima de 2,65 então 1
    total = fm_row[0] + fm_row[1] + fm_row[2]
    nome = fm_row[0]

    if (nome >=0.9 and total >=2.65):
        #print('é par')
        final_merge8.loc[index, 'classif'] = '1'
    elif (nome < 0.9 and total >= 2.65):
        #print('revisar')
        final_merge8.loc[index, 'classif'] = '2'
    elif (nome >= 0.85 and total <= 2.65):
        #print('revisar')
        final_merge8.loc[index, 'classif'] = '2'
    else:
        #print('Não par')
        final_merge8.loc[index, 'classif'] = '0'

    #Análise de inconsistências
    if( days_between(fm_row['dt_sin_pri_x'],fm_row['dt_sin_pri_y']) >=90):
        final_merge8.loc[index, 'inconsistencia'] = 'Reinfecção'
    else:
        if(fm_row['classi_fin_x'] == fm_row['classi_fin_y']):
            final_merge8.loc[index,'inconsistencia'] = 'Duplicidade mesma classificação final'
        else:
            final_merge8.loc[index, 'inconsistencia'] = 'Duplicidade classificação final diferente'

writer = pd.ExcelWriter("C://temp/RL_1.xlsx", engine='openpyxl')
final_merge8.to_excel(writer, startrow=0, index=False, sheet_name='relatorio')
#Gera relatorio com pares classificados, dados dos pares para análise e inconsistências encontradas
writer.save()