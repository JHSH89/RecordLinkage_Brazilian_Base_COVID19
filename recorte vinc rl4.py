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
data_gal = pd.read_csv('C:/Arquivos/Data linkage/data_gal.csv')

#Pré-Processamento
data_sivep['fuzzy_nome'] = clean(data_sivep['fuzzy_nome'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')
data_gal['fuzzy_nome'] = clean(data_gal['fuzzy_nome'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')
data_sivep['fuzzy_nome_mae'] = clean(data_sivep['fuzzy_nome_mae'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')
data_gal['fuzzy_nome_mae'] = clean(data_gal['fuzzy_nome_mae'], lowercase=True, replace_by_none='[^ \\-\\_A-Za-z0-9]+', replace_by_whitespace='[\\-\\_]', strip_accents=None, remove_brackets=True, encoding='utf-8', decode_error='strict')

#Indexação
indexer_neigh = recordlinkage.Index()
indexer_neigh.sortedneighbourhood(left_on='dt_nasc', right_on='dt_nascimento')
candidates_neigh = indexer_neigh.index(data_sivep, data_gal)
print(len(candidates_neigh))
print ('Número de pares indexados:', len(candidates_neigh))

#Comparação
comp = recordlinkage.Compare()
comp.string('fuzzy_nome', 'fuzzy_nome', method='jarowinkler')
comp.string('fuzzy_nome_mae', 'fuzzy_nome_mae', method='jarowinkler',missing_value=0.85)
features =  comp.compute(candidates_neigh, data_sivep, data_gal)
matches = features[features.sum(axis=1) > 1.7].reset_index()
print("len(matches)")
print(len(matches))

#Construção do DataFrame com dados dos pares
data_sivep = data_sivep.reset_index()
data_sivep_select = data_sivep[['index','nu_cpf','nm_pacient','nm_mae_pac','id_mn_resi','cs_sexo','dt_nasc','dt_sin_pri', 'dt_interna','hospital', 'evolucao', 'dt_evoluca', 'pcr_sars2', 'classi_fin', 'criterio','nu_notific','co_mun_not']]
data_gal = data_gal.reset_index()
data_gal_select = data_gal[['index','paciente','nome_mae','mun_residencia','dt_nascimento','dt_coleta','resultado']]
sivep_merge = matches.merge(data_sivep_select, how='left', left_on='level_0', right_on ='index' )
final_merge = sivep_merge.merge(data_gal_select, how='left', left_on='level_1', right_on = 'index')
print(len(final_merge))

#Classificar os pares com base nos cortes analiosados
#Percorre os pares para classificação e verificação das inconsistências
for index, fm_row in final_merge.iterrows():
    #testa se nome acima de 0,9 e soma acima de 2,65 então 1
    total = fm_row[0] + fm_row[1]
    nome = fm_row[0]
    nm_mae = fm_row[1]
    if(nm_mae==0.85):
        if (nome >=0.95):
        #print('é par')
            final_merge.loc[index, 'classif'] = '1'
        elif (nome >= 0.925 and nome < 0.95):
            #print('revisar')
            final_merge.loc[index, 'classif'] = '2'
        elif (nome > 0.88 and nome < 0.925 and fm_row['id_mn_resi'] == fm_row['mun_residencia']):
            #print('revisar')
            final_merge.loc[index, 'classif'] = '2'
        else:
            #print('Não par')
            final_merge.loc[index, 'classif'] = '0'
    else:
        if (nome >=0.9 and total >=1.8):
            final_merge.loc[index, 'classif'] = '1'
        elif (nome <0.9 and total >=1.8):
            final_merge.loc[index, 'classif'] = '3'
        elif (nome >0.85 and total <1.8 and fm_row['id_mn_resi'] == fm_row['mun_residencia']):
            final_merge.loc[index, 'classif'] = '3'
        else:
            final_merge.loc[index, 'classif'] = '0'

    #Análise de inconsistências
    if( days_between(fm_row['dt_sin_pri'],fm_row['dt_coleta']) >=90):
        final_merge.loc[index, 'inconsistencia'] = 'Exame de outro período'
    else:
        if(fm_row['resultado'] == 'Não Detectável' and fm_row['pcr_sars2'] ==''):
            final_merge.loc[index,'inconsistencia'] = 'Exame realizado resultado negativo não preenchido no sivep'
        if(fm_row['resultado'] == 'Detectável' and (fm_row['pcr_sars2']!= '1' or fm_row['classi_fin']!='5')):
            final_merge.loc[index,'inconsistencia'] = 'Exame realizado resultado positivo incoerente no sivep'
        else:
            final_merge.loc[index, 'inconsistencia'] = 'Preenchimento Consistente'

#Gera relatorio excel com pares classificados, dados dos pares para análise e inconsistências encontradas
writer = pd.ExcelWriter("C://temp/4_2.xlsx", engine='openpyxl')
final_merge.to_excel(writer, startrow=0, index=False, sheet_name='relatorio')
writer.save()
