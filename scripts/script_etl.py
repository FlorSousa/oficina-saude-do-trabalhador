#importações das bibliotecas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from urllib.parse import quote_plus as urlquote
import os
import pandas as pd
import datetime


#dicionario com informacoes dos estados
codigos_estados = {
    0: {"uf": '0', "nome":"NÃO INFORMADO" , "regiao": "NÃO INFORMADO"},
    11: {"uf": 'RO', "nome":"RONDÔNIA" ,"regiao": "NORTE"}, 12: {"uf": 'AC', "nome":"ACRE" , "regiao": "NORTE"}, 13: {"uf": 'AM', "nome":"AMAZONIA" , "regiao": "NORTE"},
    14: {"uf": 'RR', "nome":"RORAIMA" , "regiao": "NORTE"}, 15: {"uf": 'PA', "nome":"PARÁ" , "regiao": "NORTE"}, 16: {"uf": 'AP', "nome":"AMAPÁ" ,"regiao": "NORTE"},
    17: {"uf": 'TO', "nome":"TOCANTINS" , "regiao": "NORTE"},
    21: {"uf": 'MA',"nome":"MARANHÃO" , "regiao": "NORDESTE"}, 22: {"uf": 'PI', "nome":"PIAUI" ,"regiao": "NORDESTE"},
    23: {"uf": 'CE', "nome":"CEARÁ" ,"regiao": "NORDESTE"}, 24: {"uf": 'RN',"nome":"RIO GRANDE DO NORTE" , "regiao": "NORDESTE"},
    25: {"uf": 'PB', "nome":"PARAÍBA" , "regiao": "NORDESTE"}, 26: {"uf": 'PE', "nome":"PERNANBUCO" , "regiao": "NORDESTE"},
    27: {"uf": 'AL', "nome":"ALAGOAS" , "regiao": "NORDESTE"}, 28: {"uf": 'SE', "nome":"SERGIPE" , "regiao": "NORDESTE"},
    29: {"uf": 'BA', "nome":"BAHIA" ,"regiao": "NORDESTE"},
    31: {"uf": 'MG', "nome":"MINAS GERAIS" , "regiao": "SUDESTE"}, 32: {"uf": 'ES', "nome":"ESPÍRITO SANTOS" , "regiao": "SUDESTE"}, 33: {"uf": 'RJ', "nome":"RIO DE JANEIRO" , "regiao": "SUDESTE"},
    35: {"uf": 'SP', "nome":"SÃO PAULO" , "regiao": "SUDESTE"},
    41: {"uf": 'PR', "nome":"PARANÁ" ,"regiao": "SUL"}, 42: {"uf": 'SC', "nome":"SANTA CATARINA" , "regiao": "SUL"}, 43: {"uf": 'RS', "nome":"RIO GRANDE DO SUL" ,"regiao": "SUL"},
    50: {"uf": 'MS', "nome":"MATO GROSSO DO SUL" ,"regiao": "CENTRO-OESTE"}, 51: {"uf": 'MT', "nome":"MATO GROSSO" , "regiao": "CENTRO-OESTE"},
    52: {"uf": 'GO', "nome":"GOIÁS" ,"regiao": "CENTRO-OESTE"}, 53: {"uf": 'DF', "nome":"DISTRITO FEDERAL" , "regiao": "CENTRO-OESTE"}
}


#conectadno o banco de dados 
def inicializa_bd():
    url = "postgresql://%s:%s@%s" % (os.environ.get('POSTGRES_USER'), urlquote(os.environ.get('POSTGRES_PASSWORD')),
                                         os.environ.get('POSTGRES_HOST') + ":5432/" + os.environ.get('POSTGRES_DB'))
        
    if not database_exists(url):
        create_database(url)

    engine = create_engine(url,pool_size = 50, echo=False)

    return engine

#criada para não passar varios parametros toda vez as informacoes do banco


#criada para ler o arquivo csv
def ler_arquivo_csv(arq):
    arquivo = pd.read_csv( arq, sep = ';',low_memory=False)
    df = pd.DataFrame(arquivo)
    print(f'Tamanho do arquivo {len(df.index)}')

    return df

#criação das dimensões 
def dim_data(engine):
    #Utilizando pela a ORM SQLALchemy
    try:
        print('criando dimensao data')
        conn = engine.connect()
        trans = conn.begin() 
        conn.execute('''DROP TABLE IF EXISTS dim_data''')

    except Exception as e:
        print(f"finalizado com erro dim_data: {e}") 

    finally:
        conn.execute('''CREATE TABLE dim_data as SELECT DISTINCT SUBSTR(dt_notific, 7, 4)::int AS ano, SUBSTR(dt_notific, 4, 2)::int AS mes
                        FROM srag
                        ORDER BY mes;''')
        trans.commit()
        print('finalizado com sucesso dim_data')

    trans.close()
  
def dim_sexo(engine):
    
    try:
        print('criando dimensao sexo')
        conn = engine.connect()
        trans = conn.begin() 
        conn.execute('''DROP TABLE IF EXISTS dim_sexo''')

    except Exception as e:
        print(f"finalizado com erro dim_sexo: {e}") 

    finally:
        conn.execute('''CREATE TABLE dim_sexo as SELECT DISTINCT cs_sexo AS sexo
                        FROM srag;''')
        trans.commit()
        print('finalizado com sucesso dim_sexo')

    trans.close()

def dim_ocupacao(engine):
    #Utilizando o pandas
    try:
        print('criando dimensao ocupacao')
        query = 'SELECT DISTINCT pac_cocbo , lower(pac_dscbo) FROM srag;'
        cbo_srag = pd.read_sql_query(query, engine)
        print(cbo_srag)
        df3 = pd.DataFrame(cbo_srag)
        df3.rename(columns={'pac_cocbo': 'CODIGO', 'lower':'TITULO'}, inplace=True)
        

        df_saude = pd.read_csv("arquivos/cbo.csv", encoding="UTF-8", dtype={'CODIGO': object}, delimiter=";", index_col=None)
        

        dataset = df3.merge(df_saude, on=['CODIGO','TITULO'], how='outer')
        dataset["CODIGO"].fillna('0', inplace= True)
        dataset["TITULO"].fillna('não informado', inplace= True)
        dataset["PROFISSIONAL"].fillna(False, inplace= True)

        dataset =  dataset.drop_duplicates()
        dataset.columns = [col.lower() for col in dataset.columns]
        
    except Exception as e:
        print(f"finalizado com erro dim_ocupacao: {e}")

    finally:
        #to_sql ele envia os dados do csv para o BD, possuindo assim um autocommit    
        dataset.to_sql('dim_ocupacao', con = engine,index=False, if_exists='replace')
        print('finalizado com sucesso dim_ocupacao')

def dim_estado(engine):
    try:
        print('criando dimensao estado')
        query = 'SELECT DISTINCT sg_uf as uf FROM srag;'
        estado = pd.read_sql_query(query, engine)
            

        df3 = pd.DataFrame(codigos_estados)
        df3 = df3.transpose()
        df3["codigo_ibge"] = df3.index

        df_uf = df3.merge(estado, on=['uf'], how='outer')
        df_uf = df_uf.dropna()

    except Exception as e:
        print(f"finalizado com erro dim_estado: {e}")

    finally:    
        df_uf.to_sql('dim_estado', con = engine,index=False, if_exists='replace')
        print('finalizado com sucesso dim_estado')
    
#tabela fato e mini ETL
def tabela_fato(engine):
        query1 = '''  with notificacoes as (
            SELECT SUBSTR("dt_notific", 7, 4)::int  ano,SUBSTR("dt_notific", 4, 2)::int mes, srag.cs_sexo  sexo,dim_estado.codigo_ibge codigo_uf, srag.pac_cocbo  ocupacao, count(*) notificacoes
            FROM srag
            left JOIN dim_data ON dim_data.ano =SUBSTR(dt_notific, 7, 4)::int AND 
                        dim_data.mes = SUBSTR(dt_notific, 4, 2)::int
            left JOIN dim_sexo ON (srag.cs_sexo = dim_sexo.sexo)
            left JOIN dim_estado ON (srag.sg_uf = dim_estado.uf)
            left JOIN dim_ocupacao ON (srag.pac_cocbo::VARCHAR = dim_ocupacao.codigo)
                WHERE "classi_fin" = 5    
            GROUP BY SUBSTR("dt_notific", 7, 4)::int  ,SUBSTR("dt_notific", 4, 2)::int, srag.cs_sexo ,srag.pac_cocbo,codigo_uf
            ) select notificacoes.*  from notificacoes
            
            '''
        query2 = ''' with internacoes AS( 
            SELECT COALESCE(SUBSTR(dt_interna, 7, 4),SUBSTR(dt_notific, 7, 4))::int  ano, COALESCE(SUBSTR(dt_interna, 4, 2),SUBSTR(dt_notific, 4, 2))::int mes, srag.cs_sexo  sexo, dim_estado.codigo_ibge codigo_uf, srag.pac_cocbo  ocupacao,count(*) internacoes
            FROM srag
            left JOIN dim_data ON dim_data.ano = COALESCE(SUBSTR(dt_interna, 7, 4),SUBSTR(dt_notific, 7, 4))::int AND 
                        dim_data.mes = COALESCE(SUBSTR(dt_interna, 4, 2),SUBSTR(dt_notific, 4, 2))::int 
            LEFT JOIN dim_sexo ON (srag.cs_sexo = dim_sexo.sexo)
            LEFT JOIN dim_estado ON (srag.sg_uf = dim_estado.uf)
            LEFT JOIN dim_ocupacao ON (srag.pac_cocbo::VARCHAR = dim_ocupacao.codigo)
            WHERE "classi_fin" = 5 
                AND "hospital" = 1
            GROUP BY   COALESCE(SUBSTR(dt_interna, 7, 4),SUBSTR(dt_notific, 7, 4))::int,COALESCE(SUBSTR(dt_interna, 4, 2),SUBSTR(dt_notific, 4, 2))::int,  srag.cs_sexo ,srag.pac_cocbo,codigo_uf
            )select internacoes.*  from internacoes
        '''
        query3 =  ''' with internacoes_entrada_uti AS (
            SELECT COALESCE(SUBSTR(dt_entuti, 7, 4),SUBSTR(dt_notific, 7, 4))::int  ano, COALESCE(SUBSTR(dt_entuti, 4, 2),SUBSTR(dt_notific, 4, 2))::int mes,  srag.cs_sexo  sexo, dim_estado.codigo_ibge codigo_uf,srag.pac_cocbo  ocupacao,count(*) internacoes_entrada_uti 
            FROM srag
            left JOIN dim_data ON dim_data.ano = COALESCE(SUBSTR(dt_entuti, 7, 4),SUBSTR(dt_notific, 7, 4))::int AND 
                        dim_data.mes = COALESCE(SUBSTR(dt_entuti, 4, 2),SUBSTR(dt_notific, 4, 2))::int
            LEFT JOIN dim_sexo ON (srag.cs_sexo = dim_sexo.sexo)
            LEFT JOIN dim_estado ON (srag.sg_uf = dim_estado.uf)
            LEFT JOIN dim_ocupacao ON (srag.pac_cocbo::VARCHAR = dim_ocupacao.codigo)
            WHERE classi_fin = 5  
                AND  uti = 1       
            GROUP BY  COALESCE(SUBSTR(dt_entuti, 7, 4),SUBSTR(dt_notific, 7, 4))::int, COALESCE(SUBSTR(dt_entuti, 4, 2),SUBSTR(dt_notific, 4, 2))::int , srag.cs_sexo,srag.sg_uf ,srag.pac_cocbo,codigo_uf
        )  select internacoes_entrada_uti.*  from internacoes_entrada_uti'''

        query4 =''' with internacoes_saida_uti AS (
            SELECT COALESCE(SUBSTR(dt_saiduti, 7, 4),SUBSTR(dt_notific, 7, 4))::int ano, COALESCE(SUBSTR(dt_saiduti, 4, 2),SUBSTR(dt_notific, 4, 2))::int mes, srag.cs_sexo  sexo, dim_estado.codigo_ibge codigo_uf,srag.pac_cocbo  ocupacao,count(*) internacoes_saida_uti
            FROM srag
            LEFT JOIN dim_data ON dim_data.ano = COALESCE(SUBSTR(dt_saiduti, 7, 4),SUBSTR(dt_notific, 7, 4))::int  AND 
                    dim_data.mes =  COALESCE(SUBSTR(dt_saiduti, 4, 2),SUBSTR(dt_notific, 4, 2))::int
            LEFT JOIN dim_sexo ON (srag.cs_sexo = dim_sexo.sexo)
            LEFT JOIN dim_estado ON (srag.sg_uf = dim_estado.uf)
            LEFT JOIN dim_ocupacao ON (srag.pac_cocbo::VARCHAR = dim_ocupacao.codigo)
            WHERE classi_fin = 5  
                AND  evolucao = 2
            GROUP BY COALESCE(SUBSTR(dt_saiduti, 7, 4),SUBSTR(dt_notific, 7, 4))::int, COALESCE(SUBSTR(dt_saiduti, 4, 2),SUBSTR(dt_notific, 4, 2))::int , srag.cs_sexo ,srag.pac_cocbo,codigo_uf
        )select internacoes_saida_uti.*  from internacoes_saida_uti'''

        query5= '''with obitos AS (
            SELECT COALESCE(SUBSTR(dt_evoluca, 7, 4),SUBSTR(dt_notific, 7, 4))::int  ano, COALESCE(SUBSTR(dt_evoluca, 4, 2),SUBSTR(dt_notific, 4, 2))::int mes,   
            srag.cs_sexo  sexo,dim_estado.codigo_ibge codigo_uf, srag.pac_cocbo  ocupacao,count(*) obitos
            FROM srag 
            left JOIN dim_data ON dim_data.ano = COALESCE(SUBSTR(dt_evoluca, 7, 4),SUBSTR(dt_notific, 7, 4))::int AND 
                        dim_data.mes = COALESCE(SUBSTR(dt_evoluca, 4, 2),SUBSTR(dt_notific, 4, 2))::int 
            left JOIN dim_sexo ON (srag.cs_sexo = dim_sexo.sexo)
            left JOIN dim_estado ON (srag.sg_uf = dim_estado.uf)
            left JOIN dim_ocupacao ON (srag.pac_cocbo::VARCHAR = dim_ocupacao.codigo)
            WHERE "classi_fin" = 5 
                AND  "evolucao" = 2
            GROUP BY COALESCE(SUBSTR(dt_evoluca, 7, 4),SUBSTR(dt_notific, 7, 4))::int,COALESCE(SUBSTR(dt_evoluca, 4, 2),SUBSTR(dt_notific, 4, 2))::int, srag.cs_sexo,srag.pac_cocbo ,codigo_uf
            )
            select obitos.*  from obitos'''

        x1 = pd.read_sql_query(query1, engine)
        x2 = pd.read_sql_query(query2, engine)
        x3= pd.read_sql_query(query3, engine)
        x4= pd.read_sql_query(query4, engine)
        x5 = pd.read_sql_query(query5, engine)
        fato = pd.concat([x1,x2,x3,x4,x5])

        fato["sexo"] = fato["sexo"].replace('M','1').replace('F','2').replace('I','3').fillna('0').astype(int)
        fato["ano"] = fato["ano"].fillna('0').astype(int)
        fato["mes"] = fato["mes"].fillna('0').astype(int)
        fato['internacoes'] = fato['internacoes'].fillna(0).astype(int)
        fato['internacoes_entrada_uti'] = fato['internacoes_entrada_uti'].fillna('0').astype(int)
        fato['internacoes_saida_uti'] = fato['internacoes_saida_uti'].fillna(0).astype(int)
        fato['obitos'] =fato['obitos'].fillna(0).astype(int)
        fato['ocupacao'] =fato['ocupacao'].fillna('0')
        fato['notificacoes'] =fato['notificacoes'].fillna(0).astype(int)
        print(fato)
        print(fato['notificacoes'].sum())
        print(fato['internacoes'].sum())
        print(fato['obitos'].sum())
        print(fato['internacoes_entrada_uti'].sum())
        print(fato[fato['ano'] == 0].sum())
        print(fato['ano'].unique())

        data_atual = datetime.datetime.now()
        ano_atual = data_atual.date().year

        df_remove =fato.loc[(fato['ano'] > ano_atual) |  (fato['ano'] < 2020)]
        
        fato.drop(df_remove.index, inplace =True)
        fato.to_sql('tabela_fato', con = engine,index=False, if_exists='replace')

#feito para chamar as outras funções 
def main():
  
    engine = inicializa_bd()
    df_2022 = ler_arquivo_csv('arquivos/INFLUD22-25-07-2022.csv')
    print(df_2022)
    df_2022.columns = [col.lower() for col in df_2022.columns]
    #print(df_2022)
    df_2022.to_sql('srag', con = engine,index=False, if_exists='replace')
    dim_data(engine)
    dim_sexo(engine)
    dim_ocupacao(engine)
    dim_estado(engine)
    tabela_fato(engine)

    
   
    
if __name__ == '__main__': #Só executa a função principal se o script não estiver sendo exportado em outro arquivo
    main() # chamada da função main
