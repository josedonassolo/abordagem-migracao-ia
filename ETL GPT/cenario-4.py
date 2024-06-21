import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Definição das credenciais de conexão do banco de dados de origem
origem_db_config = {
    "host": "",
    "port": "",
    "dbname": "",
    "user": "",
    "password": ""
}

# Definição das credenciais de conexão do banco de dados de destino
destino_db_config = {
    "host": "127.0.0.1",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": ""
}

# SQL para extrair dados
sql_query = """
SELECT 
    idproduto AS nk_produto,
    initcap(descricao) AS ds_produto,
    embalagem AS ds_embalagem_produto,
    flaginativo::boolean AS bl_produto_inativo,
    initcap(d.descrdivisao) AS ds_divisao,
    initcap(s.descrsecao) AS ds_secao,
    initcap(g.descrgrupo) AS ds_grupo,
    initcap(sg.descrsubgrupo) AS ds_subgrupo
FROM 
    produto p
JOIN divisao d ON p.iddivisao = d.iddivisao
JOIN secao s ON p.idsecao = s.idsecao
JOIN grupo g ON p.idgrupo = g.idgrupo
JOIN subgrupo sg ON p.idsubgrupo = sg.idsubgrupo;
"""

# Conectar com o banco de dados de origem e extrair dados
conn_origem = psycopg2.connect(**origem_db_config)
df = pd.read_sql_query(sql_query, conn_origem)
conn_origem.close()

# Conectar com o banco de dados de destino
conn_destino = psycopg2.connect(**destino_db_config)
cursor = conn_destino.cursor()

# Limpar dados existentes na tabela dim_produto
cursor.execute("DELETE FROM dw_ia.dim_produto;")
conn_destino.commit()

# Inserir dados na tabela dim_produto usando pandas e sqlalchemy
engine = create_engine(f"postgresql://{destino_db_config['user']}:{destino_db_config['password']}@{destino_db_config['host']}:{destino_db_config['port']}/{destino_db_config['dbname']}")
df.to_sql('dim_produto', engine, schema='dw_ia', if_exists='append', index=False)

# Fechar conexões
cursor.close()
conn_destino.close()
engine.dispose()

print("Dados transferidos com sucesso!")
