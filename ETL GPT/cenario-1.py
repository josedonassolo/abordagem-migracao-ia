import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Informações da base de origem
source_config = {
    "host": "",
    "port": "",
    "dbname": "",
    "user": "",
    "password": ""
}

# Conexão com o banco de dados de origem
source_conn_str = f"postgresql://{source_config['user']}:{source_config['password']}@{source_config['host']}:{source_config['port']}/{source_config['dbname']}"
source_engine = create_engine(source_conn_str)

# SQL para extrair os dados
query = """
SELECT "idlocalestoque" AS "nk_local_estoque", initcap("descrlocal") AS "ds_local_estoque"
FROM "local_estoque"
"""

# Carregando dados para um DataFrame
df = pd.read_sql(query, source_engine)

### Passo 2: Conectar ao banco de destino e preparar a tabela

# Informações da base de destino
destination_config = {
    "host": "127.0.0.1",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": ""
}

# Conexão com o banco de dados de destino
destination_conn_str = f"postgresql://{destination_config['user']}:{destination_config['password']}@{destination_config['host']}:{destination_config['port']}/{destination_config['dbname']}"
destination_engine = create_engine(destination_conn_str)

# Criando conexão para usar o cursor
conn = psycopg2.connect(destination_conn_str)
cursor = conn.cursor()

# Excluindo dados existentes na tabela de destino
cursor.execute("DELETE FROM dw_ia.dim_local_estoque")
conn.commit()  # Importante para que as mudanças sejam aplicadas
cursor.close()
conn.close()

### Passo 3: Inserir os dados transformados na tabela de destino

# Inserindo dados no banco de destino
df.to_sql('dim_local_estoque', destination_engine, schema='dw_ia', if_exists='append', index=False)