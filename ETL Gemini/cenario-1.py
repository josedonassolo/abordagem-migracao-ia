import pandas as pd
import psycopg2

# Estabelecer conexão com o banco de dados de origem
conn_origem = psycopg2.connect(
    host="",
    port="",
    dbname="",
    user="",
    password="",
)

# Executar consulta SQL para obter dados da tabela de origem
df = pd.read_sql_query(
    """
    SELECT "idlocalestoque" AS "nk_local_estoque", "descrlocal" AS "ds_local_estoque"
    FROM "local_estoque"
    ORDER BY "idlocalestoque";
    """,
    con=conn_origem,
)

# Fechar conexão com o banco de dados de origem
conn_origem.close()

# Estabelecer conexão com o banco de dados de destino
conn_destino = psycopg2.connect(
    host="127.0.0.1",
    port="5432",
    dbname="postgres",
    user="postgres",
    password="",
)

# Criar cursor para deletar dados existentes na tabela de destino
cur = conn_destino.cursor()

# Deletar dados existentes na tabela de destino
cur.execute("DELETE FROM dw_ia.dim_local_estoque")

# Confirmar alterações
conn_destino.commit()

# Fechar cursor
cur.close()

# Inserir dados na tabela de destino
df.to_sql("dim_local_estoque", con=conn_destino, schema="dw_ia", if_exists="append", index=False)