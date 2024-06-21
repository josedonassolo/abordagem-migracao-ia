import pandas as pd
import psycopg2

# Conexão com a base de origem
conn_origem = psycopg2.connect(
    host="",
    port=15445,
    dbname="",
    user="",
    password="",
)

# Consulta à base de origem
query = """
SELECT
    "idproduto" AS "nk_produto",
    "descricao" AS "ds_produto",
    "embalagem" AS "ds_embalagem_produto",
    "flaginativo" = 'F' AS "bl_produto_inativo",
    "descrdivisao" AS "ds_divisao",
    "descrsecao" AS "ds_secao",
    "descrgrupo" AS "ds_grupo",
    "descrsubgrupo" AS "ds_subgrupo"
FROM "produto"
JOIN "divisao"
    ON "produto"."iddivisao" = "divisao"."iddivisao"
JOIN "secao"
    ON "produto"."idsecao" = "secao"."idsecao"
JOIN "grupo"
    ON "produto"."idgrupo" = "grupo"."idgrupo"
JOIN "subgrupo"
    ON "produto"."idsubgrupo" = "subgrupo"."idsubgrupo";
"""

# Carregamento dos dados em um DataFrame
df = pd.read_sql(query, conn_origem)

# Conexão com a base de destino
conn_destino = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="",
)

# Criação do cursor para deletar os dados existentes
cursor = conn_destino.cursor()

# Deleção dos dados existentes
cursor.execute("DELETE FROM dw_ia.dim_produto")
conn_destino.commit()

# Inserção dos dados no DataFrame na tabela de destino
df.to_sql("dim_produto", conn_destino, schema="dw_ia", if_exists="append", index=False)

# Fechamento das conexões
conn_origem.close()
conn_destino.close()