import psycopg2
from psycopg2 import sql

# Conexão com a base de dados de origem
def connect_source():
    return psycopg2.connect(
        host="",
        port="",
        dbname="",
        user="",
        password=""
    )

# Conexão com a base de dados de destino
def connect_destination():
    return psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        dbname="postgres",
        user="postgres",
        password=""
    )

# Query para seleção de dados
query = """
SELECT
    (ea.idplanilha::varchar||ea.numsequencia::varchar)::bigint AS NK_PRODUTO_MOVIMENTO,
    idproduto AS NK_PRODUTO,
    ea.idempresa AS NK_EMPRESA,
    idlocalestoque AS NK_LOCAL_ESTOQUE,
    n.idplanilha AS NK_NOTA_FISCAL,
    idoperacao AS NK_OPERACAO,
    n.idclifor AS NK_PESSOA,
    valtotliquido AS VL_TOTAL_LIQUIDO,
    qtdproduto AS VL_QTD_PRODUTO,
    n.dtmovimento::date AS DT_MOVIMENTO
FROM
    public.estoque_analitico as ea
    join public.nota as n on (n.idplanilha = ea.idplanilha)
where ea.dtmovimento between '2023-01-01' and '2023-12-31' AND numsequencia > 0
"""

# Função para carga incremental
def incremental_load():
    source_conn = connect_source()
    dest_conn = connect_destination()

    with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
        source_cursor.execute(query)
        rows = source_cursor.fetchall()

        for row in rows:
            dest_cursor.execute(
                sql.SQL("SELECT * FROM dw_ia.fat_produto_movimento WHERE nk_produto_movimento = %s"),
                (row[0],)
            )
            result = dest_cursor.fetchone()

            if result:
                # Verificar se há mudança nos dados
                if result != row:
                    # Atualizar dados
                    dest_cursor.execute(
                        sql.SQL("""UPDATE dw_ia.fat_produto_movimento SET
                                   nk_produto = %s, nk_empresa = %s, nk_local_estoque = %s,
                                   nk_nota_fiscal = %s, nk_operacao = %s, nk_pessoa = %s,
                                   vl_total_liquido = %s, vl_qtd_produto = %s, dt_movimento = %s
                                   WHERE nk_produto_movimento = %s"""),
                        row[1:] + (row[0],)
                    )
            else:
                # Inserir novo registro
                dest_cursor.execute(
                    sql.SQL("""INSERT INTO dw_ia.fat_produto_movimento
                               (nk_produto_movimento, nk_produto, nk_empresa, nk_local_estoque, nk_nota_fiscal,
                                nk_operacao, nk_pessoa, vl_total_liquido, vl_qtd_produto, dt_movimento)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""),
                    row
                )
        dest_conn.commit()

    source_conn.close()
    dest_conn.close()


def executar():
    # Executar a função de carga incremental
    if __name__ == "__main__":
        incremental_load()
