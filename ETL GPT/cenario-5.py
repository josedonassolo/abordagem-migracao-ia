import psycopg2
from psycopg2 import sql

def connect_to_source():
    return psycopg2.connect(
        host="",
        port="",
        dbname="",
        user="",
        password=""
    )

def connect_to_destination():
    return psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        dbname="postgres",
        user="postgres",
        password=""
    )

### Passo 3: Implementar a extração de dados

def fetch_data():
    with connect_to_source() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    CONCAT(ea.idplanilha, ea.numsequencia)::numeric AS nk_produto_movimento,
                    ea.idproduto AS nk_produto,
                    ea.idempresa AS nk_empresa,
                    ea.idlocalestoque AS nk_local_estoque,
                    ea.idoperacao AS nk_operacao,
                    fp.idpessoa AS nk_pessoa,
                    ea.valtotliquido AS vl_total_liquido,
                    ea.qtdproduto AS vl_qtd_produto,
                    ea.dtmovimento AS dt_movimento
                FROM
                    estoque_analitico AS ea
                INNER JOIN financeiro_pagar AS fp ON ea.idempresa = fp.idempresa AND ea.idplanilha = fp.idplanilha
                WHERE
                    ea.dtmovimento >= CURRENT_DATE - INTERVAL '12 months'
            """)
            return cur.fetchall()

### Passo 4: Implementar a inserção incrementada dos dados

def upsert_data(data):
    with connect_to_destination() as conn:
        with conn.cursor() as cur:
            for record in data:
                cur.execute(sql.SQL("""
                    INSERT INTO dw_ia.fat_produto_movimento
                    (nk_produto_movimento, nk_produto, nk_empresa, nk_local_estoque, nk_operacao, nk_pessoa, vl_total_liquido, vl_qtd_produto, dt_movimento)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nk_produto_movimento) DO UPDATE SET
                    nk_produto = EXCLUDED.nk_produto,
                    nk_empresa = EXCLUDED.nk_empresa,
                    nk_local_estoque = EXCLUDED.nk_local_estoque,
                    nk_operacao = EXCLUDED.nk_operacao,
                    nk_pessoa = EXCLUDED.nk_pessoa,
                    vl_total_liquido = EXCLUDED.vl_total_liquido,
                    vl_qtd_produto = EXCLUDED.vl_qtd_produto,
                    dt_movimento = EXCLUDED.dt_movimento
                    WHERE dw_ia.fat_produto_movimento.dt_movimento <> EXCLUDED.dt_movimento
                """), record)
            conn.commit()


### Passo 5: Combinar tudo em uma função principal

def main():
    data = fetch_data()
    upsert_data(data)

def executar():
    if __name__ == "__main__":
        main()