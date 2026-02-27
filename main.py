from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool = None
try:
    pool = SimpleConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), cursor_factory=RealDictCursor
    )
except psycopg2.OperationalError as e:
    print(f"ERRO CRÍTICO: Falha ao inicializar o pool de conexões. {e}")


@app.get("/")
def health_check():
    return {"status": "ok", "api": "Carteira e Fluxo Projetado/Realizado/Fee"}


# ==========================================
# ROTA 1: CARTEIRA PROJEÇÃO
# ==========================================
@app.get("/carteira_projecao")
def obter_carteira_projecao(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                WITH
                    cte_taxas AS (
                        SELECT
                            id_fundo,
                            valor
                        FROM
                            tb_taxas_adicionais
                        WHERE
                            id_taxa = 6
                    ),
                    cte_primeiro_boleto AS (
                        SELECT
                            fundo_id,
                            MIN(dt_vencimento_original) AS primeiro_boleto
                        FROM
                            tb_ordem
                        WHERE
                            fl_ativo IS TRUE
                        GROUP BY
                            fundo_id
                    )
                SELECT
                    u.nm_unidade AS "FRANQUIA",
                    f.id AS "COD FUNDO",
                    f.nm_fundo AS "FUNDO",
                    f.vl_orcamento_contrato AS "MAF INICIAL",
                    (ta.valor / NULLIF(f.vl_orcamento_contrato, 0)) AS "%% FEE/MAF",
                    ta.valor AS "FEE INICIAL",
                    CASE
                        WHEN f.dt_contrato IS NULL
                        OR f.dt_contrato > f.dt_cadastro THEN f.dt_cadastro
                        ELSE f.dt_contrato
                    END AS "DATA DE CONTRATO DO FUNDO",
                    pb.primeiro_boleto AS "PRIMEIRO BOLETO",
                    f.dt_baile AS "DATA BAILE",
                    ROUND(SUM(
                        CASE 
                            WHEN (i.fl_ativo IS TRUE OR i.fl_ativo IS NULL) 
                             AND (i.nu_status NOT IN (11, 9, 8, 13) OR i.nu_status IS NULL) 
                            THEN fc.vl_plano 
                            ELSE 0 
                        END
                    )::numeric, 2) AS "VVR ATUAL"
                FROM
                    tb_fundo f
                    INNER JOIN tb_unidade u ON u.id = f.unidade_id
                    INNER JOIN tb_integrante i ON i.fundo_id = f.id
                    INNER JOIN tb_fundo_cota fc ON fc.cota_id = i.cota_id
                    AND i.fundo_id = fc.fundo_id
                    LEFT JOIN cte_taxas ta ON ta.id_fundo = f.id
                    LEFT JOIN cte_primeiro_boleto pb ON pb.fundo_id = f.id
                WHERE
                    u.categoria = '2'
                    AND f.tipocliente_id IN (15,17)
                    AND f.is_fundo_teste IS FALSE
                    AND f.is_assessoria_pura IS FALSE
                    AND f.fl_ativo IS TRUE
                    AND f.situacao NOT IN (10,11)
                GROUP BY
                    u.nm_unidade,
                    f.id,
                    f.nm_fundo,
                    f.vl_orcamento_contrato,
                    ta.valor,
                    f.dt_cadastro,
                    f.dt_contrato,
                    pb.primeiro_boleto,
                    f.dt_baile
                ORDER BY 
                    f.id
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        print(f"Erro na query carteira_projecao: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)


# ==========================================
# ROTA 2: CARTEIRA REALIZADO
# ==========================================
@app.get("/carteira_realizado")
def obter_carteira_realizado(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                WITH
                    -- CTEs da Consulta Original (Taxas e Primeiro Boleto)
                    cte_taxas AS (
                        SELECT
                            id_fundo,
                            valor
                        FROM
                            tb_taxas_adicionais
                        WHERE
                            id_taxa = 6
                    ),
                    cte_dados_ordem AS (
                        SELECT
                            fundo_id,
                            MIN(dt_vencimento_original) AS primeiro_boleto
                        FROM
                            tb_ordem
                        WHERE
                            fl_ativo IS TRUE
                        GROUP BY
                            fundo_id
                    ),
                    
                    -- CTEs da Nova Lógica SPDX
                    -- Soma de Ordens (Base para o Saldo SPDX) -> CRÉDITO
                    cte_ordem AS (
                        SELECT
                            t0.fundo_id,
                            SUM(t0.vl_pago) AS vl_pago_spdx_base
                        FROM
                            tb_ordem t0
                        WHERE
                            t0.fl_ativo = TRUE
                            AND t0.tipo_conta_fundo = 'SPDX'
                            AND t0.dt_liquidacao IS NOT NULL
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    tb_integrante_financeiro t2
                                WHERE
                                    t2.ordem_id = t0.id
                                    AND t2.tipocobranca_id = 12
                            )
                        GROUP BY
                            t0.fundo_id
                    ),
                    -- Soma de Requisições (Compensadas e Projetadas) -> DÉBITO
                    cte_requisicao AS (
                        SELECT
                            r.tb_fundo_id_fundo AS fundo_id,
                            SUM(
                                CASE
                                    WHEN r.data_compensacao IS NULL
                                    AND r.data_hora_terceira_aprovacao IS NOT NULL THEN (
                                        COALESCE(r.valor_receber, 0) + COALESCE(r.valor_tarifa, 0)
                                    )
                                    ELSE 0
                                END
                            ) AS calc_proj_spdx,
                            SUM(
                                CASE
                                    WHEN r.data_compensacao IS NOT NULL THEN (
                                        COALESCE(r.valor_receber, 0) + COALESCE(r.valor_tarifa, 0)
                                    )
                                    ELSE 0
                                END
                            ) AS calc_comp_spdx
                        FROM
                            tb_requisicao_pagamento r
                        WHERE
                            r.habilitado = TRUE
                            AND r.tp_conta = 'SPDX'
                        GROUP BY
                            r.tb_fundo_id_fundo
                    ),
                    -- Soma de Tarifas Bancárias -> DÉBITO
                    cte_tarifa AS (
                        SELECT
                            v.fundo_id,
                            SUM(v.valor_tarifa) AS totaltarifasbancariasspdx
                        FROM
                            vw_materializada_tarifa_bancaria v
                            INNER JOIN tb_ordem t ON t.id = v.ordem_id
                        WHERE
                            t.tipo_conta_fundo = 'SPDX'
                        GROUP BY
                            v.fundo_id
                    ),
                    -- Soma de Transferências -> SEPARADO EM CRÉDITO E DÉBITO
                    cte_transferencia AS (
                        SELECT
                            fundo_id,
                            SUM(
                                CASE
                                    WHEN tipo_movimentacao = 'CRED' THEN valor
                                    ELSE 0
                                END
                            ) AS transferencia_credito,
                            SUM(
                                CASE
                                    WHEN tipo_movimentacao <> 'CRED' THEN valor
                                    ELSE 0
                                END
                            ) AS transferencia_debito
                        FROM
                            tb_transferencia_saldo
                        WHERE
                            tipo_conta = 'SPDX'
                        GROUP BY
                            fundo_id
                    )

                -- Resultado Final
                SELECT
                    u.nm_unidade AS "FRANQUIA",
                    f.id AS "COD FUNDO",
                    f.nm_fundo AS "FUNDO",
                    f.vl_orcamento_contrato AS "MAF INICIAL",
                    (ta.valor / NULLIF(f.vl_orcamento_contrato, 0)) AS "%% FEE/MAF",
                    ta.valor AS "FEE INICIAL",
                    CASE
                        WHEN f.dt_contrato IS NULL
                        OR f.dt_contrato > f.dt_cadastro THEN f.dt_cadastro
                        ELSE f.dt_contrato
                    END AS dt_contrato_fundo,
                    ordem_resumo.primeiro_boleto AS "PRIMEIRO BOLETO",
                    f.dt_baile,
                    
                    -- NOVAS COLUNAS SPDX
                    -- COLUNA DE CRÉDITOS (Entradas)
                    (
                        COALESCE(o.vl_pago_spdx_base, 0) + 
                        COALESCE(tr.transferencia_credito, 0)
                    ) AS "TOTAL CREDITO",
                    
                    -- COLUNA DE DÉBITOS (Saídas)
                    (
                        COALESCE(r.calc_proj_spdx, 0) + 
                        COALESCE(r.calc_comp_spdx, 0) + 
                        COALESCE(tar.totaltarifasbancariasspdx, 0) + 
                        COALESCE(tr.transferencia_debito, 0)
                    ) AS "TOTAL DEBITO",
                    
                    -- SALDO PROJETADO (Crédito - Débito)
                    (
                        (COALESCE(o.vl_pago_spdx_base, 0) + COALESCE(tr.transferencia_credito, 0))
                        - 
                        (COALESCE(r.calc_proj_spdx, 0) + COALESCE(r.calc_comp_spdx, 0) + COALESCE(tar.totaltarifasbancariasspdx, 0) + COALESCE(tr.transferencia_debito, 0))
                    ) AS "SALDO PROJETADO"

                FROM
                    tb_fundo f
                    INNER JOIN tb_unidade u ON u.id = f.unidade_id
                    INNER JOIN tb_integrante i ON i.fundo_id = f.id
                    INNER JOIN tb_fundo_cota fc ON fc.cota_id = i.cota_id AND i.fundo_id = fc.fundo_id
                    LEFT JOIN cte_taxas ta ON ta.id_fundo = f.id
                    LEFT JOIN cte_dados_ordem ordem_resumo ON ordem_resumo.fundo_id = f.id
                    
                    -- JOINS da lógica SPDX
                    LEFT JOIN cte_ordem o ON o.fundo_id = f.id
                    LEFT JOIN cte_requisicao r ON r.fundo_id = f.id
                    LEFT JOIN cte_tarifa tar ON tar.fundo_id = f.id
                    LEFT JOIN cte_transferencia tr ON tr.fundo_id = f.id

                WHERE
                    u.categoria = '2'
                    AND f.tipocliente_id IN (15, 17) -- FUNDO DE FORMATURA E PRE EVENTO
                    AND f.is_assessoria_pura IS FALSE
                    AND f.is_fundo_teste IS FALSE
                    AND f.dt_cadastro > '2019-01-01'
                    AND f.dt_baile > '2024-08-01'
                    AND f.carteiracobranca_id IN (2428, 2574) -- 2428 CONTA UNIFICADA - SICOOB | 2574 SPDX (FIXO) - BBPay

                GROUP BY
                    u.nm_unidade,
                    f.id,
                    f.nm_fundo,
                    f.vl_orcamento_contrato,
                    ta.valor,
                    f.dt_cadastro,
                    f.dt_contrato,
                    ordem_resumo.primeiro_boleto,
                    f.dt_baile,
                    o.vl_pago_spdx_base, 
                    tr.transferencia_credito, 
                    r.calc_proj_spdx, 
                    r.calc_comp_spdx, 
                    tar.totaltarifasbancariasspdx, 
                    tr.transferencia_debito
                ORDER BY 
                    f.id
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        print(f"Erro na query carteira_realizado: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)


# ==========================================
# ROTA 3: RPS FEE
# ==========================================
@app.get("/rps_fee")
def obter_rps_fee(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                SELECT
                    rp.tb_fundo_id_fundo AS "CÓD. FUNDO",
                    f.nm_fundo AS "FUNDO",
                    u.nm_unidade AS "FRANQUIA",
                    fo.nome_fantasia AS "FORNECEDOR",
                    cf.nome_categoria AS "CATEGORIA",
                    rp.valor_pagar AS "VALOR",
                    rp.data_compensacao AS "COMPENSADO EM",
                    u.ds_cnpj AS "CNPJ DA FRANQUIA",
                    fo.id_fornecedor AS "COD. FORNECEDOR",
                    fo.tipo_fornecedor AS "TIPO DE CADASTRO FORNECEDOR",
                    CASE
                        WHEN fo.cnpj IS NULL THEN fo.cpf
                        ELSE fo.cnpj
                    END AS "DOCUMENTO FORNECEDOR",
                    CASE
                        WHEN fo.unidade_propria IS FALSE THEN 'Não'
                        ELSE 'Sim'
                    END AS "UNIDADE PROPRIA",
                    fo.classificacao AS "CLASSIFICAÇÃO",
                    fdb.id AS "CÓD. FAVORECIDO",
                    fdb.nm_favorecido AS "FAVORECIDO",
                    rp.id_requisicao_pagamento AS "ID DA RP",
                    ops.nome AS "SERVIÇO OP",
                    rp.descricao AS "DESCRIÇÃO",
                    f.dt_baile AS "DATA BAILE",
                    CASE 
                        WHEN EXTRACT(DAY FROM (f.dt_baile - rp.data_compensacao)) < 90 THEN 'ULTIMA PARCELA'
                        ELSE 'ANTECIPAÇÃO'
                    END AS "TIPO DE RECEITA"
                FROM
                    tb_fornecedores fo
                    JOIN tb_unidade u ON u.id = fo.tb_unidade_id
                    JOIN tb_categoria_fornecedor cf ON cf.id_categoria_fornecedor = fo.tb_categoria_fornecedor_id_categoria_fornecedor
                    JOIN tb_requisicao_pagamento rp ON rp.tb_fornecedores_id_fornecedor = fo.id_fornecedor
                    JOIN tb_fornecedores_dados_bancarios fdb ON fdb.id_tb_fornecedores = rp.id_tb_fornecedores_dados_bancarios
                    JOIN tb_requisicao_pagamento_op_servico rpos ON rpos.id_rp = rp.id_requisicao_pagamento
                    JOIN tb_op_produto_servico ops ON ops.id = rpos.id_servico_associado
                    JOIN tb_fundo f ON f.id = rp.tb_fundo_id_fundo
                WHERE
                    fo.habilitado IS TRUE
                    AND u.categoria = '2'
                    AND ops.nome = 'FEE/CERIMONIAL - MC FRANQUIA %%'
                    AND rp.data_hora_cadastro > '2026-01-01'
                    AND rp.data_compensacao IS NOT NULL
                    AND rp.novo_cadastro IS TRUE
                    AND rp.descricao NOT ILIKE '%%MARGEM%%'
                GROUP BY
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
                ORDER BY
                    rp.tb_fundo_id_fundo
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        print(f"Erro na query rps_fee: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)
