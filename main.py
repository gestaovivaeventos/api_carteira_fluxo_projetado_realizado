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
    return {"status": "ok"}

@app.get("/dados")
def obter_dados(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                SELECT
                    u.id,
                    CASE
                        WHEN uni.id = 31 THEN 'Itaperuna Muriae'
                        ELSE uni.nm_unidade
                    END AS nm_unidade,
                    CASE
                        WHEN uni_main.id = 31 THEN 'Itaperuna Muriae'
                        ELSE uni_main.nm_unidade
                    END AS nm_unidade_principal_desc,
                    u.nome,
                    u.username,
                    u.enabled,
                    uni.id AS id_unidade,
                    uv.prioritaria AS unidade_principal,
                    gr.name AS "nm_grupo/cargo",
                    u.datacriacao AS dt_criacao,
                    u.last_login AS ultimo_acesso
                FROM
                    tb_usuario u
                    LEFT JOIN tb_unidade_vinculada uv ON uv.id_usuario = u.id
                    LEFT JOIN tb_unidade uni ON uni.id = uv.id_unidade
                    LEFT JOIN tb_grupo gr ON gr.id = uv.id_grupo
                    JOIN tb_grupo_permissao per ON per.id_grupo = gr.id
                    LEFT JOIN tb_setor s ON s.id = gr.id_setor
                    LEFT JOIN tb_unidade_vinculada uv_main ON uv_main.id_usuario = u.id
                    AND uv_main.prioritaria IS TRUE
                    LEFT JOIN tb_unidade uni_main ON uni_main.id = uv_main.id_unidade
                WHERE
                    -- CORREÇÃO ABAIXO: Uso de %% para escapar a porcentagem
                    gr.name NOT LIKE '%%Integrantes%%'
                    AND gr.name NOT LIKE '%%Comissão%%'
                    AND u.cpf IS NULL
                    AND uni.id NOT IN (8,7,43,49,69,75,50,79,51,61,73,47,48,37)
                    AND uni.fl_ativa
                GROUP BY
                    u.username,
                    uni.nm_unidade,
                    u.id,
                    uni.id,
                    s.setor,
                    gr.name,
                    uv.prioritaria,
                    uni_main.id,
                    uni_main.nm_unidade
                ORDER BY
                    u.id
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        # Isso vai te ajudar a ver o erro real no log do Vercel se acontecer de novo
        print(f"Erro na query: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)
