# app.py
import os
import datetime
import psycopg2
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from flask import Flask, jsonify, request

app = Flask(__name__) # Cria a aplicação Flask (nome 'app' é padrão)

# --- Configurações Essenciais ---
# Lendo do ambiente Render
DATABASE_URL = os.environ.get("DATABASE_URL")
# Lendo do Secret File no Render
GCP_KEY_PATH = '/etc/secrets/gsc_service_account.json'
GSC_SITE_URL = 'https://www.feirasderua.com.br/' # Sua propriedade GSC

# --- Constantes da API ---
GSC_SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
API_SERVICE_NAME = 'searchconsole'
API_VERSION = 'v1'
ROW_LIMIT_PER_REQUEST = 5000

# --- Funções do Script Original (authenticate_gsc, fetch_gsc_data, load_data_to_postgres) ---
# (Cole as funções completas aqui, como na versão anterior - v5)

def authenticate_gsc(key_path, scopes):
    """Autentica com a API GSC usando a chave da conta de serviço."""
    try:
        creds = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds, cache_discovery=False)
        print("✅ Autenticação com API GSC bem-sucedida.")
        return service, None # Retorna serviço e None para erro
    except FileNotFoundError:
        error_msg = f"❌ ERRO: Arquivo de chave não encontrado em: {key_path}"
        print(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"❌ ERRO na autenticação GSC: {e}"
        print(error_msg)
        return None, error_msg

def fetch_gsc_data(service, site_url, start_date_str, end_date_str):
    """Busca dados de desempenho do GSC para o período especificado."""
    all_rows = []
    start_row = 0
    processed_rows_count = 0
    print(f"Buscando dados do GSC para {site_url} entre {start_date_str} e {end_date_str}...")
    while True:
        try:
            request_body = {
                'startDate': start_date_str, 'endDate': end_date_str,
                'dimensions': ['date', 'page', 'query', 'device'],
                'rowLimit': ROW_LIMIT_PER_REQUEST, 'startRow': start_row
            }
            response = service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
            rows = response.get('rows', [])
            if not rows:
                print(f"   -> Nenhuma linha encontrada a partir da linha {start_row}.")
                break
            num_rows_in_batch = len(rows)
            processed_rows_count += num_rows_in_batch
            print(f"   -> Recebido lote de {num_rows_in_batch} (Total: {processed_rows_count}).")
            all_rows.extend(rows)
            if num_rows_in_batch < ROW_LIMIT_PER_REQUEST: break
            start_row += ROW_LIMIT_PER_REQUEST
        except HttpError as e:
            error_msg = f"❌ ERRO API GSC: {e}"
            print(error_msg)
            try:
                error_details = e.resp.status
                error_reason = e._get_reason()
                print(f"   Status: {error_details}, Razão: {error_reason}")
                if hasattr(e, 'content'): print(f"   Conteúdo: {e.content.decode()}")
            except Exception: pass
            return None, error_msg
        except Exception as e:
            error_msg = f"❌ ERRO inesperado fetch GSC: {e}"
            print(error_msg)
            return None, error_msg
    print(f"✅ Busca GSC concluída. Total: {len(all_rows)} linhas.")
    return all_rows, None

def load_data_to_postgres(db_url, data_rows, site_url_prop):
    if not data_rows: return 0, 0, "Nenhum dado para carregar."
    conn = None
    inserted_count = 0
    updated_count = 0
    upsert_sql = """
    INSERT INTO gsc_desempenho (data, site_url, page, query, device, search_type, clicks, impressions, ctr, position)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (data, site_url, page, query, device, search_type) DO UPDATE SET
        clicks = EXCLUDED.clicks, impressions = EXCLUDED.impressions, ctr = EXCLUDED.ctr,
        position = EXCLUDED.position, data_extracao = CURRENT_TIMESTAMP;
    """
    try:
        print(f"\nConectando ao PostgreSQL...")
        if not db_url:
             raise ValueError("DATABASE_URL não está configurada.")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        print("✅ Conexão PostgreSQL bem-sucedida.")
        print(f"Iniciando carregamento de {len(data_rows)} linhas...")
        for row in data_rows:
            keys = row.get('keys', [])
            data, page, query, device_raw = (keys + [None]*4)[:4]
            device = device_raw.upper() if device_raw else 'UNKNOWN'
            search_type = 'WEB'
            clicks, impressions, ctr, position = row.get('clicks', 0), row.get('impressions', 0), row.get('ctr', 0.0), row.get('position', 0.0)
            values = (data, site_url_prop, page or 'N/A', query or 'N/A', device, search_type, clicks, impressions, ctr, position)
            cur.execute(upsert_sql, values)
            if cur.statusmessage == "INSERT 0 1": inserted_count += 1
            elif "UPDATE 1" in cur.statusmessage: updated_count += 1
        conn.commit()
        success_msg = f"Carregamento concluído! Inseridas: {inserted_count}, Atualizadas: {updated_count}"
        print(f"\n✅ {success_msg}")
        return inserted_count, updated_count, success_msg
    except Exception as e:
        error_msg = f"❌ ERRO PostgreSQL: {e}"
        print(f"\n{error_msg}")
        if conn: conn.rollback()
        return inserted_count, updated_count, error_msg
    finally:
        if conn:
            cur.close()
            conn.close()
            print("\n🔌 Conexão PostgreSQL fechada.")

# --- Endpoint da API ---
@app.route('/trigger-gsc-sync', methods=['POST'])
def trigger_gsc_sync():
    print("\n--- Recebido trigger para Pipeline GSC -> PostgreSQL ---")
    if not DATABASE_URL:
        print("❌ ERRO Fatal: DATABASE_URL não definida nas variáveis de ambiente.")
        return jsonify({"status": "error", "message": "DATABASE_URL not configured"}), 500
    try:
        days_param = int(request.args.get('days', '2'))
    except ValueError:
        days_param = 2
    print(f"Buscando dados de {days_param} dia(s) atrás.")

    gsc_service, auth_error = authenticate_gsc(GCP_KEY_PATH, GSC_SCOPES)
    if not gsc_service:
        return jsonify({"status": "error", "message": f"GSC Authentication Failed: {auth_error}"}), 500

    target_date = datetime.date.today() - datetime.timedelta(days=days_param)
    start_date_str = target_date.strftime('%Y-%m-%d')
    end_date_str = target_date.strftime('%Y-%m-%d')

    gsc_rows, fetch_error = fetch_gsc_data(gsc_service, GSC_SITE_URL, start_date_str, end_date_str)
    if gsc_rows is None:
         return jsonify({"status": "error", "message": f"GSC Fetch Failed: {fetch_error}"}), 500

    inserted, updated, load_message = load_data_to_postgres(DATABASE_URL, gsc_rows, GSC_SITE_URL)

    if "ERRO" in load_message:
         return jsonify({"status": "error", "message": f"PostgreSQL Load Failed: {load_message}", "inserted": inserted, "updated": updated}), 500
    else:
         return jsonify({"status": "success", "message": load_message, "date_processed": start_date_str, "rows_found": len(gsc_rows), "inserted": inserted, "updated": updated}), 200

@app.route('/')
def index():
    return jsonify({"message": "GSC API Sync Service is running"})
