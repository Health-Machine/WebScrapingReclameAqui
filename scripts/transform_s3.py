import sys
import io
import boto3
import pandas as pd
import re
import chardet

# --- Palavras-chave do filtro (mais restrito e contextual) ---
BASE_TERMS = [
    "pintura", "tinta", "verniz", "revestimento"
]

DEFECT_TERMS = [
    # --- Defeitos físicos / desgaste ---
    "descasc", "desgast", "desbot", "descolor", "descolad", "descamad", "soltando",
    "craquel", "rachad", "trincad", "fissur", "microfissur", "microbolh", "bolh",
    "peeling", "falh", "imperfei", "danific", "defeituos", "incomplet", "mal aplicad",
    "queimad", "oxid", "ferrug", "poros", "enrugad",
    
    # --- Aparência / tonalidade / brilho ---
    "manch", "irregular", "tonalid", "cor irregular", "diferença de cor", "diferença de tonalidade",
    "sem brilho", "falta de brilho", "perda de brilho", "opac", "fosc",
    
    # --- Textura / superfície ---
    "camada fina", "camada grossa", "superfície irregular", "porosidade", "textura irregular",
    
    # --- Riscos e arranhões ---
    "arranh", "risc", "arranhadura", "arranhaduras", "arranhões", "riscos",
    
    # --- Termos compostos frequentes ---
    "pintura falha", "pintura falhada", "tinta falhada", "tinta falhando",
    "revestimento falho", "revestimento descascado", "revestimento desgastado",
    "revestimento defeituoso", "revestimento oxidado", "revestimento poroso",
    
    # --- Combinações específicas ---
    "pintura danificada", "pintura defeituosa", "pintura incompleta", "pintura mal aplicada",
    "pintura queimada", "pintura com bolhas", "pintura com manchas",
    "pintura com riscos", "pintura fosca", "pintura opaca",
    "verniz queimado", "verniz rachado", "verniz descascado", "verniz descascando",
    "verniz desbotado", "verniz com bolhas", "verniz com manchas",
    "verniz com riscos", "verniz fosco", "verniz opaco"
]

# Cria um padrão regex combinando base + defeito
PATTERN = re.compile(
    r"\b(?:" + "|".join(BASE_TERMS) + r")\b.{0,100}\b(?:" + "|".join(DEFECT_TERMS) + r")\b",
    re.IGNORECASE
)

# --- Parâmetros e conexão com S3 ---
if len(sys.argv) < 4:
    print("Uso: transform_s3.py <raw_bucket> <trusted_bucket> <client_bucket>")
    sys.exit(1)

raw_bucket, trusted_bucket, client_bucket = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client('s3')

print(f"Iniciando transformação entre buckets:")
print(f"RAW → {raw_bucket}")
print(f"TRUSTED → {trusted_bucket}")
print(f"CLIENT → {client_bucket}")

# --- Listar arquivos no bucket RAW ---
response = s3.list_objects_v2(Bucket=raw_bucket)
if 'Contents' not in response:
    print("Nenhum arquivo encontrado no bucket RAW.")
    sys.exit(0)

for obj in response['Contents']:
    key = obj['Key']
    if not key.endswith(".csv"):
        continue

    print(f"\n📥 Lendo arquivo: s3://{raw_bucket}/{key}")
    raw_obj = s3.get_object(Bucket=raw_bucket, Key=key)
    raw_bytes = raw_obj['Body'].read()

    # Detecta a codificação automaticamente
    detected = chardet.detect(raw_bytes)
    encoding = detected.get('encoding') or 'utf-8'
    print(f"🔍 Codificação detectada: {encoding}")

    try:
        body = raw_bytes.decode(encoding)
    except UnicodeDecodeError:
        print("⚠️ Falha ao decodificar com a codificação detectada. Tentando fallback latin1...")
        body = raw_bytes.decode('latin1', errors='replace')

    # --- Lê CSV em DataFrame ---
    try:
        df = pd.read_csv(io.StringIO(body))
    except Exception as e:
        print(f"❌ Erro ao ler CSV {key}: {e}")
        continue

    print(f"Arquivo carregado com {len(df)} linhas e {len(df.columns)} colunas.")

    # --- Limpeza básica ---
    df = df.dropna()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # --- Filtro contextual ---
    if 'description' in df.columns:
        print("Filtrando linhas com defeitos relacionados a pintura, verniz ou revestimento...")
        df['description_lower'] = df['description'].astype(str).str.lower()

        mask = df['description_lower'].apply(lambda x: bool(PATTERN.search(x)))
        df = df[mask].drop(columns=['description_lower'])
        print(f"Após o filtro contextual, restaram {len(df)} linhas.")
    else:
        print("Coluna 'description' não encontrada. Nenhum filtro aplicado.")

    # --- Criar camada CLIENT (reduzida) ---
    if set(['order_id', 'amount']).issubset(df.columns):
        df_client = df[['order_id', 'amount']]
        print("Camada client criada com colunas: order_id, amount")
    else:
        df_client = df.copy()
        print("Colunas order_id/amount não encontradas, usando dataset completo.")

    # --- Salvar arquivos filtrados e limpos ---
    trusted_key = f"trusted_{key}"
    client_key = f"client_{key}"

    trusted_buf = io.StringIO()
    client_buf = io.StringIO()
    df.to_csv(trusted_buf, index=False)
    df_client.to_csv(client_buf, index=False)

    print(f"📤 Enviando arquivo filtrado (trusted) para s3://{trusted_bucket}/{trusted_key}")
    s3.put_object(Bucket=trusted_bucket, Key=trusted_key, Body=trusted_buf.getvalue().encode('utf-8-sig'))

    print(f"📤 Enviando arquivo client para s3://{client_bucket}/{client_key}")
    s3.put_object(Bucket=client_bucket, Key=client_key, Body=client_buf.getvalue().encode('utf-8-sig'))

print("\n🏁 Transformação concluída com sucesso.")
