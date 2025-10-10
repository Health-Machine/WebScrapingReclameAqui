import sys
import io
import boto3
import pandas as pd

# --- Palavras-chave do filtro (copiadas do seu script Lambda) ---
KEYWORDS = [
    "pintura", "pintado", "pintada",
    "verniz", "envernizado", "envernizada",
    "acabamento", "acabado", "acabada",
    "descascado", "descascada",
    "descascando", "descascante",
    "desbotado", "desbotada",
    "desgastado", "desgastada",
    "manchado", "manchada",
    "arranhado", "arranhada",
    "riscado", "riscada",
    "arranhadura", "arranhaduras",
    "riscos", "arranhões",
    "poroso", "porosa",
    "fosco", "fosca",
    "opaco", "opaca",
    "craquelado", "craquelada",
    "rachado", "rachada",
    "trincado", "trincada",
    "soltando tinta", "soltando verniz",
    "falha de pintura", "falhas de pintura",
    "imperfeito", "imperfeita",
    "imperfeição", "imperfeições",
    "bolha", "bolhas",
    "bolhoso", "bolhosa",
    "descamando", "descamado", "descamada",
    "soltando camadas", "descolado", "descolada",
    "oxidação", "oxidado", "oxidada",
    "ferrugem na pintura", "ferrugem no verniz",
    "descoloração", "descolorido", "descolorida",
    "enrugado", "enrugada",
    "reparo mal feito", "retoque mal feito",
    "diferença de tonalidade", "tonalidade irregular",
    "diferença de cor", "cor irregular",
    "acabamento irregular", "acabamento ruim",
    "pintura irregular", "verniz irregular",
    "mancha", "manchas",
    "peeling", "peelings",
    "craquelamento", "craquelamentos",
    "tinta falhada", "tinta falhando",
    "camada fina", "camada grossa",
    "pintura falha", "pintura falhada",
    "microbolhas", "microfissuras",
    "pintura soltando", "verniz soltando",
    "verniz rachado", "verniz rachada",
    "verniz desbotado", "verniz desbotada",
    "verniz descascando", "verniz descascado",
    "pintura danificada", "pintura defeituosa",
    "verniz danificado", "verniz defeituoso",
    "pintura incompleta", "pintura mal aplicada",
    "tinta irregular", "tinta manchada",
    "revestimento falho", "revestimento descascado",
    "revestimento desgastado", "revestimento defeituoso",
    "revestimento oxidado", "revestimento poroso",
    "falta de brilho", "sem brilho",
    "perda de brilho", "pintura sem brilho",
    "pintura queimada", "verniz queimado",
    "pintura com bolhas", "verniz com bolhas",
    "pintura com manchas", "verniz com manchas",
    "pintura com riscos", "verniz com riscos",
    "pintura fosca", "verniz fosco",
    "pintura opaca", "verniz opaco"
]

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
    body = raw_obj['Body'].read().decode('utf-8-sig')

    # Lê CSV em DataFrame
    df = pd.read_csv(io.StringIO(body))
    print(f"Arquivo carregado com {len(df)} linhas e {len(df.columns)} colunas.")

    # --- Limpeza básica ---
    df = df.dropna()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # --- Filtro por palavras-chave (somente se existir 'description') ---
    if 'description' in df.columns:
        print("Filtrando linhas com palavras-chave relacionadas a pintura/verniz...")
        mask = df['description'].str.lower().apply(
            lambda x: any(keyword in x for keyword in KEYWORDS)
        )
        df = df[mask]
        print(f"Após o filtro, restaram {len(df)} linhas.")
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
