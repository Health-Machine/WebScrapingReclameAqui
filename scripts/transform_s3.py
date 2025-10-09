import sys
import io
import boto3
import pandas as pd

# Verifica argumentos
if len(sys.argv) < 4:
    print("Uso: transform_s3.py <raw_bucket> <trusted_bucket> <client_bucket>")
    sys.exit(1)

raw_bucket, trusted_bucket, client_bucket = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client('s3')

print(f"Iniciando transformação entre buckets:")
print(f"RAW → {raw_bucket}")
print(f"TRUSTED → {trusted_bucket}")
print(f"CLIENT → {client_bucket}")

# Lista arquivos do bucket raw
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

    # Regras simples: remover linhas com nulos e padronizar nomes de colunas
    df = df.dropna()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Cria camada client com colunas específicas (se existirem)
    if set(['order_id', 'amount']).issubset(df.columns):
        df_client = df[['order_id', 'amount']]
        print("Camada client criada com colunas: order_id, amount")
    else:
        df_client = df.copy()
        print("Colunas order_id/amount não encontradas, usando dataset completo.")

    # Salvar arquivos para trusted e client
    trusted_key = f"trusted_{key}"
    client_key = f"client_{key}"

    # Converter DataFrames em CSVs em memória
    trusted_buf = io.StringIO()
    client_buf = io.StringIO()
    df.to_csv(trusted_buf, index=False)
    df_client.to_csv(client_buf, index=False)

    # Upload para buckets correspondentes
    print(f"📤 Enviando arquivo limpo para s3://{trusted_bucket}/{trusted_key}")
    s3.put_object(Bucket=trusted_bucket, Key=trusted_key, Body=trusted_buf.getvalue().encode('utf-8-sig'))

    print(f"📤 Enviando arquivo client para s3://{client_bucket}/{client_key}")
    s3.put_object(Bucket=client_bucket, Key=client_key, Body=client_buf.getvalue().encode('utf-8-sig'))

print("\n🏁 Transformação concluída com sucesso.")
