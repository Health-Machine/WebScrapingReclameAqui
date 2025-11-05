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
    df = pd.read_csv(io.StringIO(body))
    print(f"Arquivo carregado com {len(df)} linhas e {len(df.columns)} colunas.")

    # --- Limpeza básica ---
    df = df.dropna()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # --- Filtro contextual (pintura, verniz, revestimento) ---
    if 'description' in df.columns:
        print("Filtrando linhas com defeitos relacionados a pintura, verniz ou revestimento...")
        df['description_lower'] = df['description'].str.lower()

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
