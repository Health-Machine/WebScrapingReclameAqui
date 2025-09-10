import boto3
import pandas as pd
import io

s3 = boto3.client('s3')

# Palavras-chave para filtrar reclamações
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

def lambda_handler(event, context):
    # Pega bucket e key do evento S3
    for record in event['Records']:
        src_bucket = record['s3']['bucket']['name']
        src_key = record['s3']['object']['key']

        # Só processa se for o arquivo reclamacoes_bruto.csv
        if not src_key.endswith("reclamacoes_bruto.csv"):
            print(f"Arquivo {src_key} ignorado.")
            continue

        # Baixa arquivo do S3
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
        body = obj['Body'].read()

        # Lê CSV em DataFrame
        df = pd.read_csv(io.BytesIO(body), encoding='utf-8-sig')

        # Previne NaN em description
        df['description'] = df['description'].fillna('')

        # Filtra linhas
        mask = df['description'].str.lower().apply(
            lambda x: any(keyword in x for keyword in KEYWORDS)
        )
        df_filtrado = df[mask]

        # Converte para bytes
        csv_buffer = io.StringIO()
        df_filtrado.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')

        # Salva no bucket trusted (mesmo nome, mas filtrado)
        dest_bucket = 'trusted'
        dest_key = src_key.replace("reclamacoes_bruto.csv", "reclamacoes_pintura.csv")

        s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=csv_bytes)

        print(f"Arquivo filtrado enviado para s3://{dest_bucket}/{dest_key} "
              f"({len(df_filtrado)} linhas filtradas)")

    return {"statusCode": 200, "body": "Processamento concluído"}
