import boto3
import io
import csv

s3 = boto3.client('s3')

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
    print("Lambda inicializada, evento recebido:", event)

    for record in event['Records']:
        src_bucket = record['s3']['bucket']['name']
        src_key = record['s3']['object']['key']
        print(f"Arquivo recebido do S3: s3://{src_bucket}/{src_key}")

        if not src_key.endswith("reclamacoes_bruto.csv"):
            print(f"Arquivo {src_key} ignorado (não é reclamacoes_bruto.csv).")
            continue

        # Baixa arquivo do S3
        print("Baixando arquivo do S3…")
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
        body = obj['Body'].read().decode('utf-8-sig')
        print(f"Arquivo baixado, tamanho {len(body)} bytes.")

        # Lê CSV usando csv.DictReader
        reader = csv.DictReader(io.StringIO(body))
        fieldnames = reader.fieldnames
        if 'description' not in fieldnames:
            print("Coluna 'description' não encontrada!")
            return {"statusCode": 400, "body": "Coluna description não encontrada"}

        # Filtra linhas
        print("Filtrando linhas…")
        filtered_rows = [
            row for row in reader
            if any(keyword in row.get('description', '').lower() for keyword in KEYWORDS)
        ]
        print(f"Filtragem concluída: {len(filtered_rows)} linhas encontradas.")

        # Escreve CSV de volta
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_rows)

        dest_bucket = 'trusted-bucket-199917718936'
        dest_key = src_key.replace("reclamacoes_bruto.csv", "reclamacoes_pintura.csv")

        print(f"Enviando arquivo filtrado para s3://{dest_bucket}/{dest_key}…")
        s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=output.getvalue().encode('utf-8-sig'))
        print("Upload concluído com sucesso.")

    print("Fim do handler.")
    return {"statusCode": 200, "body": "Processamento concluído"}
