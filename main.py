import scraping  # o módulo que você já tem
import pandas as pd

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


def main():
    reclamacoes, _ = scraping.get_all_data()

    # Lista para acumular as reclamações filtradas
    dados_filtrados = []

    for reclamacao in reclamacoes:
        descricao = scraping.get_full_description(reclamacao)
        if not descricao:  # se vier None ou vazio
            continue

        texto_lower = descricao.lower()

        # Se qualquer palavra-chave estiver na descrição, guarda
        if any(keyword in texto_lower for keyword in KEYWORDS):
            dados_filtrados.append({
                "title": reclamacao.get('title', ''),
                "description": descricao,
                "userState": reclamacao.get('userState', ''),
                "userCity": reclamacao.get('userCity', ''),
                "status": reclamacao.get('status', ''),
                "created": reclamacao.get('created', ''),
                "url": f"https://www.reclameaqui.com.br/{reclamacao['companyShortname']}/{reclamacao['url']}/"
            })

    print(f"Total de reclamações filtradas: {len(dados_filtrados)}")

    # Salva em CSV
    df = pd.DataFrame(dados_filtrados)
    df.to_csv("reclamacoes_pintura.csv", index=False, encoding="utf-8-sig")
    print("Arquivo reclamacoes_pintura.csv salvo com sucesso.")

if __name__ == "__main__":
    main()
