import scraping  # o módulo que você já tem
import pandas as pd

def main():
    company_id = scraping.get_company_id("chevrolet")
    reclamacoes, _ = scraping.get_all_data(company_id)

    dados = []

    for reclamacao in reclamacoes:
        descricao = scraping.get_full_description(reclamacao)
        if not descricao:
            descricao = ''  # evita None

        dados.append({
            "title": reclamacao.get('title', ''),
            "description": descricao,
            "userState": reclamacao.get('userState', ''),
            "userCity": reclamacao.get('userCity', ''),
            "status": reclamacao.get('status', ''),
            "created": reclamacao.get('created', ''),
            "url": f"https://www.reclameaqui.com.br/{reclamacao['companyShortname']}/{reclamacao['url']}/"
        })

    print(f"Total de reclamações coletadas: {len(dados)}")

    # Salva tudo
    df = pd.DataFrame(dados)
    df.to_csv("data/reclamacoes_bruto.csv", index=False, encoding="utf-8-sig")
    print("Arquivo reclamacoes_bruto.csv salvo com sucesso.")

if __name__ == "__main__":
    main()
