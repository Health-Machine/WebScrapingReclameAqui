import scraping

def main():
    reclamacoes, problemas = scraping.get_all_data()
    print(f"Total Reclamacoes: {len(reclamacoes)}")
    print(f"Total Problemas: {len(problemas)}")

    without_description = 0

    for reclamacao in reclamacoes:
        title = reclamacao['title']
        full_description = scraping.get_full_description(reclamacao)
        userState = reclamacao['userState']
        userCity = reclamacao['userCity']
        status = reclamacao['status']
        created = reclamacao['created']
        url = f"https://www.reclameaqui.com.br/{reclamacao['companyShortname']}/{reclamacao['url']}/"

        print(f"title: {title}")
        print(f"description: {full_description}")
        print(f"userState: {userState}")
        print(f"userCity: {userCity}")
        print(f"status: {status}")
        print(f"created: {created}")
        print(f"url: {url}")
        print("-----")

        # Continuação do código...

        if full_description == "No description available":
            without_description += 1

    print(f"Reclamacoes without description: {without_description}")


if __name__ == '__main__':
    main()