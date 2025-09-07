import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

# URL da página alvo
url = "https://acesso.cesmac.edu.br/maceio/pos-graduacao"
base_url = "https://acesso.cesmac.edu.br"

print(f"Iniciando scraping da página: {url}")

try:
    # 1. Faz a requisição para obter o conteúdo HTML da página
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    # 2. Cria o objeto BeautifulSoup para analisar o HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Lista para armazenar os dados dos cursos
    cursos_lista = []

    # MUDANÇA 1: Lógica de busca aninhada para capturar a ÁREA do curso
    
    # Primeiro, encontramos o contêiner principal de conteúdo da especialização
    seletor_container = "article.accordion-especializacao div.courses-accordion-item-content"
    container_de_categorias = soup.select_one(seletor_container)

    if container_de_categorias:
        # Depois, encontramos todas as "caixas de categoria" (div.box) dentro do contêiner
        caixas_de_categoria = container_de_categorias.find_all('div', class_='box', recursive=False)
        print(f"\nEncontradas {len(caixas_de_categoria)} áreas de curso (boxes).")

        # Iteramos sobre cada caixa de categoria
        for box in caixas_de_categoria:
            # Pegamos o nome da área, que está no H4
            area_curso_tag = box.find('h4')
            area_curso = area_curso_tag.text.strip() if area_curso_tag else "Área não definida"
            
            # Agora, encontramos todos os links de curso DENTRO desta caixa de categoria
            tags_de_link_dos_cursos = box.select("div.all a")
            
            # Iteramos sobre os links encontrados nesta área específica
            for link_tag in tags_de_link_dos_cursos:
                nome_curso = link_tag.get('title', '').strip()
                link_relativo = link_tag.get('href', '')
                
                if nome_curso and link_relativo:
                    link_absoluto = urljoin(base_url, link_relativo)
                    
                    # Adicionamos a ÁREA junto com as outras informações
                    cursos_lista.append({
                        'area': area_curso,
                        'curso': nome_curso,
                        'link': link_absoluto
                    })

    # 3. Exibe os resultados e exporta para Excel
    if cursos_lista:
        print(f"\nExtraídos {len(cursos_lista)} cursos de especialização com sucesso.")
        
        df_cursos = pd.DataFrame(cursos_lista)
        
        print("\n--- Tabela de Cursos (Prévia) ---")
        print(df_cursos.head()) # Mostra apenas as 5 primeiras linhas como prévia

        # MUDANÇA 2: Exporta o DataFrame para um arquivo Excel
        nome_do_arquivo = 'cursos_pos_graduacao_cesmac.xlsx'
        df_cursos.to_excel(nome_do_arquivo, index=False, engine='openpyxl')
        
        print(f"\nDados exportados com sucesso para o arquivo '{nome_do_arquivo}'!")

    else:
        print("\nNenhum curso de especialização foi encontrado.")

except requests.exceptions.RequestException as e:
    print(f"Erro ao acessar a página: {e}")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")