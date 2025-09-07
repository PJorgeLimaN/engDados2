import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import boto3
import io # Usado para salvar o arquivo em memória

# --- Função Principal do Lambda ---
def lambda_handler(event, context):
    """
    Função principal que a AWS Lambda executará.
    """
    # URL da página alvo
    url = "https://acesso.cesmac.edu.br/maceio/pos-graduacao"
    base_url = "https://acesso.cesmac.edu.br"

    print(f"Iniciando scraping da página: {url}")

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        
        cursos_lista = []

        seletor_container = "article.accordion-especializacao div.courses-accordion-item-content"
        container_de_categorias = soup.select_one(seletor_container)

        if container_de_categorias:
            caixas_de_categoria = container_de_categorias.find_all('div', class_='box', recursive=False)
            print(f"Encontradas {len(caixas_de_categoria)} áreas de curso (boxes).")

            for box in caixas_de_categoria:
                area_curso_tag = box.find('h4')
                area_curso = area_curso_tag.text.strip() if area_curso_tag else "Área não definida"
                
                tags_de_link_dos_cursos = box.select("div.all a")
                
                for link_tag in tags_de_link_dos_cursos:
                    nome_curso = link_tag.get('title', '').strip()
                    link_relativo = link_tag.get('href', '')
                    
                    if nome_curso and link_relativo:
                        link_absoluto = urljoin(base_url, link_relativo)
                        cursos_lista.append({
                            'area': area_curso,
                            'curso': nome_curso,
                            'link': link_absoluto,
                            'data_extracao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
        
        if not cursos_lista:
            print("Nenhum curso encontrado. Encerrando.")
            return {'statusCode': 200, 'body': 'Nenhum curso encontrado.'}

        # Converte a lista em DataFrame do Pandas
        df_cursos = pd.DataFrame(cursos_lista)
        
        # --- Parte de salvar no S3 ---
        
        # Nome do bucket S3 onde você quer salvar o arquivo
        bucket_name = 'class-bucket-pj'
        
        # Cria um nome de arquivo dinâmico com a data e hora
        now = datetime.now()
        file_name = f"cursos-cesmac/{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}/cursos_{now.strftime('%Y%m%d_%H%M%S')}.parquet"

        # Salva o DataFrame em formato Parquet em um buffer na memória
        out_buffer = io.BytesIO()
        df_cursos.to_parquet(out_buffer, index=False, engine='pyarrow')
        
        # Inicia o cliente S3
        s3_client = boto3.client('s3')
        
        # Faz o upload do buffer para o S3
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=out_buffer.getvalue())

        print(f"Sucesso! Arquivo '{file_name}' salvo no bucket '{bucket_name}'.")
        
        return {'statusCode': 200, 'body': f'Arquivo {file_name} salvo com sucesso!'}

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a página: {e}")
        raise e
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        raise e