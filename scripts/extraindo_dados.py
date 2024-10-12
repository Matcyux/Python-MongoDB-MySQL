from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

# Função para conectar ao MongoDB
def conectar_mongodb(uri):
    """
    Conecta ao MongoDB usando o URI fornecido.
    Envia um 'ping' para garantir que a conexão foi bem-sucedida.
    Retorna o cliente MongoDB se a conexão for bem-sucedida, senão retorna None.
    """
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        # Envia um comando 'ping' para verificar a conexão
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None

# Função para criar um banco de dados e uma coleção
def criar_db_colecao(client, db_name, collection_name):
    """
    Cria um banco de dados e uma coleção dentro dele.
    Retorna o banco de dados e a coleção criados.
    """
    db = client[db_name]  # Cria o banco de dados
    collection = db[collection_name]  # Cria a coleção dentro do banco de dados
    print(f"Banco de dados '{db_name}' e coleção '{collection_name}' criados.")
    return db, collection

# Função para inserir um documento de exemplo
def inserir_exemplo(collection, produto, quantidade):
    """
    Insere um documento de exemplo na coleção, com um produto e sua quantidade.
    """
    product = {"produto": produto, "quantidade": quantidade}
    collection.insert_one(product)  # Insere o documento na coleção
    print(f"Produto de exemplo inserido: {product}")

# Função para extrair dados de uma API
def extrair_dados_api(url):
    """
    Faz uma requisição GET para uma API e extrai os dados em formato JSON.
    Retorna os dados extraídos.
    """
    response = requests.get(url)  # Faz a requisição para a API
    if response.status_code == 200:
        # Se a requisição for bem-sucedida, converte os dados para JSON
        dados = response.json()
        print(f"Dados extraídos da API: {len(dados)} registros.")
        return dados
    else:
        print(f"Erro ao extrair dados da API. Status: {response.status_code}")
        return []

# Função para inserir dados extraídos na coleção
def inserir_dados_api(collection, dados):
    """
    Insere múltiplos documentos (dados extraídos da API) na coleção.
    Verifica se há dados antes de realizar a inserção.
    """
    if dados:
        # Insere vários documentos na coleção de uma vez
        docs = collection.insert_many(dados)
        print(f"{len(docs.inserted_ids)} documentos inseridos na coleção.")
    else:
        print("Nenhum dado para inserir.")

# Função para remover o registro de exemplo
def remover_exemplo(collection):
    """
    Remove o primeiro documento encontrado na coleção (exemplo inserido inicialmente).
    """
    exemplo = collection.find_one()  # Busca o primeiro documento na coleção
    if exemplo:
        # Remove o documento usando seu ID único
        collection.delete_one({"_id": exemplo["_id"]})
        print(f"Documento de exemplo removido: {exemplo}")
    else:
        print("Nenhum documento de exemplo encontrado.")

# Função principal para o pipeline de dados
def pipeline_dados(uri, db_name, collection_name, api_url):
    """
    Função principal que gerencia o pipeline de dados:
    1. Conecta ao MongoDB.
    2. Cria o banco de dados e a coleção.
    3. Insere um documento de exemplo.
    4. Extrai dados de uma API.
    5. Insere os dados extraídos na coleção.
    6. Remove o documento de exemplo.
    7. Conta e exibe o total de documentos na coleção.
    8. Fecha a conexão com o MongoDB.
    """
    # Passo 1: Conectar ao MongoDB
    client = conectar_mongodb(uri)
    
    if client:
        # Passo 2: Criar banco de dados e coleção
        db, collection = criar_db_colecao(client, db_name, collection_name)
        
        # Passo 3: Inserir um documento de exemplo
        inserir_exemplo(collection, "computador", 77)
        
        # Passo 4: Extrair dados da API
        dados_api = extrair_dados_api(api_url)
        
        # Passo 5: Inserir dados extraídos da API na coleção
        inserir_dados_api(collection, dados_api)
        
        # Passo 6: Remover o documento de exemplo
        remover_exemplo(collection)
        
        # Passo 7: Contar e exibir o total de documentos na coleção
        print(f"Total de documentos na coleção: {collection.count_documents({})}")
        
        # Passo 8: Fechar a conexão com o MongoDB
        client.close()

# Executando o pipeline de dados
if __name__ == "__main__":
    # Definindo as variáveis principais (URI, nome do banco de dados, nome da coleção, e URL da API)
    uri = "mongodb+srv://matheus_cavalheiro:12345@cluster0-pipeline.5jqpo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0-pipeline"
    db_name = "db_produtos"  # Nome do banco de dados
    collection_name = "produtos"  # Nome da coleção
    api_url = "https://labdados.com/produtos"  # URL da API para extração de dados
    
    # Executa o pipeline de dados completo
    pipeline_dados(uri, db_name, collection_name, api_url)
