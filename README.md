# Pipeline de Dados: MongoDB para MySQL 🚀

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) completo, focado na segurança de dados e boas práticas de engenharia.

## 🏗️ Arquitetura do Projeto
O pipeline extrai dados de produtos de uma API externa, armazena em um banco NoSQL (Cloud), processa as informações e carrega em um banco SQL estruturado.



1. **Extração**: Dados consumidos via API e persistidos no **MongoDB Atlas**.
2. **Transformação**: Limpeza de dados, renomeação de campos e normalização de avaliações utilizando **Pandas**.
3. **Carga**: Inserção otimizada no **MySQL** local com tratamento de duplicatas (`ON DUPLICATE KEY UPDATE`).

## 🛠️ Tecnologias Utilizadas
* **Linguagem**: Python 3.10+
* **Bancos de Dados**: MongoDB Atlas & MySQL
* **Bibliotecas**: `pandas`, `pymongo`, `mysql-connector-python`, `python-dotenv`

## 🔐 Segurança
As credenciais de acesso não estão expostas no código. O projeto utiliza variáveis de ambiente gerenciadas via arquivo `.env`.

### Como rodar o projeto:
1. Clone o repositório.
2. Crie um ambiente virtual: `python -m venv venv`.
3. Instale as dependências: `pip install -r requirements.txt`.
4. Configure o arquivo `.env` baseado no `.env.example`.
5. Execute os scripts na ordem:
   - `python scripts/extract_and_save_data.py`
   - `python scripts/transform_data.py`
   - `python scripts/save_data_mysql.py`