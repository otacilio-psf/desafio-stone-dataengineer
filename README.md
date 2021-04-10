# Desafio Stone - Data Engineer

### O desafio implica em:
1. Coletar dados de APIs publicas da Procuradoria Geral da Fazenda e do Banco Central
2. Armazenar no AWS S3 seguindo as boas práticas de governança de um data lake
3. Disponibilizar em um metastore os dados para que os mesmos sejam consumidos com linguagem SQL like
4. Criar chave única para consultar a base de dívidas e outra chave temporal para cruzamento com a base de indicadores

### Arquitetura
Visando uma arquitetura cloud agnostic, preferi utilizar o Databricks uma vez que o mesmo está presente nos principais players (AWS, Azure e GCP) e proporciona um ambiente preparado para SPARK, robusto e escalável.
Utilizamos também do formato delta que nos permite transações ACID além de muitas outras vantagens como data upsert & Time Travel.
Seguindo a ideia de cloud agnostic, ao invés de utilizar a ferramenta proprietária AWS Athena, que se baseia nas tecnologias open source (Hive e Presto), utilizei o Hive para o metastore presente no environment do Databricks e o próprio Spark como engine de processamento. Porem ainda é possivel utilizar um external metastore como o AWS Glue ou uma banco MySQL estanciado de diversas formas (PAAS ou container).

![alt text](https://raw.githubusercontent.com/otacilio-psf/desafio-stone-dataengineer/7ac481fd8040bf0920f67991f3f9bf827eab3aac/architecture.jpg?token=AP5DIO2HTWNNG3LCYF7C4S3AOIFES "Arquitetura")

### Etapas
#### 1)
Para coletar os dados das APIs, utilizei o python para realizar as conexões persistir na landing
#### 2)
Uma vez os dados disponiveis na landing, utilizando SPARK movimentei o dados através das camadas bronze, silver e gold, tratando e agregando os dados de acordo com as necessidades de negócio
#### 3)
Criado a tabela no hive metastore para disponibilizar acesso SQL like para os cientistas consumirem
#### 4)
Presente na etapa 2
