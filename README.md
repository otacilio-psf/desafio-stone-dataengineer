# Desafio Stone - Data Engineer

### O desafio implica em:
1. Coletar dados de APIs publicas da Procuradoria Geral da Fazenda e do Banco Central
2. Armazenar no AWS S3 seguindo as boas práticas de governancia de um data lake
3. Disponibilizar em um metastore os dados para que os mesmos sejam consumidos com liguagem SQL like
4. Criar chave única para consultar a base de dívidas e outra chave temporal para cruzamento com a base de indicadores

### Arquitetura
Visando uma arquitetura cloud agnostic, preferi utilizar o Databricks uma vez que o mesmo está presente nos principais players (AWS, Azure e GCP) e proporciona um ambiente preparado para SPARK, robusto e escalável.
Utilizamos também do formato delta que nos permite trasações ACID no dado, além de muitas outras vantagens como data upsert & Time Travel.
Seguindo a ideia de cloud agnostic, utilizei o Hive metastore presente no environment do Databricks, porem o mesmo pode utilizar external metastore como o Glue da AWS ou uma banco mySQL estanciado na Azure

### Etapas
#### 1)
Para coletar os dados das APIs, utilizei o python para realizar as conecções persistir na landing
#### 2)
Uma vez os dados disponiveis na landing, utilizando SPARK movimentei o dados através das camadas bronze, silver e gold, tratando e agragando os dados de acordo com as necessidades de negócio
#### 3)
Criado a tabela no hive metastore para disponibilizar acesso SQL like para os cientistas consumirem
#### 4)
Presente na etapa 2 quando o dado é levado para gold layer
