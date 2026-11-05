# Arquitetura de Dados --- Projeto Medalhão

**Autora:** Lidiane Costa da Silva

Projeto demonstrativo de engenharia de dados que implementa uma
arquitetura Medalhão (*Medallion Architecture*) para ingestão,
organização, tratamento, validação e disponibilização de dados do
cenário Mercado Nordeste.

A solução utiliza **PySpark**, **MinIO**, **Apache Iceberg**,
**PostgreSQL** e **Metabase**, organizando o fluxo em camadas Bronze,
Silver e Gold.

------------------------------------------------------------------------

## Sumário

-   [Objetivo](#objetivo)
-   [Tecnologias](#tecnologias)
-   [Arquitetura e fluxo de dados](#arquitetura-e-fluxo-de-dados)
-   [Camadas da arquitetura](#camadas-da-arquitetura)
-   [Qualidade, quarentena e
    reconciliação](#qualidade-quarentena-e-reconciliação)
-   [Publicação e consumo](#publicação-e-consumo)
-   [Governança e segurança](#governança-e-segurança)
-   [Escalabilidade](#escalabilidade)
-   [Autoria](#autoria)

## Objetivo

Construir um fluxo de dados que transforme arquivos de origem em
informações organizadas e confiáveis para análise. A arquitetura busca
preservar os dados recebidos, aplicar regras explícitas de qualidade,
registrar rejeições e disponibilizar conjuntos analíticos para consulta
no Metabase.

## Tecnologias

  -----------------------------------------------------------------------
  Tecnologia                          Papel no projeto
  ----------------------------------- -----------------------------------
  Python / PySpark                    Ingestão e processamento dos dados

  MinIO                               Armazenamento de objetos e arquivos
                                      do lakehouse

  Apache Iceberg                      Formato de tabela e gerenciamento
                                      dos dados nas camadas do lakehouse

  Parquet                             Formato colunar utilizado nos
                                      arquivos de dados das tabelas
                                      Iceberg

  PostgreSQL                          Camada relacional de
                                      disponibilização dos dados Gold

  Metabase                            Consulta e visualização dos
                                      indicadores

  pgvector                            Componente previsto para
                                      funcionalidades vetoriais
  -----------------------------------------------------------------------

## Arquitetura e fluxo de dados

O fluxo começa com arquivos CSV e JSONL recebidos na Landing. O PySpark
ingere os dados para a Bronze, aplica transformações e regras na Silver,
encaminha registros inválidos para a quarentena e produz tabelas
analíticas na Gold. As tabelas Gold são publicadas no PostgreSQL para
consumo pelo Metabase.

``` mermaid
flowchart TD
    A["Origem — Mercado Nordeste<br/>clientes.csv · produtos.csv<br/>pedidos.jsonl · avaliacoes.csv"]

    subgraph MINIO["MinIO — Object Storage"]
        L["Bucket: landing<br/>Arquivos CSV / JSONL"]
        subgraph WH["Bucket: warehouse"]
            B["Bronze — Iceberg / Parquet<br/>lake.bronze"]
            S["Silver — Iceberg / Parquet<br/>lake.silver"]
            Q["Quarentena<br/>lake.silver.quarentena"]
            DQ["Relatório DQ<br/>lake.silver.dq_resultados"]
            G["Gold — Iceberg / Parquet<br/>lake.gold"]
        end
    end

    C["Contrato de dados<br/>Artefato de governança"]
    P["PySpark<br/>ingestão e transformações"]
    PG["PostgreSQL<br/>schema gold"]
    M["Metabase<br/>dashboards e consultas"]

    A --> L --> P --> B --> P
    C -. "orienta as validações" .-> P
    P --> S
    P --> Q
    P --> DQ
    S --> P --> G --> PG --> M
```

### Buckets e organização

**Bucket `landing`** --- arquivos originais:

``` text
mercadonordeste/
└── lote1/
    ├── clientes.csv
    ├── produtos.csv
    ├── pedidos.jsonl
    └── avaliacoes.csv
```

**Bucket `warehouse`** --- armazenamento das tabelas Iceberg:

``` text
lakehouse/
├── bronze/
├── silver/
└── gold/
```

As tabelas Iceberg mantêm dados e metadados próprios. Os dados são
armazenados em arquivos Parquet.

## Camadas da arquitetura

  -----------------------------------------------------------------------
  Camada            Entrada           Processamento /   Saída
                                      finalidade        
  ----------------- ----------------- ----------------- -----------------
  **Landing**       Arquivos de       Recepção dos      CSV e JSONL no
                    origem            arquivos sem      bucket `landing`
                                      transformação     

  **Bronze**        Arquivos da       Ingestão com      Tabelas Iceberg
                    Landing           preservação dos   em `lake.bronze`
                                      dados e metadados 
                                      de                
                                      rastreabilidade   

  **Silver**        Tabelas Bronze    Limpeza, tipagem, Tabelas Iceberg
                                      padronização,     tratadas em
                                      deduplicação e    `lake.silver`
                                      validações        

  **Gold**          Tabelas Silver    Agregações e      Tabelas Iceberg
                                      modelagem         em `lake.gold`
                                      orientadas ao     
                                      consumo analítico 

  **Serving**       Tabelas Gold      Publicação para   Tabelas
                                      acesso relacional PostgreSQL no
                                                        schema `gold`
  -----------------------------------------------------------------------

### Regras principais da Silver

As regras descritas no notebook incluem:

-   **Clientes:** conversão de `cliente_id` para inteiro; padronização
    de `estado` em maiúsculas; e-mail em minúsculas; e-mail inválido
    convertido para `NULL`; tratamento de datas em três formatos;
    manutenção da versão mais recente por `cliente_id`.
-   **Produtos:** `preco` e `custo` devem ser maiores que zero; valores
    fora da regra vão para a quarentena.
-   **Pedidos:** correção dos tipos, deduplicação por `pedido_id` e
    envio para quarentena quando o cliente não existe. A coluna `itens`
    é explodida para gerar `itens_pedido`.
-   **Avaliações:** `nota` deve estar entre 1 e 5; registros inválidos
    vão para a quarentena; deduplicação por `avaliacao_id`.

A Silver é reconstruída a partir de toda a Bronze, conforme descrito no
notebook, permitindo reexecução do tratamento.

## Qualidade, quarentena e reconciliação

Os registros que violam regras não devem desaparecer silenciosamente:
são direcionados à quarentena com o motivo da rejeição.

O notebook registra resultados das verificações de qualidade na tabela:

``` text
lake.silver.dq_resultados
```

Os resultados de DQ utilizam os estados **OK**, **AVISO** e **FALHA**.

A reconciliação deve considerar a granularidade de cada conjunto. Para
registros submetidos às regras de validação, a referência é:

``` text
Registros de entrada = Registros válidos + Registros em quarentena
```

Deduplicação e explosão de itens precisam ser contabilizadas
separadamente: a deduplicação remove ocorrências repetidas segundo a
chave definida, enquanto a explosão de `itens` transforma um pedido em
uma ou mais linhas de itens. Por isso, a contagem de pedidos não deve
ser comparada diretamente com a contagem de itens.

No exercício documentado, a regra que mais enviou registros para a
quarentena foi `pedidos_cliente_orfao`, com 40 registros, porque o
`cliente_id` não foi encontrado na tabela de clientes da Bronze.

## Publicação e consumo

As tabelas analíticas produzidas na Gold são:

-   `vendas_diarias` --- informações de vendas e receitas por período;
-   `clientes_360` --- visão consolidada dos clientes;
-   `avaliacoes_resumo` --- resumo das avaliações.

Essas tabelas são armazenadas em Iceberg e publicadas no PostgreSQL, no
schema `gold`. O Metabase consulta o PostgreSQL para construir
dashboards e indicadores.

### Perfis de acesso

  -----------------------------------------------------------------------
  Perfil / componente                 Responsabilidade
  ----------------------------------- -----------------------------------
  PySpark / administração             Lê e grava dados durante ingestão e
                                      processamento

  MinIO / Iceberg                     Armazena os arquivos e tabelas
                                      conforme as credenciais do serviço

  Publicador PostgreSQL               Escreve/atualiza as tabelas Gold no
                                      PostgreSQL

  Administrador Metabase              Administra conexões, perguntas e
                                      dashboards

  Usuário somente leitura             Consulta dashboards e indicadores
  -----------------------------------------------------------------------

**Configuração de segurança:** o documento de arquitetura recomenda que
o usuário de consulta do Metabase tenha somente permissão `SELECT` nas
tabelas do schema `gold`. A criação efetiva desse perfil somente leitura
deve ser configurada na implantação; não é demonstrada como concluída
nos notebooks.

## Governança e segurança

O contrato de dados é o artefato que deve definir estrutura esperada,
campos obrigatórios, tipos, formatos e regras de negócio. Ele deve
orientar as validações da Silver.

**Limite da implementação documentada:** os notebooks demonstram a
quarentena e o relatório de qualidade, mas não apresentam o contrato de
dados implementado explicitamente nem sua localização em um bucket
específico.

O documento identifica o e-mail na tabela de clientes como dado pessoal
e registra que **não foi realizado tratamento de mascaramento,
anonimização ou restrição específica**. Esse ponto deve ser considerado
uma melhoria necessária antes de utilizar dados pessoais em ambientes de
consumo.

## Escalabilidade

Para um cenário de crescimento expressivo do volume de dados, a proposta
é preservar o padrão Medalhão e evoluir a infraestrutura:

-   substituir o Spark local por um cluster Spark distribuído;
-   utilizar armazenamento de objetos escalável, como S3, OCI Object
    Storage ou MinIO distribuído;
-   manter Apache Iceberg e Parquet, com atenção ao particionamento e à
    evolução de schema;
-   implementar ingestão incremental na Bronze;
-   otimizar deduplicação e validações distribuídas na Silver;
-   pré-calcular agregações e otimizar tabelas na Gold;
-   adotar Apache Airflow para orquestração, dependências e novas
    tentativas;
-   monitorar tempo de processamento, volume ingerido, rejeições na
    quarentena, uso de CPU/memória, falhas e atrasos.

Esses itens representam propostas de evolução da arquitetura, não
necessariamente componentes já implementados no ambiente atual.

## Autoria

**Lidiane Costa da Silva**

Projeto desenvolvido para estudo e demonstração de conceitos de
Engenharia de Dados, arquitetura Medalhão, processamento distribuído,
qualidade de dados e disponibilização analítica.
