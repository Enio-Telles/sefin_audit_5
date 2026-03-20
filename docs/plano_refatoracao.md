# **Plano Detalhado de Refatoração e Implementação do Novo Modelo de Dados Fiscais**

Este documento detalha o plano de ação para substituir a extração de dados brutos (espelhos do Oracle como C100, C170, NFe) por um modelo de dados consolidado e analítico, além de reorganizar toda a estrutura do projeto para seguir boas práticas de engenharia de software (Clean Architecture/MVC).

## **1\. Visão Geral da Mudança de Arquitetura**

O sistema atual faz uma extração 1:1 das tabelas do Oracle e as salva em arquivos .parquet isolados. A interface gráfica (App) precisa lidar com a complexidade de cruzar esses dados e cruzar com tabelas de referência.

**O Novo Paradigma:** O pipeline de engenharia de dados (ETL) assumirá a responsabilidade de cruzar e consolidar os dados do Oracle com as **tabelas de referência (ex: CO\_SEFIN, CST, CFOP)**, gerando **Tabelas de Negócio** altamente enriquecidas (ex: *Tabela de Produtos*, *Tabela de Documentos Fiscais*, *Tabela de Itens Consolidada*). O App servirá apenas para visualizar e interagir com essas tabelas já prontas para auditoria.

### **1.1 Diagrama do Novo Fluxo de Dados (ETL)**

graph TD  
    subgraph 1\. Fontes de Dados (Oracle & Locais)  
        O\_C100\[(SPED C100)\]  
        O\_C170\[(SPED C170)\]  
        O\_NFE\[(XML NFe/NFCe)\]  
        R\_SEFIN\[(Ref. CO\_SEFIN)\]  
    end

    subgraph 2\. Camada de Extração (Python)  
        E\_FUNC\[conectar\_banco\_oracle\]  
        E\_READ\[ler\_consultas\_sql\]  
        E\_REF\[carregar\_referencias\_parquet\]  
        E\_FUNC \--\> E\_READ  
    end

    subgraph 3\. Camada de Transformação (Novo Core)  
        T\_DOC\[processar\_tabela\_documentos\]  
        T\_ITENS\[processar\_tabela\_itens\_enriquecida\]  
        T\_PROD\[processar\_tabela\_produtos\]  
        T\_SEFIN\[enriquecer\_co\_sefin\_e\_fatores\]  
          
        E\_READ \--\>|C100 \+ NFe| T\_DOC  
        E\_READ \--\>|C170 \+ XML Itens| T\_ITENS  
        E\_READ \--\>|0200 \+ Tabelas Ref| T\_PROD  
          
        T\_ITENS \--\> T\_SEFIN  
        E\_REF \--\>|sitafe\_produto\_sefin| T\_SEFIN  
        T\_SEFIN \--\>|Itens \+ Fator Conv. \+ SEFIN| T\_ITENS\_FINAL\[Finalizar Tb. Itens\]  
    end

    subgraph 4\. Camada de Armazenamento  
        P\_DOC\[(tb\_documentos.parquet)\]  
        P\_ITENS\[(tb\_itens\_consolidada.parquet)\]  
        P\_PROD\[(tb\_produtos.parquet)\]  
          
        T\_DOC \--\> P\_DOC  
        T\_ITENS\_FINAL \--\> P\_ITENS  
        T\_PROD \--\> P\_PROD  
    end

    subgraph 5\. Interface Gráfica (App PyQt)  
        UI\[Painel de Auditoria Fiscal\]  
        P\_DOC \-.-\> UI  
        P\_ITENS \-.-\> UI  
        P\_PROD \-.-\> UI  
    end

## **2\. Reorganização da Estrutura de Diretórios**

O projeto atual possui scripts soltos e pastas redundantes (funcoes/, Sistema-Monitoramento/, fiscal\_app/). A nova estrutura unificará tudo sob o diretório raiz.

### **2.1 Diagrama de Diretórios (Antes x Depois)**

graph LR  
    subgraph Estrutura Atual (Confusa)  
        A1\[funcoes/\] \--\> A2\[app.py\]  
        A1 \--\> A3\[Sistema-Monitoramento/\]  
        A3 \--\> A4\[fiscal\_app/\]  
        A1 \--\> A5\[funcoes\_auxiliares/\]  
        A1 \--\> A6\[funcoes\_tabelas/\]  
    end

    subgraph Nova Estrutura (Organizada)  
        N1\[projeto\_auditoria/\] \--\> N2\[dados/\]  
        N1 \--\> N3\[sql/\]  
        N1 \--\> N4\[src/\]  
        N4 \--\> N4a\[extracao/\]  
        N4 \--\> N4b\[transformacao/\]  
        N4 \--\> N4c\[interface\_grafica/\]  
        N4 \--\> N4d\[utilitarios/\]  
        N1 \--\> N5\[testes/\]  
    end

## **3\. Fases de Implementação**

### **Fase 1: Mapeamento "De \-\> Para" (Semanas 1-2)**

Antes de alterar o código, documentaremos exatamente como as novas tabelas serão preenchidas.

1. **Criar Dicionário de Dados:** Mapear cada campo da nova tabela solicitada para sua origem no Oracle ou tabela de referência.  
   * *Exemplo Base:* Tb\_Itens.Valor\_Total virá de C170.VL\_ITEM ou NFe.vProd.  
   * *Exemplo Novos Campos:* Mapear de onde virão co\_sefin, co\_sefin\_desc, co\_sefin\_ncm, co\_sefin\_cest cruzando o NCM/CEST do item com a base sitafe\_produto\_sefin.parquet e sitafe\_cest\_ncm.parquet.  
   * *Fator de Conversão:* Estabelecer a regra de negócio para a coluna fator\_conversao baseada nas tabelas de conversão de unidades.  
2. **Ajuste das Consultas SQL:** Reescrever os arquivos dentro de /sql (c100.sql, c170.sql, etc.) para trazerem apenas as colunas estritamente necessárias para as novas tabelas, otimizando o tempo de consulta.

### **Fase 2: Construção da Camada de Transformação (Semanas 2-3)**

Criar os scripts Python que farão a conversão dos dados extraídos para o novo modelo consolidado.

**Principais Funções a serem desenvolvidas (em src/transformacao/):**

* processar\_tabela\_documentos(df\_c100, df\_nfe, df\_nfce): Consolida cabeçalhos de notas.  
* processar\_tabela\_produtos(df\_0200, df\_descricoes): Agrupa códigos internos, NCM, CEST e descrições únicas.  
* processar\_tabela\_itens(df\_c170, df\_nfe\_itens): Consolida a movimentação linha a linha, agrupando CFOP, CST, Quantidades e Valores base de ICMS/ST.  
* enriquecer\_itens\_com\_referencias(df\_itens): **\[NOVO\]** Pega a tabela de itens consolidada e faz o JOIN/Merge com as tabelas de referência para preencher as colunas co\_sefin, co\_sefin\_desc, co\_sefin\_ncm, co\_sefin\_cest e aplica a função para calcular o fator\_conversao.

### **Fase 3: Refatoração do Pipeline Principal (Semana 4\)**

Substituir o atual pipeline\_oracle\_parquet.py pelo novo fluxo orquestrado.

**Estrutura do Novo Pipeline (src/orquestrador\_pipeline.py):**

def executar\_pipeline\_completo(cnpj\_alvo: str, data\_inicio: str, data\_fim: str):  
    \# 1\. Extração  
    dados\_brutos \= extrair\_dados\_fiscais\_oracle(cnpj\_alvo, data\_inicio, data\_fim)  
    dados\_referencia \= carregar\_tabelas\_referencia()  
      
    \# 2\. Transformação Básica  
    tb\_documentos \= processar\_tabela\_documentos(dados\_brutos)  
    tb\_itens\_base \= processar\_tabela\_itens(dados\_brutos)  
    tb\_produtos \= processar\_tabela\_produtos(dados\_brutos)  
      
    \# 3\. Enriquecimento de Dados (Foco Analítico e SEFIN)  
    tb\_itens\_final \= enriquecer\_itens\_com\_referencias(tb\_itens\_base, dados\_referencia)  
      
    \# 4\. Validação e Carga  
    validar\_consistencia\_dados(tb\_documentos, tb\_itens\_final)  
    salvar\_arquivos\_parquet(cnpj\_alvo, tb\_documentos, "tb\_documentos")  
    salvar\_arquivos\_parquet(cnpj\_alvo, tb\_itens\_final, "tb\_itens\_consolidada")  
    \# ...

### **Fase 4: Atualização da Interface Gráfica / App (Semana 5\)**

O front-end (fiscal\_app/ui/) e os serviços locais precisarão ser religados aos novos arquivos Parquet.

1. **Atualizar Modelos (models/table\_model.py):** Modificar o cabeçalho das tabelas (QtTableView) para refletirem as novas colunas como co\_sefin\_desc e fator\_conversao.  
2. **Refatorar Filtros (services/aggregation\_service.py):** Adaptar as buscas para apontarem para as colunas consolidadas da nova arquitetura. Agora, filtrar por "Produto SEFIN" será um filtro direto na tabela, sem necessidade de joins em tempo de execução no App.  
3. **Atualizar Relatórios (exportar\_excel.py):** Garantir que a função exportar\_planilha\_excel() gere relatórios baseados no novo layout amplo, incluindo as novas métricas analíticas.

### **Fase 5: Testes, Homologação e Limpeza (Semana 6\)**

Garantir que os valores batem e apagar código morto.

1. **Testes de Regressão:** Comparar os totais de ICMS, PIS, COFINS e Base de Cálculo do modelo antigo com o modelo novo para um mesmo CNPJ de amostra. Testar especificamente se os de-paras de co\_sefin não estão gerando linhas duplicadas (explosão de joins).  
2. **Limpeza (Sunset):** Excluir permanentemente os diretórios antigos, arquivos .pyc soltos e scripts obsoletos.  
3. **Atualização do README.md:** Documentar como rodar o novo pipeline, a nova estrutura de pastas e como adicionar novas regras de negócio.

## **4\. Dicionário Base de Funções (Português \- BR)**

Para manter a padronização e facilitar a manutenção, o projeto utilizará os seguintes padrões de nomenclatura:

| Módulo/Ação | Nome Proposto da Função | Objetivo |
| :---- | :---- | :---- |
| **Banco de Dados** | conectar\_banco\_oracle() | Cria e retorna a engine do SQLAlchemy/cx\_Oracle. |
| **Banco de Dados** | executar\_consulta\_sql(caminho) | Lê um arquivo .sql e executa retornando um DataFrame Pandas. |
| **Transformação** | calcular\_mva\_ajustado(...) | Aplica as regras tributárias para encontrar a MVA. |
| **Transformação** | calcular\_fator\_conversao(...) | Calcula o multiplicador para uniformizar quantidades baseadas na unidade (UN, CX, KG). |
| **Transformação** | mapear\_codigos\_sefin(...) | Realiza o *merge* do DataFrame com as referências SITAFE para trazer descrições e códigos oficias SEFIN. |
| **Armazenamento** | salvar\_arquivos\_parquet(...) | Salva o DataFrame no formato colunar comprimido. |
| **Utilitários** | formatar\_cnpj\_cpf(doc) | Adiciona pontuação (se necessário) ou limpa caracteres. |
| **Utilitários** | validar\_consistencia\_dados() | Verifica se há itens órfãos e previne explosões de JOIN no enriquecimento. |

