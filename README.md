# 20240218_CVM_CiasAbertas_DFP

## 1 Descrição do projeto

Programa que realiza a extração, validação e ingestão de dados
de demonstrações financeiras padronizadas (DFP) disponíveis no site da
Comissão de Valores Mobiliários (CVM). Este programa processa todos os
relatórios contábeis das empresas de capital aberto listadas na bolsa de
valores brasileira, armazenando-os em um database duckdb.

Link oficial: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp.

Link dos dicionários de dados oficiais: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp/resource/cbf44db1-06b8-45f7-8318-88d1564e9451.

## 2 Ferramentas e técnicas utilizadas

- VS Code
- Python 3.11.0b4
- duckdb

## 3 Objetivos do autor

Facilitar acesso aos dados das companhias brasileiras de capital aberto
ao público em geral.

## 4 Funcionamento do projeto

O programa foi desenvolvido utilizando uma abordagem de orientação a objetos,
organizando suas principais atividades em três módulos distintos:

1. **extract.py**:
    - Realiza a extração das pastas compactadas contendo os arquivos (.csv) dos
    relatórios contábeis.
    - Descompacta os arquivos e os salva em uma pasta local, processando uma
    pasta e seus respectivos arquivos por vez.

2. **data_quality.py**:
    - Verifica se os dados de cada arquivo estão de acordo com o dicionário de
    dados específico para o relatório contábil correspondente, conforme disponibilizado pela CVM.
    - Garante a integridade e a precisão dos dados antes de prosseguir para a
    próxima etapa.

3. **ingestion.py**:
    - Cria o banco de dados (caso ainda não exista).
    - Lê um arquivo por vez e verifica se a tabela do relatório contábil
    correspondente já existe no banco de dados.
    - Caso a tabela não exista, o programa a cria e, em seguida, realiza a
    ingestão dos dados no banco.

No arquivo de extração e ingestão há a funcionalidade de realizar carga incremental,
ou seja, verifica o que foi extraído e inserido anteriormente e apenas realizar
o trabalho nos faltantes; como também a carga total, onde tudo que já foi processado
anteriormente é apagado e todo o processo é iniciado novamente.

Por fim, temos o arquivo **main.py**, sendo o coordenador e executor dos módulos
de extração e ingestão:

- Define o tipo de carga (total ou incremental) e inicia o processo de extração
dos dados através do módulo `extract.py`.
- Coleta os arquivos de dados extraídos e os insere no banco de dados utilizando
o módulo `ingestion.py`.
- Garante o fechamento da conexão com o banco de dados após a conclusão do processo.
