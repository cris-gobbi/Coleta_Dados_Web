Esse código cria uma API que consulta dados do site da EMBRAPA - mas pode ser adaptado para outros tipos de site.

O site contem dados sobre produção, processamento, comercialização, importação e exportação de uvas ano a ano.
O código consulta esses dados  ano a ano salvando os dados em arquivos parquet no diretório C do computador.

Abaixo existe um DE-PARA ou dicionário de dados para os PARQUETs ou BASES de Processamento, Importação e Expostação.
Para essas bases o PARQUET terá um campo chamado subopção que se refere ao filtro de seleção da página sobre o tipo de Uva ou Vinho


**Base de processamento:**


subopt_01 = Viníferas

subopt_02 = Americanas e Hibridas

subopt_03 = Uvas de mesa

subopt_04 = Sem classificação


**Base de importação:**


subopt_01 = Vinhos de Mesa

subopt_02 = Espumantes

subopt_03 = Uvas Frescas

subopt_04 = Uvas Passas

subopt_05 = Suco de Uva


**Base de exportação:**


subopt_01 = Vinhos de Mesa

subopt_02 = Espumantes

subopt_03 = Uvas Frescas

subopt_04 = Suco de Uva

