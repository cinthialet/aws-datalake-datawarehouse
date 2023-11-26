
--------- Tabela Desnormalizada registros.registros_vendas
-- Checar os 5 primeiros registros da tabela desnormalizada registros_vendas no schema registros
SELECT * FROM registros.registros_vendas limit 5;

-- Checar quantidade de registros da tabela desnormalizada
SELECT COUNT(*) FROM registros.registros_vendas;

-- Checar quantos registros por arquivo de origem da tabela desnormalizada ('value_counts')
SELECT arquivo_de_origem, COUNT(arquivo_de_origem)  FROM registros.registros_vendas GROUP by arquivo_de_origem;


--------- Tabelas Normalizada (Modelagem de dados estrela - FATO, DIM)
-- Checar 5 primeiros registros da tabela FATO de vendas
SELECT * FROM registros.fato_vendas limit 5;

-- Checar quantidade de registros da tabela FATO de vendas
SELECT COUNT(*) FROM registros.fato_vendas;

-- Checar quantos registros por arquivo de origem da tabela FATO de vendas
SELECT arquivo_de_origem, COUNT(arquivo_de_origem)  FROM registros.fato_vendas GROUP by arquivo_de_origem;

-- Checar unicidade da chave primaria de uma das tabelas DIM
SELECT * FROM registros.dim_vendedor;

-- Checar contagem de registros da tabela DIM clientes
SELECT count(*) FROM registros.dim_cliente;

-- Checar contagem de valores UNICOS na coluna cliente_id (chave primária) da tabela DIM clientes
-- Deve ser o mesmo valor da query anterior 
SELECT count(distinct cliente_id) FROM registros.dim_cliente;

--------------Apagar dados e tabelas

-- Apagar todos os dados da tabela desnormalizada (mantém a tabela criada, com schema)
TRUNCATE TABLE registros.registros_vendas;

-- Apagar todas as tabelas normalizadas
DROP TABLE registros.fato_vendas;
DROP TABLE registros.dim_cliente;
DROP TABLE registros.dim_produto;
DROP TABLE registros.dim_canal;
DROP TABLE registros.dim_plataforma;
DROP TABLE registros.dim_campanha;
DROP TABLE registros.dim_vendedor;
DROP TABLE registros.dim_desconto;

