-- Modelagem de dados - tabelas dim e tabela fato
-- Sobre gerar a PK : https://stackoverflow.com/questions/29220956/redshift-psql-auto-increment-on-even-number

-- Tabela FATO depende das tabelas DIM. Tem que ser dropada antes das outras serem dropadas
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.fato_vendas;

-- TABLE registros.dim_CLIENTE
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_cliente;

-- Criar a tabela com PK
CREATE TABLE registros.dim_cliente (
    cliente_id bigint identity(0, 1),
    nome_completo VARCHAR(255),
    email VARCHAR(255),
    dominio_email VARCHAR(100),
    telefone VARCHAR(50),
    primary key (cliente_id)
    );

    INSERT INTO registros.dim_cliente (nome_completo, email, dominio_email, telefone)
SELECT DISTINCT nome_completo, email, dominio_email, telefone
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
-- Select * from registros.dim_cliente;

-- TABLE registros.dim_PRODUTO
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_produto;

-- Criar a tabela com PK
CREATE TABLE registros.dim_produto (
    produto_id bigint identity(0, 1),
    produto_adquirido VARCHAR(255),
    primary key (produto_id)
);

INSERT INTO registros.dim_produto (produto_adquirido)
SELECT DISTINCT produto_adquirido
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
-- select * from registros.dim_produto;

-- TABLE registros.dim_CANAL
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_canal;

-- Criar a tabela com PK
CREATE TABLE registros.dim_canal (
    canal_id bigint identity(0, 1),
    canal VARCHAR(255),
    primary key (canal_id)
);

INSERT INTO registros.dim_canal (canal)
SELECT DISTINCT canal
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
-- select * from registros.dim_canal;

-- TABLE registros.dim_PLATAFORMA
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_plataforma;

-- Criar a tabela com PK
CREATE TABLE registros.dim_plataforma (
    plataforma_id bigint identity(0, 1),
    plataforma_de_interacao VARCHAR(255),
    primary key (plataforma_id)
    
);

INSERT INTO registros.dim_plataforma (plataforma_de_interacao)
SELECT DISTINCT plataforma_de_interacao
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
--select * from registros.dim_plataforma;

-- TABLE registros.dim_CAMPANHA
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_campanha;
CREATE TABLE registros.dim_campanha (
    campanha_id bigint identity(0, 1),
    campanha_de_marketing VARCHAR(255),
    primary key (campanha_id)
);

INSERT INTO registros.dim_campanha (campanha_de_marketing)
SELECT DISTINCT campanha_de_marketing
FROM  registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
-- select * from registros.dim_campanha;

-- TABLE registros.dim_VENDEDOR
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_vendedor;
CREATE TABLE registros.dim_vendedor (
    vendedor_id bigint identity(0, 1),
    vendedor VARCHAR(255),
    primary key (vendedor_id)
);

INSERT INTO registros.dim_vendedor (vendedor)
SELECT DISTINCT vendedor
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
-- select * from registros.dim_vendedor;

-- TABLE registros.dim_DESCONTO
-- Verificar se a tabela existe e excluir se existir
DROP TABLE IF EXISTS registros.dim_desconto;
CREATE TABLE registros.dim_desconto (
    desconto_id bigint identity(0, 1),
    codigo_de_desconto_usado VARCHAR(50),
    desconto_oferecido FLOAT,
    primary key (desconto_id)
);

INSERT INTO registros.dim_desconto (codigo_de_desconto_usado, desconto_oferecido)
SELECT DISTINCT codigo_de_desconto_usado, desconto_oferecido
FROM registros.registros_vendas;

-- Descomente para testar o resultado da tabela criada acima
 select * from registros.dim_desconto;

-- TABLE registros.fato_VENDAS
CREATE TABLE registros.fato_vendas (
    venda_id bigint identity(0, 1),
    timestamp_do_registro TIMESTAMP,
    ano VARCHAR(4),
    cliente_id INT REFERENCES registros.dim_cliente(cliente_id),           --FK
    produto_id INT REFERENCES registros.dim_produto(produto_id),           --FK
    canal_id INT REFERENCES registros.dim_canal(canal_id),                 --FK
    plataforma_id INT REFERENCES registros.dim_plataforma(plataforma_id),  --FK
    campanha_id INT REFERENCES registros.dim_campanha(campanha_id),        --FK
    vendedor_id INT REFERENCES registros.dim_vendedor(vendedor_id),        --FK
    desconto_id INT REFERENCES registros.dim_desconto(desconto_id),        --FK
    valor_da_compra FLOAT,
    turno_compra VARCHAR(50),
    valor_com_desconto FLOAT,
    arquivo_de_origem VARCHAR(200),
    primary key(venda_id)
);

INSERT INTO registros.fato_vendas (
    timestamp_do_registro, ano, cliente_id, produto_id, canal_id,
    plataforma_id, campanha_id, vendedor_id, desconto_id,
    valor_da_compra, turno_compra, valor_com_desconto , arquivo_de_origem
)
SELECT 
    r.timestamp_do_registro, 
    r.ano,
    c.cliente_id,
    p.produto_id,
    cv.canal_id,
    pl.plataforma_id,
    cm.campanha_id,
    v.vendedor_id,
    d.desconto_id,
    r.valor_da_compra,
    r.turno_compra,
    r.valor_com_desconto,
    r.arquivo_de_origem
FROM 
    registros.registros_vendas AS r
JOIN registros.dim_cliente AS c ON r.nome_completo = c.nome_completo AND r.email = r.email --chave composta de join
JOIN registros.dim_produto AS p ON r.produto_adquirido = p.produto_adquirido
JOIN registros.dim_canal AS cv ON r.canal = cv.canal
JOIN registros.dim_plataforma AS pl ON r.plataforma_de_interacao = pl.plataforma_de_interacao
JOIN registros.dim_campanha AS cm ON r.campanha_de_marketing = cm.campanha_de_marketing
JOIN registros.dim_vendedor AS v ON r.vendedor = v.vendedor
JOIN registros.dim_desconto AS d ON r.codigo_de_desconto_usado = d.codigo_de_desconto_usado;

-- Descomente para testar o resultado da tabela criada acima
-- select * from registros.fato_vendas;
