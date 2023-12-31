CREATE TABLE registros.registros_vendas (
    timestamp_do_registro timestamp without time zone ENCODE az64,
    turno_compra character varying(256) ENCODE lzo,
    nome character varying(256) ENCODE lzo,
    sobrenome character varying(256) ENCODE lzo,
    nome_completo character varying(256) ENCODE lzo,
    email character varying(256) ENCODE lzo,
    dominio_email character varying(256) ENCODE lzo,
    telefone character varying(256) ENCODE lzo,
    produto_adquirido character varying(256) ENCODE lzo,
    valor_da_compra real ENCODE raw,
    desconto_oferecido double precision ENCODE raw,
    valor_com_desconto double precision ENCODE raw,
    canal character varying(256) ENCODE lzo,
    plataforma_de_interacao character varying(256) ENCODE lzo,
    campanha_de_marketing character varying(256) ENCODE lzo,
    vendedor character varying(256) ENCODE lzo,
    codigo_de_desconto_usado character varying(256) ENCODE lzo,
    arquivo_de_origem character varying(256) ENCODE lzo,
    ano integer ENCODE raw
) DISTSTYLE AUTO;
