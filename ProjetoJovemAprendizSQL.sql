--> RUN
--> Run script python
--> Exec join_tables
--> Exec data_clean
--> Pronto pra uso


--> RUN para criar as tabelas

CREATE TABLE modalidades (
MOD_ID INT NOT NULL PRIMARY KEY,
MODALIDADE VARCHAR(100) NOT NULL,
TIPO VARCHAR(20) NOT NULL
)

CREATE TABLE instituicoes (
INST_ID INT NOT NULL PRIMARY KEY,
INST_NOME VARCHAR(100) NOT NULL
)

CREATE TABLE taxas (
MODALIDADE varchar(100) NOT NULL,
INST_NOME varchar(100) NOT NULL,
TAXAS_AO_ANO DECIMAL(9, 2) NOT NULL,
TAXAS_AO_MES DECIMAL(9, 2) NOT NULL,
PAIR_ID INT NOT NULL PRIMARY KEY
);

-- Dar run no script python para importar os dados


GO
CREATE PROCEDURE join_tables
 AS (select modalidades.MOD_ID, modalidades.MODALIDADE, modalidades.TIPO, taxas.INST_NOME, taxas.TAXAS_AO_ANO, taxas.TAXAS_AO_MES
  into series
  from modalidades inner join taxas on modalidades.MODALIDADE = taxas.MODALIDADE)

 (select series.MOD_ID, series.MODALIDADE, series.INST_NOME, instituicoes.INST_ID, series.TAXAS_AO_ANO, series.TAXAS_AO_MES
  into series_completas
  from instituicoes inner join series on instituicoes.INST_NOME = series.INST_NOME);
GO

-- "Exec join_tables" --> para juntar as tabelas importadas 

GO
CREATE PROCEDURE data_clean
 AS
 WITH cte AS (
    SELECT 
        MODALIDADE, 
        INST_NOME, 
        TAXAS_AO_ANO, 
        TAXAS_AO_MES,
		PAIR_ID,
        ROW_NUMBER() OVER (
            PARTITION BY 
                MODALIDADE, 
                INST_NOME, 
                TAXAS_AO_ANO, 
                TAXAS_AO_MES
            ORDER BY 
                MODALIDADE, 
                INST_NOME, 
                TAXAS_AO_ANO, 
                TAXAS_AO_MES
        ) row_num
     FROM 
        taxas
)
DELETE FROM cte
WHERE row_num > 1;
GO 

-- "Exec data_clean" -> para excluir as linhas repetidas da tabela "taxas"



-- STORED PROCEDURES -- PARÂMETROS --

CREATE PROCEDURE check_all
AS
select * from series_completas;
GO

-- Exec check_all -> para visualizar todas as séries 

CREATE PROCEDURE check_modalidades
AS
select * from modalidades;
GO

-- Exec check_modalidades -> para visulizar apenas as modalidades e seus respectivos ID's

CREATE PROCEDURE check_inst
AS
select * from instituicoes;
GO

-- Exec check_modalidades -> para visualizar apenas as instituições financeiras e seus respectivos ID's

CREATE PROCEDURE mod_by_name @Modalidade varchar(100)
AS
SELECT * FROM taxas WHERE MODALIDADE = @Modalidade;
GO

-- EXEC mod_by_name @Modalidade = <'nome da modalidade'> --> para pesquisar séries utilizando o nome de uma modalidade


CREATE PROCEDURE mod_by_id @ID INT
AS
SELECT * FROM series WHERE MOD_ID = @ID;
GO

-- EXEC mod_by_id @ID = <ID da Modalidade> --> para pesquisar séries utilizando o ID de uma modalidade

CREATE PROCEDURE series_by_inst @Inst varchar(100)
AS
SELECT * FROM taxas WHERE INST_NOME = @Inst;
GO

-- EXEC series_by_inst @Inst = <'nome da inst. financeira'> --> para pesquisar séries utilizando o nome de uma instituição financeira

CREATE PROCEDURE inst_by_id @Inst_ID INT
AS
SELECT * FROM series_completas WHERE INST_ID = @Inst_ID;
GO

-- EXEC inst_by_id @ID = <id da instituição> --> para pesquisar séries utilizando o ID de uma instituição

CREATE PROCEDURE mod_and_inst @Mod_ID INT, @Inst_ID INT
AS
SELECT * FROM series_completas WHERE MOD_ID = @Mod_ID AND INST_ID = @Inst_ID;
GO

-- EXEC mod_and_inst @Mod_ID = <id da modalidade>, @Inst_ID = <id da instituição> --> para pesquisar uma série específica 
-- utilizando o ID da modalidade e o ID da instituição

CREATE PROCEDURE check_tipo @Tipo varchar(20)
AS
SELECT * FROM series WHERE TIPO = @Tipo 
GO

-- EXEC check_tipo @Tipo = <'tipo'> --> para filtrar séries baseadas em sua natureza

--* opções podem ser "PF-TaxasDiarias", "PF-TaxasMensais" ou "PJ-TaxasMensais"





