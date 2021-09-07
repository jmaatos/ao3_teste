import pandas as pd
import psycopg2


p3 = pd.read_excel('teste-ao3-dataset-vacinacao-covid19.xlsx')
df = pd.DataFrame(p3)


# Função para criar conexão no banco
def conecta_db():
  con = psycopg2.connect(host='localhost',
                         database='db_vacinacao',
                         user='postgres',
                         password='Joao@99120431')
  return con


# criacao da tabela
con = conecta_db()
cur = con.cursor()
sql = """
drop view IF EXISTS qtd_dosesaplicadas_nome;
drop view IF EXISTS vacinas_porgrupo;
drop view IF EXISTS qtd_paciente_sexo;
drop view IF EXISTS qtd_vacinaspor_municipio;
DROP TABLE IF EXISTS public.vacinacao_covid19;
create table vacinacao_covid19(
document_id varchar(200) not null primary key,
paciente_id varchar(200) not null,
paciente_idade integer,
paciente_datanascimento date,
paciente_enumsexobiologico char(1),
paciente_racacor_codigo integer,
paciente_racacor_valor varchar(20),
paciente_endereco_coibgemunicipio varchar(20),
paciente_endereco_copais varchar(100),
paciente_endereco_nmmunicipio varchar(100),
paciente_endereco_nmpais varchar(100),
paciente_endereco_uf varchar(100),
paciente_endereco_cep varchar(30),
paciente_nacionalidade_enumnacionalidade varchar(100),
estabelecimento_valor varchar(100),
estabelecimento_razaosocial varchar(100),
estalecimento_nofantasia varchar(100),
estabelecimento_municipio_codigo varchar(100),
estabelecimento_municipio_nome varchar(100),
estabelecimento_uf varchar(5),
vacina_grupoatendimento_codigo varchar(20),
vacina_grupoatendimento_nome varchar(100),
vacina_categoria_codigo varchar(5),
vacina_categoria_nome varchar(100),
vacina_lote varchar(100),
vacina_fabricante_nome varchar(100),
vacina_fabricante_referencia varchar(100),
vacina_dataaplicacao date,
vacina_descricao_dose varchar(100),
vacina_codigo varchar(100),
vacina_nome varchar(100),
sistema_origem varchar(100),
data_importacao_rnds timestamp,
id_sistema_origem varchar(100)
);"""
cur.execute(sql)
con.commit()
con.close()
print("\n\nProcedimento de criacao da tabela finalizado!")


#insert/update
print("\n\nIniciando procedimento de insercao de dados...")

for i in df.index:
    con = conecta_db()
    cur = con.cursor()
    try:
        txt1 = str(p3.loc[i][9])
        txt2 = str(p3.loc[i][18])
        x = txt1.replace("'", "")
        y = txt2.replace("'", "")
        sql = """insert into vacinacao_covid19 (document_id,paciente_id,paciente_idade,paciente_datanascimento,paciente_enumsexobiologico,paciente_racacor_codigo,paciente_racacor_valor,paciente_endereco_coibgemunicipio,paciente_endereco_copais,paciente_endereco_nmmunicipio,paciente_endereco_nmpais,paciente_endereco_uf,paciente_endereco_cep,paciente_nacionalidade_enumnacionalidade,estabelecimento_valor,estabelecimento_razaosocial,estalecimento_nofantasia,estabelecimento_municipio_codigo,estabelecimento_municipio_nome,estabelecimento_uf,vacina_grupoatendimento_codigo,vacina_grupoatendimento_nome,vacina_categoria_codigo,vacina_categoria_nome,vacina_lote,vacina_fabricante_nome,vacina_fabricante_referencia,vacina_dataaplicacao,vacina_descricao_dose,vacina_codigo,vacina_nome,sistema_origem,data_importacao_rnds,id_sistema_origem)
                values ('"""+str(p3.loc[i][0])+"""','"""+str(p3.loc[i][1])+"""','"""+str(p3.loc[i][2])+"""','"""+str(p3.loc[i][3])+"""','"""+str(p3.loc[i][4])+"""','"""+str(p3.loc[i][5])+"""','"""+str(p3.loc[i][6])+"""','"""+str(p3.loc[i][7])+"""','"""+str(p3.loc[i][8])+"""','"""+x+"""','"""+str(p3.loc[i][10])+"""','"""+str(p3.loc[i][11])+"""','"""+str(p3.loc[i][12])+"""','"""+str(p3.loc[i][13])+"""','"""+str(p3.loc[i][14])+"""','"""+str(p3.loc[i][15])+"""','"""+str(p3.loc[i][16])+"""','"""+str(p3.loc[i][17])+"""','"""+y+"""','"""+str(p3.loc[i][19])+"""','"""+str(p3.loc[i][20])+"""','"""+str(p3.loc[i][21])+"""','"""+str(p3.loc[i][22])+"""','"""+str(p3.loc[i][23])+"""','"""+str(p3.loc[i][24])+"""','"""+str(p3.loc[i][25])+"""','"""+str(p3.loc[i][26])+"""','"""+str(p3.loc[i][27])+"""','"""+str(p3.loc[i][28])+"""','"""+str(p3.loc[i][29])+"""','"""+str(p3.loc[i][30])+"""','"""+str(p3.loc[i][31])+"""','"""+str(p3.loc[i][32])+"""','"""+str(p3.loc[i][33])+"""');
                update vacinacao_covid19
                set vacina_descricao_dose = replace(vacina_descricao_dose,'1ÂªÂ','Primeira');
                update vacinacao_covid19
                set vacina_categoria_nome = replace(replace(replace(replace(replace(
                vacina_categoria_nome,'Ã¡','a'),'Ã§','c'),'Ã£','a'),'Ãº','u'),'Ãª','e');
                update vacinacao_covid19
                set vacina_grupoatendimento_nome = replace(replace(replace(replace(replace(replace(replace(replace(replace
                (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                vacina_grupoatendimento_nome,'Ã©','e'),'Ã£','a'),'Ã','i'),'Ã³','o'),'Ã§','c'),'Ãµ','o'),'Ãª','e'),'Ã¡','a'),'Ãº','u'),'Ã´','o'),'â‰¥','>='),'iª','e'),'iº','u'),'i§','c'),'i¡','a'),'iµ','o'),'i³','o'),'iª','e'),'i´','o');
                update vacinacao_covid19
                set sistema_origem = replace(sistema_origem,'Ãº','u');

                update vacinacao_covid19
                set vacina_fabricante_referencia = 'Organization/33781055000135'
                where vacina_fabricante_nome = 'FUNDACAO OSWALDO CRUZ';
                update vacinacao_covid19
                set vacina_fabricante_referencia = 'Organization/61189445000156'
                where vacina_fabricante_nome = 'FUNDACAO BUTANTAN';
                update vacinacao_covid19
                set vacina_fabricante_referencia = 'Organization/00394544000851'
                where vacina_fabricante_nome = 'MINISTERIO DA SAUDE';
                update vacinacao_covid19
                set vacina_fabricante_referencia = 'NAO INFORMADO'
                where vacina_fabricante_referencia = 'nan';

                update vacinacao_covid19
                set paciente_endereco_cep = 'NAO INFORMADO'
                where paciente_endereco_cep = 'nan';
                update vacinacao_covid19
                set paciente_endereco_uf = 'NAO INFORMADO'
                where paciente_endereco_uf = 'nan';
                update vacinacao_covid19
                set paciente_endereco_nmpais = 'BRASIL'
                where paciente_endereco_nmpais = 'nan';
                update vacinacao_covid19
                set paciente_endereco_copais = '10'
                where paciente_endereco_copais = 'nan';
                update vacinacao_covid19
                set paciente_endereco_nmmunicipio = 'NAO INFORMADO'
                where paciente_endereco_nmmunicipio = 'nan';
                update vacinacao_covid19
                set paciente_endereco_coibgemunicipio = 'NAO INFORMADO'
                where paciente_endereco_coibgemunicipio = 'nan';

                """
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
print("\n\nProcedimento de insert e update finalizado!")


#views
con = conecta_db()
cur = con.cursor()
sql = """
create view vacinas_porgrupo as
select vacina_grupoatendimento_nome, count(*) qtd
from vacinacao_covid19
group by vacina_grupoatendimento_nome
order by 2 desc;

create view qtd_dosesaplicadas_nome as
select vacina_nome, count(*) qtd
from vacinacao_covid19
group by vacina_nome
order by 2 desc;

create view qtd_paciente_sexo as
select paciente_enumsexobiologico, count(*) as qtd
from vacinacao_covid19
group by paciente_enumsexobiologico
order by 2 desc;

create view qtd_vacinaspor_municipio as
select estabelecimento_municipio_nome, count(*) as qtd
from vacinacao_covid19
group by estabelecimento_municipio_nome
order by 2 desc;

"""
cur.execute(sql)
con.commit()
con.close()
print("\n\nProcedimento de criacao de views finalizado!")