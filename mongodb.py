import pandas as pd
import pymongo
from numpy import NaN

client = pymongo.MongoClient("mongodb://localhost:27017")

df = pd.read_excel("teste-ao3-dataset-vacinacao-covid19.xlsx")

dt = pd.DataFrame(df)

data = df.to_dict(orient="records")


dblist = client.list_database_names()
if "ao3_teste" in dblist:
    print("O banco de dados ao3_teste já existe, portanto nao foi criado.")
else:
    db = client["ao3_teste"]
    print("Iniciando procedimento de criacao do banco de dados ao3_teste. (1/4)")
    print("O banco de dados ao3_teste foi criado. (1/4)")
    db.vacinacao.insert_many(data)
    print("Iniciando procedimento de insercao dos dados dentro da colecao vacinacao do banco de dados ao3_teste. (2/4)")
    print("Os dados foram inseridos dentro da colecao vacinacao do banco de dados ao3_teste. (2/4)")


mydb = client["ao3_teste"]
mycol = mydb["vacinacao"]

for i in df.index:
    x = mycol.count_documents({"document_id": df.loc[i][0]})
    if x>1:
        mycol.delete_one({"document_id":df.loc[i][0]})
        print("Dados duplicados deletados. (3/4)")

print("Iniciando procedimento de atualizacao dos dados. (4/4)")
vacina_descricao_dose = {"vacina_descricao_dose": "1ÂªÂ Dose"}
newvalues = {"$set": {"vacina_descricao_dose": "Pimeira Dose"}}
mycol.update_many(vacina_descricao_dose, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "Faixa EtÃ¡ria"}
newvalues = {"$set": {"vacina_categoria_nome": "Faixa Etaria"}}
mycol.update_many(vacina_categoria_nome, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "Trabalhadores da EducaÃ§Ã£o"}
newvalues = {"$set": {"vacina_categoria_nome": "Trabalhadores da Educacao"}}
mycol.update_many(vacina_categoria_nome, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "Trabalhadores de SaÃºde"}
newvalues = {"$set": {"vacina_categoria_nome": "Trabalhadores de Saude"}}
mycol.update_many(vacina_categoria_nome, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "Trabalhadores PortuÃ¡rios"}
newvalues = {"$set": {"vacina_categoria_nome": "Trabalhadores Portuarios"}}
mycol.update_many(vacina_categoria_nome, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "PopulaÃ§Ã£o Privada de Liberdade"}
newvalues = {"$set": {"vacina_categoria_nome": "Populacao Privada de Liberdade"}}
mycol.update_many(vacina_categoria_nome, newvalues)
vacina_categoria_nome = {"vacina_categoria_nome": "Pessoas com DeficiÃªncia"}
newvalues = {"$set": {"vacina_categoria_nome": "Pessoas com Deficiencia"}}
mycol.update_many(vacina_categoria_nome, newvalues)
sistema_origem = {"sistema_origem": "IDS SaÃºde"}
newvalues = {"$set": {"sistema_origem": "IDS Saude"}}
mycol.update_many(sistema_origem, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "TÃ©cnico de Enfermagem"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Tecnico de Enfermagem"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "Obesidade Grave (Imcâ‰¥40)"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Obesidade Grave (Imc>=40)"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "Ensino BÃ¡sico"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Ensino Basico"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {
    "vacina_grupoatendimento_nome": "HipertensÃ£o de difÃ­cil controle ou com complicaÃ§Ãµes/lesÃ£o de Ã³rgÃ£o alvo"}
newvalues = {
    "$set": {"vacina_grupoatendimento_nome": "Hipertensao de dificil controle ou com complicacoes/lesao de Orgao alvo"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "AÃ©reo"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Aereo"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "DoenÃ§as Cardiovasculares e Cerebrovasculares"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Doenças Cardiovasculares e Cerebrovasculares"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {"vacina_grupoatendimento_nome": "Pneumopatias CrÃ´nicas Graves"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Pneumopatias Cronicas Graves"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {
    "vacina_grupoatendimento_nome": "Coletivo RodoviÃ¡rio Passageiros Urbano e de Longo Curso"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Coletivo Rodoviario Passageiros Urbano e de Longo Curso"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)
vacina_grupoatendimento_nome = {
    "vacina_grupoatendimento_nome": "AcadÃªmicos/estudantes em estÃ¡gio em estabelecimentos de saÃºde"}
newvalues = {"$set": {"vacina_grupoatendimento_nome": "Academicos/estudantes em estagio em estabelecimentos de saude"}}
mycol.update_many(vacina_grupoatendimento_nome, newvalues)

vacina_fabricante_referencia = {"vacina_fabricante_nome": "FUNDACAO BUTANTAN"}
newvalues = {"$set": {"vacina_fabricante_referencia": "Organization/61189445000156"}}
mycol.update_many(vacina_fabricante_referencia, newvalues)
vacina_fabricante_referencia = {"vacina_fabricante_nome": "FUNDACAO OSWALDO CRUZ"}
newvalues = {"$set": {"vacina_fabricante_referencia": "Organization/33781055000135"}}
mycol.update_many(vacina_fabricante_referencia, newvalues)
vacina_fabricante_referencia = {"vacina_fabricante_nome": "MINISTERIO DA SAUDE"}
newvalues = {"$set": {"vacina_fabricante_referencia": "Organization/00394544000851"}}

vacina_fabricante_referencia = {"vacina_fabricante_referencia":NaN}
newvalues = {"$set": {"vacina_fabricante_referencia": "NAO INFORMADO"}}
mycol.update_many(vacina_fabricante_referencia, newvalues)
paciente_endereco_copais = {"paciente_endereco_copais":NaN}
newvalues = {"$set": {"paciente_endereco_copais": 10}}
mycol.update_many(paciente_endereco_copais, newvalues)
paciente_endereco_cep = {"paciente_endereco_cep":NaN}
newvalues = {"$set": {"paciente_endereco_cep": "NAO INFORMADO"}}
mycol.update_many(paciente_endereco_cep, newvalues)
paciente_endereco_uf = {"paciente_endereco_uf":NaN}
newvalues = {"$set": {"paciente_endereco_uf": "NAO INFORMADO"}}
mycol.update_many(paciente_endereco_uf, newvalues)
paciente_endereco_nmpais = {"paciente_endereco_nmpais":NaN}
newvalues = {"$set": {"paciente_endereco_nmpais": "BRASIL"}}
mycol.update_many(paciente_endereco_nmpais, newvalues)
paciente_endereco_nmmunicipio = {"paciente_endereco_nmmunicipio":NaN}
newvalues = {"$set": {"paciente_endereco_nmmunicipio": "NAO INFORMADO"}}
mycol.update_many(paciente_endereco_nmmunicipio, newvalues)
paciente_endereco_coibgemunicipio = {"paciente_endereco_coibgemunicipio":NaN}
newvalues = {"$set": {"paciente_endereco_coibgemunicipio": "NAO INFORMADO"}}
mycol.update_many(paciente_endereco_coibgemunicipio, newvalues)
print("Encerrando procedimento de atualizacao dos dados. (4/4)")