from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime as dt
import pandas as pd

sc = SparkContext()
sc.setLogLevel("ERROR")
conf = SparkConf().setAppName('Cartola')
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# Diretórios
base = '/home/marcelio/Git/dev_local/cartola'
base_atleta = base + '/bases/atletas'
base_scout = base + '/bases/scouts'
base_time = base + '/bases/times'
base_atleta = base + '/bases/atletas'
base_posicao = base + '/bases/posicao'
base_tmp = base + '/bases/tmp'

print('\nInicio: {}\n'.format(dt.datetime.today().strftime('%m/%d/%Y, %H:%M:%S')))


##########
# Scouts
##########
try:
    df_scouts = spark.read.csv(base_scout + '/base_bruta_scouts.csv', header=True, sep=",", \
                               encoding="ISO-8859-1", inferSchema=False)

    df_scouts = df_scouts.drop('Participou','Status','avg.Points','avg.last05','avg.FS','avg.FS.l05','avg.PE',
                               'avg.PE.l05','avg.A','avg.A.l05','avg.FT','avg.FT.l05','avg.FD','avg.FD.l05','avg.FF',
                               'avg.FF.l05','avg.G','avg.G.l05','avg.I','avg.I.l05','avg.PP','avg.PP.l05','avg.RB',
                               'avg.RB.l05','avg.FC','avg.FC.l05','avg.GC','avg.GC.l05','avg.CA','avg.CV.l05',
                               'avg.SG','avg.SG.l05','avg.DD','avg.DD.l05','avg.DP','avg.DP.l05','avg.GS',
                                'avg.GS.l05','risk_points','mes','dia','home.score.x','away.score.x','pred.home.score'
                               ,'pred.away.score','home.attack','home.defend','variable', 'Apelido')

    df_scouts = df_scouts.withColumnRenamed('AtletaID','id_atleta').withColumnRenamed('Rodada','rodada') \
        .withColumnRenamed('ClubeID','id_clube').withColumnRenamed('Posicao','posicao') \
        .withColumnRenamed('Pontos','pontos').withColumnRenamed('PontosMedia','pontos_media') \
        .withColumnRenamed('Preco','preco').withColumnRenamed('PrecoVariacao','preco_variacao') \
        .withColumnRenamed('FS','falta_sofrida').withColumnRenamed('PE','passe_errado') \
        .withColumnRenamed('A','assistencia').withColumnRenamed('FT','finalizacao_trave') \
        .withColumnRenamed('FD','finalizacao_defendida') \
        .withColumnRenamed('FF','finalizacao_fora').withColumnRenamed('G','gol').withColumnRenamed('I','impedimento') \
        .withColumnRenamed('PP','penalti_perdido').withColumnRenamed('RB','roubada_bola') \
        .withColumnRenamed('FC','falta_cometida').withColumnRenamed('GC','gol_contra') \
        .withColumnRenamed('CA','cartao_amarelo').withColumnRenamed('CV','cartao_vermelho') \
        .withColumnRenamed('SG','jogo_sem_sofrer_gol').withColumnRenamed('DD','defesa_dificil') \
        .withColumnRenamed('DP','defesa_penalti').withColumnRenamed('GS','gol_sofrido')

    df_scouts = df_scouts.withColumn('id_atleta', col('id_atleta').cast('int')) \
        .withColumn('rodada', col('rodada').cast('int')) \
        .withColumn('pontos_media', col('pontos_media').cast('float')) \
        .withColumn('preco', col('preco').cast('float')) \
        .withColumn('preco_variacao', col('preco_variacao').cast('float')) \
        .withColumn('falta_sofrida', col('falta_sofrida').cast('int')) \
        .withColumn('passe_errado', col('passe_errado').cast('int')). \
        withColumn('assistencia', col('assistencia').cast('int')) \
        .withColumn('finalizacao_trave', col('finalizacao_trave').cast('int')) \
        .withColumn('finalizacao_defendida', col('finalizacao_defendida').cast('int')) \
        .withColumn('finalizacao_fora', col('finalizacao_fora').cast('int')) \
        .withColumn('gol', col('gol').cast('int')) \
        .withColumn('impedimento', col('impedimento').cast('int')) \
        .withColumn('penalti_perdido', col('penalti_perdido').cast('int')) \
        .withColumn('roubada_bola', col('roubada_bola').cast('int')) \
        .withColumn('falta_cometida', col('falta_cometida').cast('int')) \
        .withColumn('gol_contra', col('gol_contra').cast('int')) \
        .withColumn('cartao_amarelo', col('cartao_amarelo').cast('int')) \
        .withColumn('cartao_vermelho', col('cartao_vermelho').cast('int')) \
        .withColumn('jogo_sem_sofrer_gol', col('jogo_sem_sofrer_gol').cast('int')) \
        .withColumn('defesa_dificil', col('defesa_dificil').cast('int')) \
        .withColumn('defesa_penalti', col('defesa_penalti').cast('int')) \
        .withColumn('gol_sofrido', col('gol_sofrido').cast('int')) \
        .withColumn('ano', col('ano').cast('int')) \
        .withColumn('id_clube', col('id_clube').cast('int'))

    df_scouts = df_scouts.withColumn('posicao', \
                         when(col('posicao') == 'ata','Atacante') \
                        .when(col('posicao') == 'gol','Goleiro') \
                        .when(col('posicao') == 'lat','Lateral') \
                        .when(col('posicao') == 'mei','Meia') \
                        .when(col('posicao') == 'tec','Tecnico') \
                        .when(col('posicao') == 'zag','Zagueiro'))

    print('\n*** SCOUTS ***')
    print('Total de registros na base de scouts: {}'.format(df_scouts.count()))

    df_scouts = df_scouts.distinct()
    print('\nTotal de registros distintos na base de scouts: {}'.format(df_scouts.count()))

    df_scouts.printSchema()
    df_scouts.show(50)

except:
    print('Erro no processo Scouts: {}'.format(sys.exc_info()[0]))
    raise
else:
    print('Processo Scouts executado com sucesso !\n')


#########
# Times
#########
try:

    df_times = spark.read.csv(base_time + '/times_ids.csv', header=True, sep=",", encoding="ISO-8859-1", \
                              inferSchema=False)

    df_times = df_times.drop('nome.cbf').drop('nome.completo').drop('cod.older').drop('cod.2017') \
                .drop('cod.2018').drop('abreviacao').drop('escudos.60x60').drop('escudos.45x45').drop('escudos.30x30')

    df_times = df_times.withColumnRenamed('nome.cartola','clube').withColumnRenamed('id','id_clube')

    print('\n*** TIMES ***')
    print('Total de registros na base de times: {}'.format(df_times.count()))
    df_times = df_times.withColumn('clube', \
                                    when(col('id_clube') == '293','Atlhetico-PR') \
                                    .when(col('id_clube') == '354', 'Ceará') \
                                    .otherwise(col('clube') )
                                   )

    df_times = df_times.distinct()

    print('Total de registros distintos na base de times: {}'.format(df_times.count()))

    df_times = df_times.withColumn('id_clube', col('id_clube').cast('int'))
    df_times = df_times.distinct()

    df_times.printSchema()
    df_times.show()

except:
    print('Erro no processo Times: {}'.format(sys.exc_info()[0]))
    raise
else:
    print('Processo Times executado com sucesso !\n')


##########
# Atletas
##########
try:
    df_atletas_2014 = spark.read.csv(base_atleta + '/2014_jogadores.csv', header=True, sep=",", \
                                     encoding="ISO-8859-1", inferSchema=False)
    df_atletas_2015 = spark.read.csv(base_atleta + '/2015_jogadores.csv', header=True, sep=",", \
                                     encoding="ISO-8859-1", inferSchema=False)
    df_atletas_2016 = spark.read.csv(base_atleta + '/2016_jogadores.csv', header=True, sep=",", \
                                     encoding="ISO-8859-1", inferSchema=False)
    df_atletas_2017 = spark.read.csv(base_atleta + '/2017_jogadores.csv', header=True, sep=",", \
                                     encoding="ISO-8859-1", inferSchema=False)

    df_atletas_2014 = df_atletas_2014.withColumnRenamed('ID','id_atleta') \
                                      .withColumnRenamed('Apelido', 'atleta') \
                                      .withColumnRenamed('ClubeID', 'id_clube') \
                                      .withColumnRenamed('PosicaoID','id_posicao')


    df_atletas_2015 = df_atletas_2015.withColumnRenamed('ID','id_atleta') \
                                      .withColumnRenamed('Apelido', 'atleta') \
                                      .withColumnRenamed('ClubeID', 'id_clube') \
                                      .withColumnRenamed('PosicaoID','id_posicao')

    df_atletas_2016 = df_atletas_2016.withColumnRenamed('ID','id_atleta') \
                                      .withColumnRenamed('Apelido', 'atleta') \
                                      .withColumnRenamed('ClubeID', 'id_clube') \
                                      .withColumnRenamed('PosicaoID','id_posicao')

    df_atletas_2017 = df_atletas_2017.withColumnRenamed('AtletaID','id_atleta') \
                                      .withColumnRenamed('Apelido', 'atleta') \
                                      .withColumnRenamed('ClubeID', 'id_clube') \
                                      .withColumnRenamed('PosicaoID','id_posicao')


    df_atletas_2014 = df_atletas_2014.withColumn('ano', lit(2014))
    df_atletas_2015 = df_atletas_2015.withColumn('ano', lit(2015))
    df_atletas_2016 = df_atletas_2016.withColumn('ano', lit(2016))
    df_atletas_2017 = df_atletas_2017.withColumn('ano', lit(2017))


    print('\n*** ATLETAS ***')
    # Union com os atletas de todos os anos
    print('Juntando bases de atletas ...')
    df_atletas = df_atletas_2014.unionAll(df_atletas_2015).unionAll(df_atletas_2016).unionAll(df_atletas_2017)

    df_atletas = df_atletas.withColumn('id_clube', col('id_clube').cast('int')) \
                    .withColumn('id_atleta', col('id_atleta').cast('int'))

    df_atletas = df_atletas.withColumn('posicao', \
                           when(col('id_posicao') == '1', 'Goleiro') \
                          .when(col('id_posicao') == '2', 'Lateral') \
                          .when(col('id_posicao') == '3', 'Zagueiro') \
                          .when(col('id_posicao') == '4', 'Meia') \
                          .when(col('id_posicao') == '5', 'Atacante') \
                          .when(col('id_posicao') == '6', 'Tecnico'))

    print('Total de registros na base de atletas: {}'.format(df_atletas.count()))

    df_atletas = df_atletas.distinct()
    print('Total de registros distintos na base de atletas: {}'.format(df_atletas.count()))

    df_atletas_tmp = df_atletas.drop('id_clube').drop('clube').drop('id_posicao').drop('posicao').drop('ano')

    df_atletas.select('id_atleta','atleta','id_clube','id_posicao','posicao','ano')
    df_atletas.printSchema()
    df_atletas.show(50)

except:
    print('Erro no processo Atletas: {}'.format(sys.exc_info()[0]))
    raise
else:
    print('Processo Atletas executado com sucesso !\n')


###############
## Principal
###############
try:
    print('DF_SCOUTS:')
    df_scouts.printSchema()

    print('DF_TIMES:')
    df_times.printSchema()

    print('DF_ATLETAS:')
    df_atletas.printSchema()

    # Adiciona nome do clube
    # df = df_scouts.join(df_times, on='id_clube', how='left')

    df = df_scouts.alias('a').join(df_times.alias('b'), col('b.id_clube') == col('a.id_clube')).select(
        [col('a.id_atleta'),col('a.rodada'),col('a.id_clube'),col('a.posicao'),\
         col('a.Jogos'),col('a.pontos'),col('a.pontos_media'),col('a.preco'),\
         col('a.preco_variacao'),col('a.falta_sofrida'),col('a.passe_errado'),\
         col('a.assistencia'),col('a.finalizacao_trave'),col('a.finalizacao_defendida'),\
         col('a.finalizacao_fora'),col('a.gol'),col('a.impedimento'),col('a.penalti_perdido'),\
         col('a.roubada_bola'),col('a.falta_cometida'),col('a.gol_contra'),col('a.cartao_amarelo'),\
         col('a.cartao_vermelho'),col('a.jogo_sem_sofrer_gol'),col('a.defesa_dificil'),\
         col('a.defesa_penalti'),col('a.gol_sofrido'),col('a.ano')] + [col('b.clube')])

    df = df.alias('a').join(df_atletas.alias('b'), col('b.id_atleta') == col('a.id_atleta')).select(
        [col('a.id_atleta'), col('a.rodada'), col('a.id_clube'), col('a.clube'), \
         col('a.Jogos'), col('a.pontos'), col('a.pontos_media'), col('a.preco'), \
         col('a.preco_variacao'), col('a.falta_sofrida'), col('a.passe_errado'), \
         col('a.assistencia'), col('a.finalizacao_trave'), col('a.finalizacao_defendida'), \
         col('a.finalizacao_fora'), col('a.gol'), col('a.impedimento'), col('a.penalti_perdido'), \
         col('a.roubada_bola'), col('a.falta_cometida'), col('a.gol_contra'), col('a.cartao_amarelo'), \
         col('a.cartao_vermelho'), col('a.jogo_sem_sofrer_gol'), col('a.defesa_dificil'), \
         col('a.defesa_penalti'), col('a.gol_sofrido'), col('a.ano')] + [col('b.posicao')])


    df = df.withColumn('id_clube', col('id_clube').cast('int'))

    df = df.join(df_atletas_tmp, on='id_atleta', how='left')
    df = df.drop('Jogos')

    df.printSchema()

    print('\n*** DF_FINAL ***')
    print('\nQuantidade de registros no dataframe (antes): {}\n'.format(df.count()))
    df = df.distinct()
    print('Quantidade de registros no dataframe (depois): {}\n'.format(df.count()))

    df = df.select('id_atleta','atleta','rodada','id_clube','clube','posicao','pontos','pontos_media',\
                   'preco','preco_variacao','falta_sofrida','passe_errado','assistencia',\
                   'finalizacao_trave','finalizacao_defendida','finalizacao_fora','gol','impedimento',\
                   'penalti_perdido','roubada_bola','falta_cometida','gol_contra','cartao_amarelo',\
                   'cartao_vermelho','jogo_sem_sofrer_gol','defesa_dificil','defesa_penalti','gol_sofrido','ano')

    print('DF_FINAL:')
    df.printSchema()
    df.show(5)

    # Gravando arquivos com resultados
    df_pd_scouts = df.toPandas()
    df_pd_scouts.to_csv(path_or_buf='/home/marcelio/Git/dev_local/cartola/bases/resultados/scout.csv', \
                     sep=';',header=True, mode='w')

    df_pd_times = df_times.toPandas()
    df_pd_times.to_csv(path_or_buf='/home/marcelio/Git/dev_local/cartola/bases/resultados/times.csv', \
                     sep=';',header=True, mode='w')

    df_pd_atletas = df_atletas.toPandas()
    df_pd_atletas.to_csv(path_or_buf='/home/marcelio/Git/dev_local/cartola/bases/resultados/atletas.csv', \
                     sep=';',header=True, mode='w')

except:
    print('Erro: {}'.format(sys.exc_info()[0]))
    raise
else:
    print('\nFim: {}\n'.format(dt.datetime.today().strftime('%m/%d/%Y, %H:%M:%S')))