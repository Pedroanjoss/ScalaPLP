import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("Transporte")
    .master("local") // Definido para rodar localmente
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfViagens = spark.read
    .format("csv")
    .option("header", "true")      // A primeira linha do arquivo tem cabeçalho
    .option("inferSchema", "true") // Garante que os tipos corretos sejam inferidos
    .load("src/transporte.csv")

  // Renomeando as colunas para facilitar o acesso
  val dfRenomeado = dfViagens
    .withColumnRenamed("ID da viagem", "idViagem")
    .withColumnRenamed("Linha de ônibus", "linhaOnibus")
    .withColumnRenamed("Tipo de veículo", "tipoVeiculo")
    .withColumnRenamed("Número de passageiros", "numPassageiros")

  // Questão 1: Viagens com mais de 50 passageiros
  val viagensComMaisDe50Passageiros = dfRenomeado
    .filter(col("numPassageiros") > 50)
    .select("idViagem", "linhaOnibus", "tipoVeiculo")
  println("Questão 1: Viagens com mais de 50 passageiros")
  viagensComMaisDe50Passageiros.show(false)

  // Questão 2: Viagens longas (tempo superior a 60 minutos)
  val viagensLongas = dfRenomeado
    .filter(col("Tempo de viagem") > 60)
    .select("idViagem", "linhaOnibus", "Tempo de viagem", "Localização de partida")
  println("Questão 2: Viagens longas")
  viagensLongas.show(false)

  // Questão 3: Média de passageiros em todas as viagens
  val mediaPassageiros = dfRenomeado
    .agg(avg("numPassageiros").alias("mediaPassageiros"))
  println("Questão 3: Média de passageiros")
  mediaPassageiros.show(false)

  // Questão 4: Média de passageiros para veículos articulados
  val mediaPassageirosArticulado = dfRenomeado
    .filter(col("tipoVeiculo") === "Articulado")
    .agg(avg("numPassageiros").alias("mediaPassageirosArticulado"))
  println("Questão 4: Média de passageiros para veículos articulados")
  mediaPassageirosArticulado.show(false)

  // Questão 5: Custo médio de todas as viagens
  val custoMedioViagens = dfRenomeado
    .agg(avg("Custo da viagem").alias("custoMedio"))
  println("Questão 5: Custo médio de todas as viagens")
  custoMedioViagens.show(false)

  // Questão 6: Viagens em horários de pico (entre 6h e 9h)
  val viagensPico = dfRenomeado
    .filter(hour(to_timestamp(col("Hora"), "HH:mm")) >= 6 && hour(to_timestamp(col("Hora"), "HH:mm")) <= 9)
    .select("idViagem", "numPassageiros", "Custo da viagem")
  println("Questão 6: Viagens em horários de pico")
  viagensPico.show(false)

  // Questão 7: Custo total de viagens por veículos do tipo Micro-ônibus
  val custoTotalMicroOnibus = dfRenomeado
    .filter(col("tipoVeiculo") === "Micro-ônibus")
    .agg(sum("Custo da viagem").alias("custoTotalMicroOnibus"))
  println("Questão 7: Custo total de viagens por Micro-ônibus")
  custoTotalMicroOnibus.show(false)

  // Questão 8: Tempo médio de viagem em todas as linhas
  val tempoMedioViagens = dfRenomeado
    .agg(avg("Tempo de viagem").alias("tempoMedio"))
  println("Questão 8: Tempo médio de viagem")
  tempoMedioViagens.show(false)

  // Questão 9: Localização de partida mais comum
  val partidaMaisComum = dfRenomeado
    .groupBy("Localização de partida")
    .count()
    .orderBy(desc("count"))
    .limit(1)
  println("Questão 9: Localização de partida mais comum")
  partidaMaisComum.show(false)

  // Questão 10: Custo médio por linha
  val custoMedioPorLinha = dfRenomeado
    .groupBy("linhaOnibus")
    .agg(avg("Custo da viagem").alias("custoMedioPorLinha"))
  println("Questão 10: Custo médio por linha")
  custoMedioPorLinha.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
