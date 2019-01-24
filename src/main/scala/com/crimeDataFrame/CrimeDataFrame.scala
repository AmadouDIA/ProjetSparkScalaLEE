package com.crimeDataFrame


import org.apache.log4j._
import org.apache.spark.sql._

object CrimeDataFrame {

  // On crée un case class crime contenant l'ID, la Date et le type de crime
  case class Crime(ID:Int, Date: String, typeCrime : String )

  // On crée une fonction mapper que sert obtenir un obtenir un objet de type Crime
  def mapper(ligne: String): Crime ={
    var champs = ligne.split(',')
    var date = formatDate(champs(2))
    val crime:Crime = Crime(champs(0).toInt, date,champs(5))
    return  crime
  }

  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    // On crée une session spark
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./")
      .getOrCreate()


    import spark.implicits._

    // On import le fichier csv contenant les données
    val ligne = spark.sparkContext.textFile("/home/dia/Documents/hadoop_spark/Crimes_data_chicago_2017.csv")

    // On affiche la première ligne du fichier contenant les noms de chaque attribut de données
    val header = ligne.first()
    print(header)

    // On fait un filtre pour enlever l'entête
    val lignesSansHeader = ligne.filter(ligne => ligne != header)

    // On crée un DataFrame aprés avoir fait un map sur les données avec la fonction mapper définie plus haut
    val crimeDF = lignesSansHeader.map(mapper).toDF

    // On cré une vue temporaire utilisant le DataFrame
    crimeDF.createOrReplaceTempView("CrimesView")

    // On peut maintenant écrire la requete sql sur cette vue pour trouver le nombre de crime par date et par type
    // trié par ordre croissant sur la date et par ordre décroissant sur le nombre de crime
    val nbCrimeDateType = spark.sql("select typeCrime, Date, count(ID) from CrimesView group by typeCrime, Date order by count(ID) desc, Date")

    // On transforme en une collection
    val g = nbCrimeDateType.collect()

    // On affiche le resultat
    g.foreach(println)
  }

  def formatDate(date : String) = {
    val bonFormat = date.split(" ")(0).split("/")(2) + date.split("/")(0)
    bonFormat
  }
}
