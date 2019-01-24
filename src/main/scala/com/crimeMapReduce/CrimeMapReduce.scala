package com.crimeMapReduce

import org.apache.spark._
import org.apache.log4j._

object CrimeMapReduce {

  def main(args: Array[String]){
    //determiner le nombre de crime par mois et par type en ordre croissant par mois et decroissant par nombre de crime
    Logger.getLogger("org").setLevel(Level.ERROR)

    // On crée un sparkContext
    val sc = new SparkContext("local[*]", "CrimeCounter")

    // On importe notre fichier csv via notre sparkContext
    val lignes = sc.textFile("/home/dia/Documents/hadoop_spark/Crimes_data_chicago_2017.csv")


    //On affiche la première ligne du fichier
    val header = lignes.first()
    println(header)

    // On fait un filtre pour enlever la première ligne contenant l'entete
    val lignesSansHeader = lignes.filter(ligne => ligne != header)

    //transformations (date jj/mm/aaaa = aaaamm)
    val ligneAvecFormatDate = lignesSansHeader.map(ligne =>{
      val l = ligne.split(",")
      val date = l(2).split(" ")(0)
      val bonFormat = date.split("/")(2) + date.split("/")(0)
      ((l(5), bonFormat), 1)
    })


    // On fait alors un reduce qui addition les crimes de meme type et meme date
    val totalCrimeParMois = ligneAvecFormatDate.reduceByKey( (x,y) => x + y)

    // On trie maintenant par nombre de crimes par ordre décroissant et par mois par ordre croissant.
    val sortie = totalCrimeParMois.map(x => ((-x._2.toInt ,x._1._2), (x._1._1, x._1._2, x._2))).sortByKey(true).map(rec => rec._2)

    // On affiche le resultat
    sortie.foreach(println)
  }

}
