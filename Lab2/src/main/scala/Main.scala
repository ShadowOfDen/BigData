import java.time.LocalDateTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object My_Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //Конфигурация
    val cfg = new SparkConf()
      .setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._


    val data = sc.textFile("file:///D://BigData//data//programming-languages.csv").map { idx => idx.toLowerCase }
    // исключаем заголовок из даты
    val junk = data.first()
    val programming_languages_tmp = data.filter(x => x != junk).collect().toList


    val programming_languages = programming_languages_tmp.map{
      row => row.split(",")
    }.filter{
      rowValues => rowValues.size==2
    }.map{
      rowValues =>
        val Seq(name, link) = rowValues.toSeq
        name.toLowerCase
    }
    programming_languages.take(10).foreach(println)
    //период времени
    val topCount = 10
    val years = 2010 to 2020
    //пример данных
    val posts = sc.textFile("file:///D://BigData//data//posts_sample.xml")
    val posts_count = posts.count
    val posts_raw = posts.zipWithIndex.filter{ case (s, idx) => idx>2 && idx<posts_count-1 }.map(_._1)
    val postsCount = posts.count
    //индексируем элементы
    val df = posts.zipWithIndex().filter{case (elem, index) => index > 2 && index < postsCount - 1}.map(_._1).map(scala.xml.XML.loadString).flatMap(parsePost)
      .filter(post => programming_languages.contains(post._2) && years.contains(post._1))
      .map(post => (post, 1))
      .reduceByKey(_+_)
      .map{case (post, count) => (post._1, (post._2, count))}
      .groupByKey()
      .flatMapValues(_.toSeq.sortBy(-_._2).take(topCount))
      .map{case (year, (tag, count)) => (year, tag, count)}

    println("res :")
    df.take(10).foreach(println)

    val res = df.toDF("Year", "Language", "Count").sort(asc("Year"), desc("Count"))

    res.show(years.size * topCount)

    res.write.format("parquet").save("language_full.parquet")

  }
  //Парсинг данных
  def parsePost(xml: scala.xml.Elem): Array[(Int, String)] = {
    val xmlDate = xml.attribute("CreationDate")
    val xmlTags = xml.attribute("Tags")
    if (xmlDate.isEmpty || xmlTags.isEmpty){
      return new Array[(Int, String)](0)
    }
    val creationDate = xmlDate.mkString
    val tags = xmlTags.mkString
    val year = creationDate.substring(0, 4)
    val tagsArray = tags.substring(4, tags.length-4).split("&gt;&lt;").map(idx => idx.toLowerCase)

    tagsArray.map(
      tag => (year.toInt, tag)
    )
  }

}
