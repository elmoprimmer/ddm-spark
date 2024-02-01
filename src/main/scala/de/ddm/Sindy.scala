package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set}


object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val data = inputs.map(input => readData(input, spark))

    val columnSets = data
      .flatMap(df => df.columns.map(column => df.select(column)
        .rdd.map(row => (row(0), Set[String](column))))) //maybe there is a better way to get a set
      .reduce(_ union _).reduceByKey(_ ++ _).values.distinct()


    val possibleInclusionsList = columnSets.flatMap(set => set.map(column => (column, set.filterNot(_ == column))))

    val inclusions = possibleInclusionsList.reduceByKey(_.intersect(_)).filter(_._2.nonEmpty)

    val inclusionsSorted = inclusions.sortByKey().collect()

    inclusionsSorted.foreach(column => print(column._1 + " < " + column._2.mkString(", ")+ "\n"))

  }
}