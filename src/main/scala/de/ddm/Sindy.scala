package de.ddm

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
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
    // Calculate a good number of partitions based on the cluster configuration
    val coresPerExecutor = spark.conf.getOption("spark.executor.cores").getOrElse("4").toInt
    val numExecutors = spark.conf.getOption("spark.executor.instances").getOrElse("2").toInt
    val totalCores = coresPerExecutor * numExecutors

    // Read and repartition data for each input file
    val data = inputs.map(input => readData(input, spark).repartition(totalCores*3))

    // Extract column sets by mapping each DataFrame to its columns and transforming into RDDs
    val columnSets = data
      .flatMap(df => df.columns.map(column => df.select(column)
        .rdd.map(row => (row(0), Set[String](column))))) // Union all RDDs and reduce by key to merge sets for the same key (column value)
      .reduce(_ union _).coalesce(totalCores*2).reduceByKey(_ ++ _).values.distinct()  // Reduce number o partitions for final calculations, hopefully for efficiency
    // e.g. Combines tuples like (valueInA, Set("A")) for all unique values in column "A"

    // Generate possible inclusion lists by pairing each column with the rest of the columns in its set
    val possibleInclusionsList = columnSets.flatMap(set => set.map(column => (column, set.filterNot(_ == column))))

    // Find inclusions by intersecting column sets for each column across all values
    val inclusions = possibleInclusionsList.reduceByKey(_.intersect(_))

    // Sort and prepare for printing
    val inclusionsSorted = inclusions.filter(_._2.nonEmpty).sortByKey().collect() // .collect executes pipeline

    inclusionsSorted.foreach(column => print(column._1 + " < " + column._2.mkString(", ")+ "\n"))

  }
}