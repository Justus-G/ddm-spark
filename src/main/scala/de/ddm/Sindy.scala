package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

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
    import spark.implicits._

    val valuesWithEmptySet = mutable.HashSet[String]()
    inputs.map(input => readData(input, spark))       // list of files -> list of Spark Datasets
      .map(ds => {                                    // Spark Dataset -> tuples of value and corresponding columnName
        val columns = ds.columns
        ds.flatMap(row => {
          for (i <- columns.indices) yield {
            (row.getString(i), columns(i))
          }
        }) // (value, colName)
      })
      .reduce(_ union _)
      .groupByKey(x => x._1)                          // group by value --> get unique values with corresponding colNames
      .mapGroups((_, it) => it.map(elem => elem._2).toSet)  // throw away value, only keep columnNames as Sets
      .flatMap(set => {                               // get each possible combination of elements in the set (candidates)
        if(set.size == 1)
          valuesWithEmptySet.add(set.head)
        set.map(colName => (colName, set - colName))
      })
      .groupByKey(x => x._1)                          // group by candidate
      .mapGroups((s, it) =>
        (s, it
          .dropWhile(_ => valuesWithEmptySet.contains(s)) // (if not relevant, empty iterator to skip map-reduce)
          .map(x => x._2)
          .reduce((a, b) => a.intersect(b)))
      )
      .filter(elem => elem._2.nonEmpty)               // throw away results with empty sets
      .collect()                                      // collect to Array
      .sortBy(x => x._1)                              // sort keys
      .map(x => (x._1, x._2.toList.sorted))           // sort values
      .foreach(x => println(x._1 + " < " + x._2.mkString(", ")))
  }
}
