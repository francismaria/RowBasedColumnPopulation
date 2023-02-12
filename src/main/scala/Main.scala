package com.rowbasedcolumnpopulation.example

import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, when, struct}
import com.rowbasedcolumnpopulation.example.Udf.hasAnyNullValue

object Main extends App {

  /* -------------------------------- *
   *         Constant Values          *
   * -------------------------------- */

  val VALID_DATA = "YES"
  val INVALID_DATA = "NO"
  val GARGE_BACKLOG_FILE = "src/main/resources/garage_backlog.csv"

  /* -------------------------------- *
   *    Spark Engine Initialization   *
   * -------------------------------- */

  val spark = SparkSession.builder
    .appName("HelloWorld")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate() 

  import spark.implicits._ 
  spark.sparkContext.setLogLevel("ERROR")  

  /* -------------------------------- *
   *       Data Transformation        *
   * -------------------------------- */

  /**
    * Reads data from a CSV file given a path.
    *
    * @param path path of the CSV file to be read.
    * @return parsed dataset from CSV file
    */
  def readData(path: String): Dataset[Row] = {
    spark.read.option("header", true).csv(GARGE_BACKLOG_FILE)
  }

  /**
    * This function defines the data to be added to the new column depending on the
    * existing row values, more specifically, an invalid value (= "NO") if any null
    * value is present in the row's columns or an valid value (= "YES") otherwise.
    *
    * @return new column value
    */
  def populateContent(): Column = {
    val nullValueBooleanConditionColumn = lit(hasAnyNullValue(struct("*")))

    when(nullValueBooleanConditionColumn, INVALID_DATA).otherwise(VALID_DATA)
  }

  /**
    * Creates a new validation column with values for each row depending on each 
    * row's content.
    *
    * @param dataframe dataframe to append new column
    * @return dataset with new validation column
    */
  def createValidationColumn(dataframe: Dataset[Row]): Dataset[Row] = {
    df.withColumn("valid_record", populateContent())
  }

  /* -------------------------------- *
   *           Entry Point            *
   * -------------------------------- */

  var df = readData(GARGE_BACKLOG_FILE)

  df = createValidationColumn(df)
  
  df.show()

  spark.stop
}