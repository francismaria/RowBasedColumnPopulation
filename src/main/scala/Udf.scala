package com.rowbasedcolumnpopulation.example

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction

object Udf {

  /**
    * This UDF iterates through the values of a row and verifies if there is
    * any null value. Note the usage of the "zipWithIndex" function which 
    * supports iteration with index so that the "isNullAt" function can be
    * used for each value.
    */
  val hasAnyNullValue: UserDefinedFunction = udf((row: Row) => {
    row.schema.fieldNames.zipWithIndex.exists { case(_, i) => row.isNullAt(i) }
  })
}