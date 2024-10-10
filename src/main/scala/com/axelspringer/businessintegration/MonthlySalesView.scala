package com.axelspringer.businessintegration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.SparkSession

object MonthlySalesView {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdSolutions Data Lake")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .getOrCreate()

    spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .format("csv")
      .load("src/main/resources/SalesRecords.csv")
      .withColumn("Month", date_format(to_date(col("Order date"), "M/d/yyyy"), "yyyy-MM"))
      .withColumn("Total Profit", col("Units Sold") * (col("Unit Price") - col("Unit Cost")))
      .groupBy("Month")
      .agg(sum("Total Profit").cast(DecimalType(10, 2)).as("Total Profit"))
      .orderBy("Month")
      .show(12, false)

    /*
      +-------+------------+
      |Month  |Total Profit|
      +-------+------------+
      |2022-01|2816857.02  |
      |2022-02|7072050.51  |
      |2022-03|928351.06   |
      |2022-04|4759999.55  |
      |2022-05|4453309.30  |
      |2022-06|2182138.63  |
      |2022-07|5578463.06  |
      |2022-08|575165.07   |
      |2022-09|2340236.43  |
      |2022-10|4504764.05  |
      |2022-11|6453580.25  |
      |2022-12|2356230.07  |
      +-------+------------+
      only showing top 12 rows
     */
  }

}
