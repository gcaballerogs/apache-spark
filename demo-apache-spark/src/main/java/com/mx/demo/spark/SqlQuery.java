package com.mx.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Ejecutar en consola con la instruccion:
 * 
 * spark-submit --class com.mx.demo.spark.SqlQuery --master local C:\WS\demo-apache-spark\target\demo-apache-spark-0.0.1-SNAPSHOT.jar
 *
 */
public class SqlQuery {

	public static void main(String[] args) {

		SparkSession sqlContext = SparkSession.builder().appName("SqlQueryExample").getOrCreate();

		StructType schema = new StructType().add("InvoiceNo", "string").add("StockCode", "string")
				.add("Description", "string").add("Quantity", "int").add("InvoiceDate", "date").add("UnitPrice", "long")
				.add("CustomerID", "string").add("Country", "string");

		Dataset<Row> df = sqlContext.read().option("mode", "DROPMALFORMED").schema(schema).option("dateFormat", "dd/MM/yyyy")
				.csv("file:///WS/demo-apache-spark/src/main/resources/data/eCommerce.csv");

		df.createOrReplaceTempView("pedidos");
		Dataset<Row> sqlResult = sqlContext.sql(
				"SELECT InvoiceNo, InvoiceDate, CustomerID, SUM( UnitPrice*Quantity ) "
				+ "FROM pedidos "
				+ "GROUP BY InvoiceNo, InvoiceDate, CustomerID "
				+ "ORDER BY 4 DESC LIMIT 10");

		sqlResult.show();

		sqlResult.write().option("header", "true").csv("file:///Datos/out");

	}

}
