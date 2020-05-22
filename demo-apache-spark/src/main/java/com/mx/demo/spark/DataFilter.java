package com.mx.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Ejecutar en consola con la instruccion:
 * 
 * spark-submit --class com.mx.demo.spark.DataFilter --master local
 * C:\WS\demo-apache-spark\target\demo-apache-spark-0.0.1-SNAPSHOT.jar
 *
 */
public class DataFilter {

	public static final void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("DataFilter").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = readData(sqlContext);
		datosMeteorologicos = datosMeteorologicos.select("Time", "Indoor_Temperature");
		datosMeteorologicos = datosMeteorologicos.filter("Indoor_Temperature < 7");

		saveData(datosMeteorologicos);
		spark.close();
	}

	private static void saveData(final Dataset<Row> datosMeteorologicos) {
		datosMeteorologicos.write().json("/Datos/out/json");
	}

	private static Dataset<Row> readData(final SparkSession sqlContext) {
		final String path = "/WS/demo-apache-spark/src/main/resources/data/weather.txt";
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}

}
