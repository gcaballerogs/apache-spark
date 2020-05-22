package com.mx.demo.spark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Ejecutar en consola con la instruccion:
 * 
 * spark-submit --class com.mx.demo.spark.WordCounter --master local C:\WS\demo-apache-spark\target\demo-apache-spark-0.0.1-SNAPSHOT.jar C:\WS\demo-apache-spark\src\main\resources\data\texto_Apache_Spark.txt
 *
 */
public class WordCounter {

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCounter.class);

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		if (args.length < 1) {
			LOGGER.error("No se proporciono un archivo a procesar...");
			System.exit(1);
		}
		wordCount(args[0]);
	}

	public static void wordCount(String fileName) {
		LOGGER.info("####### INICIA EJECUCION DE WordCounter.java ####### \n");

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaWordCount");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputFile = sparkContext.textFile(fileName, 1);

		JavaRDD<String> wordsFromFile = inputFile.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

		JavaPairRDD<String, Integer> wordAsTuple = wordsFromFile.mapToPair(word -> new Tuple2<>(word, 1));

		JavaPairRDD<String, Integer> wordWithCount = wordAsTuple.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		wordWithCount.saveAsTextFile("CountData");

		List<Tuple2<String, Integer>> output = wordWithCount.collect();
		for (Tuple2<?, ?> tuple : output) {
			LOGGER.info(" >> " + tuple._1() + ": " + tuple._2());
		}

		LOGGER.info("####### FINALIZA EJECUCION DE WordCounter.java ####### \n");
		sparkContext.close();
		sparkContext.stop();
	}

}
