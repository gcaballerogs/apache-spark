package com.mx.demo.spark;

import java.util.Formatter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Ejecutar en consola (en ambiente local) con la instruccion:
 * 
 * spark-submit --class com.mx.demo.spark.MdcReducer --master local[*] path_to_jar path_to_data_folder
 * 
 * Ejemplo:
 * spark-submit --class com.mx.demo.spark.MdcReducer --master local[*] C:\mdc-spark\target\mdc-spark-0.0.1-SNAPSHOT.jar C:\mdc-spark\src\main\resources\data
 *
 */
public class MdcReducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MdcReducer.class);
	
	public static void main(String[] args) {
		
		if (args.length < 1) {
			LOGGER.error("No se proporciono la ruta de la carpeta DATA...");
			System.exit(1);
		}
		
		LOGGER.info("DATA FOLDER = " + args[0]);
		process(args[0]);
	}

	public static void process(String path) {
		LOGGER.info("####### INICIA EJECUCION DE MdcReducer.java ####### \n");

		String accountFilePath =  path + "/cuentas.csv";
		String tradeFilePath = path + "/trades.csv.gz";
		String inquiryFilePath = path + "/inquiry.csv.gz";
		String resultPath = path + "/resultados";

		SparkConf sparkConf = new SparkConf().setAppName("MDC").setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> accounts = getDataWithoutHeader(sparkContext.textFile(accountFilePath, 1));
		JavaRDD<String> trades = getDataWithoutHeader(sparkContext.textFile(tradeFilePath, 1));
		JavaRDD<String> inquiry = getDataWithoutHeader(sparkContext.textFile(inquiryFilePath, 1));

		// RDD<K,V> con datos de cuentas, donde K es el fid
		JavaPairRDD<String, String> mapAccounts = accounts.mapToPair(line -> new Tuple2<>(line.split(",")[0], line.split(",")[1]));

		// RDD<K,V> con datos de trades, donde K es el fid
		JavaPairRDD<String, String> mapTrades = trades.mapToPair(line -> new Tuple2<>(line.split(",")[0], line.substring(line.indexOf(",") + 1)));
		
		// RDD<K,V> con datos de inquiry, donde K es el fid
		JavaPairRDD<String, String> mapInquiry = inquiry.mapToPair(line -> new Tuple2<>(line.split(",")[0], line.substring(line.indexOf(",") + 1)));
		
		// RDD<K,V> con la union de cuentas y trades, donde K es el fid. 
		JavaPairRDD<String, Tuple2<String, String>> accountTradeMap = mapAccounts.join(mapTrades);

		// RDD<K,V> con la union de cuentas e inquiry, donde K es el fid. 
		JavaPairRDD<String, Tuple2<String, String>> accountInquiryMap = mapAccounts.join(mapInquiry);
			
		// Agrupacion de datos que comparten la misma llave
		JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>> resultMap 
		= accountTradeMap.cogroup(accountInquiryMap);
			
		Function<Tuple2<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>>, String> dataFormatter = getFormatter();
			
		JavaRDD<String> result = resultMap.map(dataFormatter);
		
		result.saveAsTextFile(resultPath);
		
		sparkContext.close();
		sparkContext.stop();
		
		LOGGER.info("####### FINALIZA EJECUCION DE MdcReducer.java ####### \n");
	}
	
	
	private static JavaRDD<String> getDataWithoutHeader(JavaRDD<String> data) {
		String header = data.first();
		return data.filter(line -> !line.equals(header));
	}

	
	/**
	 * Funcion para aplicar formato al conjunto de datos relacionados a un fid.
	 * 
	 * @return
	 */
	private static Function<Tuple2<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>>, String> getFormatter() {
		Function<Tuple2<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>>, String> dataFormatter = 
				new Function<Tuple2<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(
					Tuple2<String, Tuple2<Iterable<Tuple2<String, String>>, Iterable<Tuple2<String, String>>>> tup)
					throws Exception {

				StringBuilder sb = new StringBuilder();
				Formatter fmtr = new Formatter(sb);
				int trCounter = 1, inqCounter = 1;

				// fid
				sb.append(tup._1);

				// Iterar sobre lista de TRADE relacionadas al fid
				for (Tuple2<String, String> tupTr : tup._2._1) {
					fmtr.format("|TR%03d|", trCounter++);
					sb.append(tupTr._2);
				}

				// Iterar sobre lista de INQUIRY relacionadas al fid
				for (Tuple2<String, String> tupInq : tup._2._2) {
					fmtr.format("|IQRY%03d|", inqCounter++);
					sb.append(tupInq._2);
				}

				fmtr.close();
				return sb.toString();
			}
		};

		return dataFormatter;
	}	
}
