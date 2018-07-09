package com.trulia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

public class MyWordCount {
	private transient static JavaSparkContext sparkContext=null;
	public static void main(String[] args) throws IOException {
		final HashMap<String, String> hashMap = new HashMap<>();
		
		hashMap.put("chest", "heart disease");
		hashMap.put("stroke", "heart disease");
		hashMap.put("attack", "heart disease");
		hashMap.put("blocage", "heart disease");

		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]");
		sparkContext = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sparkContext);
		DataFrame df = hiveContext.read().format("com.databricks.spark.csv").option("header", "true") // Use first line
																										// of all files
																	// as header
				.load(args[0]);
		df.cache();
		DataFrame description = df.select("ID","Gender");
		DataFrame hiveData = df.select("ID", "Name", "Age", "Salary");
		hiveData.show();
		hiveData.write().mode(SaveMode.Overwrite).saveAsTable("employee");
		JavaRDD<Row> myata = description.toJavaRDD();
		
		myata.foreach(new VoidFunction<Row>() {

			@Override
			public void call(Row arg0) throws Exception {
				JavaRDD<String> words= sparkContext.parallelize(Arrays.asList(arg0.getString(1).split(" ")));
				final String patientId=arg0.getString(0);
				words.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String arg0) throws Exception {
						if (hashMap.containsKey(arg0)) {
							return true;
						} else {
							return false;
						}
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String arg0) throws Exception {
						return new Tuple2<String, Integer>(arg0, 1);
					}

				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						// TODO Auto-generated method stub
						return arg0 + arg1;
					}
				}).foreach(new VoidFunction<Tuple2<String,Integer>>() {

					@Override
					public void call(Tuple2<String, Integer> arg0) throws Exception {
						System.out.println(arg0);

						Configuration c = HBaseConfiguration.create(); // Instantiate Configuration class

						HTable hTable = new HTable(c, "student"); // Instantiate HTable class

						
						Put P1 = new Put(Bytes.toBytes(patientId)); // Instantiate put Class

						P1.add(Bytes.toBytes("college"), Bytes.toBytes("disease"), Bytes.toBytes(hashMap.get(arg0._1)));

						P1.add(Bytes.toBytes("college"), Bytes.toBytes("count"), Bytes.toBytes(arg0._2));

						hTable.put(P1);

						System.out.println("Data is inserted"); // Save the put Instance to the HTable.

						hTable.close(); // close HTable

					}
				});
				
			}
		});


	}
}
