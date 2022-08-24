
//		List<String> result = whiteSpace.take(20);
//		result.forEach(System.out::println);

//		long count = whiteSpace.count();
//		System.out.println(count);
//long count = inputData.count();
//JavaRDD<String> splitData = inputData.flatMap(sentence ->Arrays.asList(sentence.split(" ")).iterator());

//JavaRDD <String>whiteSpace = inputData.filter(sentences->sentences.trim().length()>1);
//long count = inputData.count();

//splitData.collect().forEach(System.out::println);
//	JavaRDD<String> splitRdd = inputData.flatMap(sentences->Arrays.asList(sentences.split(" ")).iterator());

//List<String> result = splitRdd.take(20);

//result.forEach(System.out::println);

//Map<String, Long> countByKey = pairRdd.countByKey();
//		System.out.println(countByKey);

// --------------------------------------------------------------------------------

package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.Sentences;

import scala.Tuple2;




public class Main_1 {
	


	public static void main(String[] args) {
			
		Main obj = new Main();
	      
	      //obj.test();
	     
		//----------------------------------
		
		//StopWatch stopWatch = new StopWatch();
	     // stopWatch.start();
	      //obj.test();
	      
		
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputData = sc.textFile("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\reviews.txt");

		// JavaRDD <String>whiteSpace =
		// inputData.flatMap(lines->Arrays.asList(lines.split(" ")).iterator());

//----------------------------------------Question 1-------------------------------------------------------
		long start1 = System.currentTimeMillis();
		System.out.println("There are " + inputData.getNumPartitions() + " Partitions");
		JavaPairRDD<String, Long> pairRdd = inputData.mapToPair(lines -> {
			String[] columns = lines.split(".  ");
			String sentence = columns[0];

			return new Tuple2<>(sentence, 1L);
		});
		
	System.out.println(pairRdd.count());

		JavaPairRDD<String, Long> sumRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
//
		sumRdd.foreach(tuple -> System.out.println( tuple._2));
		 long end1 = System.currentTimeMillis();
		 System.out.println(end1-start1);
//-----------------------------------------------------------------------------------------------------------

//------------------------------------------Question 2-----------------------------------------------------
//
		 long start2 = System.currentTimeMillis();
		 JavaRDD<String> noSpecialChar =
		 inputData.map(sentences->sentences.replaceAll("[^a-zA-Z0-9 ]", " "));
		// java.util.List<String> results = noSpecialChar.take(1);
		//.forEach(System.out::println);
		 long end2 = System.currentTimeMillis();
		 System.out.println(end2-start2);
//----------------------------------------------------------------------------------------------------------		

//------------------------------------------Question 3-----------------------------------------------------
		// JavaRDD<String> check = inputData.map(sentence->sentence.contains("[^Z0-9
		// ]"));

//		inputData
//		 .flatMap(value ->Arrays.asList( value.split(".  ")).iterator())
//		 .filter(x ->x.contains("1"))
//		 .take(1)
//		 .forEach (System.out::println);
//		
//		inputData
//		 .flatMap(value ->Arrays.asList( value.split(".  ")).iterator())
//		.filter(x ->x.contains("1"))
//		 .take(5)
//		 .forEach (System.out::println);

		// -------------------------------------------------------------------------------------------
		 long start3 = System.currentTimeMillis();
		 JavaRDD<String> splitRdd = inputData.flatMap(value ->Arrays.asList( value.split(".  ")).iterator());
		JavaRDD<String> noNumbers = splitRdd.filter(movies ->movies.matches("^[^0-9]*$"));
		System.out.println(noNumbers.count());
		long end3 = System.currentTimeMillis();
		 System.out.println(end3-start3);
		// --------------------------------------------------------------------------------------------------------

//		long count = noNumbers.count();
//		System.out.println(count);

//		JavaRDD <String> check = inputData.filter(x ->x.contains("1"));
//		java.util.List<String> results = check.take(1);
//		results.forEach(System.out::println);
//----------------------------------------------------------------------------------------------------------			
//----------------------------------------Question 4--------------------------------------------------------
//
		 long start4 = System.currentTimeMillis();
		JavaRDD<String> splitRddd = inputData.flatMap(value -> Arrays.asList(value.split(".  ")).iterator())
				.filter(count -> count.contains("movie"));
//		// splitRddd.collect().forEach(System.out::println);
//		System.out.println(splitRddd.count());
		long end4 = System.currentTimeMillis();
		 System.out.println(end4-start4);
//------------------------------------------------------------------------------------------------------------------------------------
////---------------------------------------Question 5---------------------------------------------------------------
		 long start5 = System.currentTimeMillis();
		 System.out.println("There are " + inputData.getNumPartitions() + " Partitions");
		JavaPairRDD<String,Integer> pairRddd = inputData.mapToPair(lines -> {
			String[] columns = lines.split(".  ");
			String sentence = columns[0];
		
			
			return new Tuple2<>(sentence,sentence.length());
			
		});
		JavaPairRDD<Integer,String> switched = pairRddd.mapToPair(tuple-> new Tuple2<Integer,String>(tuple._2,tuple._1));
		JavaPairRDD<Integer,String> sorted = switched.sortByKey(false);
		//sorted.foreach(tuple -> System.out.println(tuple._1));
		long end5 = System.currentTimeMillis();
		 System.out.println(end5-start5);
		
//------------------------------------------------------------------------------------------------------------------------------------
//------------------------------------------Question 6-------------------------------------------------------------------------------------
		 long start6 = System.currentTimeMillis();
		// noSpecialChar.coalesce(1).saveAsTextFile("C:\\Users\\Paras_Shah\\Desktop\\JavaTraining\\reviews_cleansed.txt.");
		// noSpecialChar.coalesce(1).saveAsTextFile("D:\\reviews_cleansed.txt.");
		 long end6 = System.currentTimeMillis();
		 System.out.println(end6-start6);
//----------------------------------------------------------------------------------------------------------------------------------------		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		sc.close();

	}
}