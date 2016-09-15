package com.fuse.main;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;



public class Main {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String logFile = "Person.json"; // Should
																							// system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(logFile);
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").getOrCreate();
		
		Dataset<Row> df = spark.read().json("Person.json");

		// Displays the content of the DataFrame to stdout
		df.show();
		df.select("name").show();
		
		
		
		JavaRDD<Person> peopleRDD = spark.read()
				  .text("Person.json")
				  .javaRDD()
				  .map(new Function<String, Person>() {
				    @Override
				    public Person call(String line) throws Exception {
				      String[] parts = line.split(",");
				      Person person = new Person();
				      person.setName(parts[0]);
				      person.setDescription(parts[1]);
				      person.setUrl(parts[2]);
				      person.setVersion(parts[3]);
				     
				      return person;
				    }
				  });

				// Apply a schema to an RDD of JavaBeans to get a DataFrame
				Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
				// Register the DataFrame as a temporary view
				peopleDF.createOrReplaceTempView("people");
				
				Dataset<Row> people= spark.sql("SELECT * FROM people");
				peopleDF.show();
				

				// SQL statements can be run by using the sql methods provided by spark

		
		
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			
			@Override
			public Iterator<String> call(String s) {

				return Arrays.asList(SPACE.split(s)).iterator();
			}
		});
		
		System.out.println(words.collect());
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		
		System.out.println(pairs.collect());
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b)

			{
				return a + b;
			}
		});

		
		
		List<Tuple2<String, Integer>> output = counts.collect();
		
		System.out.println(counts.count());
		
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		spark.stop();
	}
}
