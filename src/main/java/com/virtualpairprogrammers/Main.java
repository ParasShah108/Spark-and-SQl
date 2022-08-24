
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

import org.apache.log4j.Level;
import org.apache.spark.sql.expressions.WindowSpec; 
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {

		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true)
				.csv("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\News_Final.csv");
		Dataset<Row> googleplusDataset = spark.read().option("header", true)
				.csv("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\GooglePlus_Economy.csv");
		Dataset<Row> facebookDataset = spark.read().option("header", true)
				.csv("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\Facebook_Microsoft.csv");
		//dataset.show();

		dataset.createOrReplaceTempView("my_news_table");
//-------------------------------------------Question 1-a----------------------------------------------------
		Dataset<Row> rule1 = spark.sql("select * from my_news_table where char_length(Title)>12 ");
		//rule1.show();
//-------------------------------------------Question 1-b-----------------------------------------------------
		rule1.createOrReplaceTempView("rule1");
		Dataset<Row> rule2 = spark.sql("select distinct(*) from rule1 where "
									+ "Topic='obama' OR Topic='economy'OR Topic='microsoft'OR Topic='palestine'");
		//rule2.show();

//-------------------------------------------Question 2-------------------------------------------------------------
//		Dataset<Row> rejectedRule1 = spark.sql("select IDLink from my_news_table where char_length(Title)<12 ");
//		//rejectedRule1.show();
//		rejectedRule1.coalesce(1).write().option("header", true).format("csv").save("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\Rule1_reject_log.csv");
//		
//		Dataset<Row> rejectedRule2 = spark.sql("select IDLink from rule1 where "
//				+ "Topic!='obama' OR Topic!='economy'OR Topic!='microsoft'OR Topic!='palestine'");
//		rejectedRule2.coalesce(1).write().option("header", true).format("csv").save("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\Rule2_reject_log.csv");
//--------------------------------------------Question 3----------------------------------------------------------------
		//rule2.coalesce(1).write().option("header", true).format("csv").save("C:\\Users\\Paras_Shah\\Desktop\\Java Training\\News_cleansed.csv");

//--------------------------------------------Question 4-a-------------------------------------------------------------
		long originalDataCount = dataset.count();
		System.out.println("Total number of records in the original file : "+originalDataCount);

//--------------------------------------------question 4-b-------------------------------------------------------------		
		
		long dataAfterRules = rule2.count();
		System.out.println("Total number of records after applying business rules : "+dataAfterRules);

//---------------------------------------------Question 4-c-------------------------------------------------------------		
		System.out.println("Year-wise average Sentiment score of the text in the news items' headline for each topic");
		rule2.createOrReplaceTempView("rule2");
		Dataset<Row> year = spark.sql("select *,year(PublishDate) as Year from rule2");
		year.createOrReplaceTempView("my_year_table");
		Dataset<Row> avgHeadline = spark.sql("select distinct(Year),Topic,round(avg(SentimentHeadline),0) as  Headline_sentiment_score_avg from my_year_table group by Year,Topic");
		avgHeadline.show();
//---------------------------------------------Question 4-d--------------------------------------------------------		
		System.out.println("Year-wise average Sentiment score of the text in the news items' title for each topic");
	
		Dataset<Row> avgSentiTitle = spark.sql("select distinct(Year),Topic,round(avg(SentimentTitle),) as Title_sentiment_score_avg from my_year_table group by Year,Topic");
		avgSentiTitle.show();
//----------------------------------------------Question 4-e------------------------------------------------------		
//		Dataset<Row> topPopularItems = spark.sql("Select IDLink,avg(Facebook+GooglePlus+LinkedIn) as Average,RANK(avg(Facebook+GooglePlus+LinkedIn)) OVER " + 
//				"    (PARTITION BY IDLink ORDER BY avg(Facebook+GooglePlus+LinkedIn) DESC) AS Rank  from rule2 group by IDLink" );
		
		System.out.println("Top 10 most popular items by average Final value of the news items' popularity on Facebook, GooglePlus, LinkedIn");
		Dataset<Row> topPopularItems = spark.sql("Select IDLink,avg(Facebook+GooglePlus+LinkedIn) as Average, "
				+ " RANK() OVER (ORDER BY avg(Facebook+GooglePlus+LinkedIn) DESC) AS ROW_RANK "
				+ "from rule2 group by IDLink" );
		topPopularItems.show(10);
//----------------------------------------------Question 5-----------------------------------------------
		googleplusDataset.createOrReplaceTempView("google_table");
		//googleplusDataset.show();
		facebookDataset.createOrReplaceTempView("facebook_table");
		Dataset<Row> newsGoogle = spark.sql("select R2.*,G.* from my_year_table R2 "
				+ "JOIN google_table G on R2.IDLink=G.IDLink where Year=2015 and Topic='economy'");
		newsGoogle.createOrReplaceTempView("news_and_google");

		Dataset<Row> newsFacebook = spark.sql("select R2.*,F.* from my_year_table R2 "
				+ "JOIN facebook_table F on R2.IDLink=F.IDLink where Year=2015 and Topic='economy'");
		newsFacebook.createOrReplaceTempView("news_and_facebook");
		//newsFacebook.show();
		Dataset<Row> feedback = spark.sql("Select * from news_and_google "
				+ "UNION "
				+ "Select * from news_and_facebook");
		//feedback.show();
		spark.close();

	}
}