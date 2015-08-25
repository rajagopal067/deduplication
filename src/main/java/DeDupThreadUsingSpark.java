import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;


public class DeDupThreadUsingSpark {
	
	public static void main(String[] args) throws ClassNotFoundException {
		
		String inputFilename = "weapons-thread-seq";
		String type = "seq";
		String outputFilename = "weapons_complete-text";
		Integer numPartitions = 1;
		
		SparkConf conf = new SparkConf().setAppName("deduplicatePosts").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		if(type.equals("text")){
			JavaRDD<String> inputRDD = sc.textFile(inputFilename);			
			PairFunction<String, String, String> keyValueFunc= new PairFunction<String, String, String>() {
				
				public Tuple2<String, String> call(String doc) throws Exception {

					JSONParser parser = new JSONParser();
					JSONObject jsonObj1 = (JSONObject) parser.parse(doc.split("\t")[1]);
					System.out.println(jsonObj1.get("uri"));
					String uri = (String) jsonObj1.get("uri");
					return new Tuple2<String, String>(uri, doc);
				}
			};
			
			JavaPairRDD<String, String> internRDD = inputRDD.mapToPair(keyValueFunc);
			System.out.println("Number of element in inputRDD is " + inputRDD.count());
			
			JavaPairRDD<String, String> outputRDD = internRDD.reduceByKey(new Function2<String, String, String>() {
				
				public String call(String doc1, String doc2) throws Exception {

					JSONParser parser = new JSONParser();
					JSONObject jsonObj1 = (JSONObject) parser.parse(doc1.split("\t")[1]);
					JSONObject jsonObj2 = (JSONObject) parser.parse(doc2.split("\t")[1]);
					String type1 = (String) jsonObj1.get("a");
					String type2 = (String) jsonObj2.get("a");
					if (type1.equals("Thread") && type2.equals("Thread")) {
						Object posts1 = jsonObj1.get("hasPost");
						Object posts2 = jsonObj2.get("hasPost");
						if (posts1 instanceof JSONArray && posts2 instanceof JSONArray) {

							return (((JSONArray) posts2).size() > ((JSONArray) posts1).size()) ? doc2 : doc1;

						} else if (posts1 instanceof JSONObject && posts2 instanceof JSONArray) {
							return doc2;

						} else {
							return doc1;
						}
					}
					return doc1;
				}
			});
			outputRDD.saveAsTextFile(outputFilename);
			
		}
		
		else {
		
		JavaPairRDD<Text, Text> inputRDD = sc.sequenceFile(inputFilename, Text.class, Text.class, numPartitions);
		inputRDD.saveAsTextFile(outputFilename);
		
		
		
		
/*
		JavaPairRDD<Text, Text> intermRDD = inputRDD.mapToPair(new PairFunction<Tuple2<Text,Text>, Text, Text>(){
			public Tuple2<Text, Text> call(Tuple2<Text, Text> tuple) throws Exception {

				JSONParser parser = new JSONParser();
				JSONObject jsonObj = (JSONObject) parser.parse(String.valueOf(tuple._2()));
				Text uriText = new Text((String) jsonObj.get("uri"));
				return new Tuple2<Text, Text>(uriText, tuple._2());
			}
		});
		
		JavaPairRDD<Text, Text> threadRDD = intermRDD.reduceByKey(new Function2<Text, Text, Text>() {

						public Text call(Text doc1, Text doc2) throws Exception {
							
							JSONParser parser = new JSONParser();
							JSONObject jsonObj1 = (JSONObject) parser.parse(String.valueOf(doc1));
							JSONObject jsonObj2 = (JSONObject) parser.parse(String.valueOf(doc2));
							String type1 = (String) jsonObj1.get("a");
							String type2 = (String) jsonObj2.get("a");
							if (type1.equals("Thread") && type2.equals("Thread")) {
								Object posts1 = jsonObj1.get("hasPost");
								Object posts2 = jsonObj2.get("hasPost");
								if (posts1 instanceof JSONArray && posts2 instanceof JSONArray) {
									return (((JSONArray) posts2).size() > ((JSONArray) posts1).size()) ? doc2 : doc1;

								} else if (posts1 instanceof JSONObject && posts2 instanceof JSONArray) {
									return doc2;

								} else {
									return doc1;
								}
							}
							return null;
							
						}}, numPartitions);
//			threadRDD.saveAsTextFile(outputFilename);
			threadRDD.saveAsNewAPIHadoopFile(outputFilename,Text.class, Text.class, SequenceFileOutputFormat.class);
			
*/
		}
		
		
	}
	
	
}
