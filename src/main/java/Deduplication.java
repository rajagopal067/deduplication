import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;


public class Deduplication {
	
	
public static void main(String[] args) throws ClassNotFoundException, IOException {
		
		String inputFileName = args[0];
		String type=args[1];
		String outputFileName = args[2];
		Integer numOfPartitions = Integer.parseInt(args[3]);
		final File statsFile = new File("statsFile.txt"); 
		
		SparkConf conf = new SparkConf().setAppName("deduplicatePosts").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		if(type.equals("seq")){
			
			JavaPairRDD<Text, Text> inputRDD = sc.sequenceFile(inputFileName,Text.class,Text.class,numOfPartitions);
			
			JavaPairRDD<String, String> inputStringRDD = inputRDD.mapToPair(new PairFunction<Tuple2<Text,Text>, String, String>() {
				public Tuple2<String, String> call(Tuple2<Text, Text> tuple) throws Exception {
					return new Tuple2<String, String>(tuple._1().toString(), tuple._2().toString());
				}
			});
			
			JavaPairRDD<String, String> postsStringRDD = inputStringRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
				
				public Boolean call(Tuple2<String, String> tuple) throws Exception {
					JSONParser parser = new JSONParser();
					JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
					String type = (String) jsonObj.get("a");
					return type.equals("Post") || type.equals("post");
					
				}
			});
			
			
			JavaPairRDD<String, String> remainingStringRDD = inputStringRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
				
				public Boolean call(Tuple2<String, String> tuple) throws Exception {
					// TODO Auto-generated method stub
					JSONParser parser = new JSONParser();
					JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
					String type = (String) jsonObj.get("a");
					
					return !type.equals("Post") && !type.equals("Thread");
				}
			});
			
			
			JavaPairRDD<String, String> postsStringIntermRDD =  postsStringRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
				public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
					
					JSONParser parser = new JSONParser();
					JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
					String rawText = null;
					if(jsonObj.containsKey("rawText")){
						rawText = (String) jsonObj.get("raw_text");	
					}else if(jsonObj.containsKey("hasBodyPart") && jsonObj.get("hasBodyPart") instanceof JSONObject){
						JSONObject hasBodyPart = (JSONObject) jsonObj.get("hasBodyPart");
						rawText = (String) hasBodyPart.get("text");
					}
					
					String sha1Value=null;
					if(rawText == null){
						System.out.println("rawtext is null");
						System.out.println(tuple._2());
					}
					else{
						sha1Value = sha1(rawText);
					}
					return new Tuple2<String, String>(sha1Value, tuple._2());				
				}
				});
			
			JavaPairRDD<String, String> postsStringReducedRDD =  postsStringIntermRDD.reduceByKey(new Function2<String, String, String>() {
				public String call(String doc1, String doc2) throws Exception {
					
					System.out.println("URL1 is ");
					JSONParser parser = new JSONParser();
					JSONObject jsonObj1 = (JSONObject) parser.parse(doc1);
					System.out.println(jsonObj1.get("url"));
					long timestamp1 = 1;
					try{
						System.out.println("timestamp1 is "+jsonObj1.get("timestamp"));
						if(jsonObj1.containsKey("timestamp"))
							timestamp1 = (Long) jsonObj1.get("timestamp");	
					}catch(ClassCastException e){
						e.printStackTrace();
					}
					JSONObject jsonObj2 = (JSONObject) parser.parse(doc2);
					System.out.println("URL2 is ");
					System.out.println(jsonObj2.get("url"));
					long timestamp2=1;
					try{
						System.out.println("timestamp2 is "+jsonObj2.get("timestamp"));
						if(jsonObj2.containsKey("timestamp"))
							timestamp2 = (Long) jsonObj2.get("timestamp");
					}catch(ClassCastException e){
						e.printStackTrace();
					}
					if(timestamp1 > timestamp2){
						return doc1;
					}else{
						return doc2;
					}
				}
			},numOfPartitions);
			
			JavaPairRDD<Text, Text> postsFinalRDD = postsStringReducedRDD.mapToPair(new PairFunction<Tuple2<String,String>, Text, Text>() {
				public Tuple2<Text, Text> call(Tuple2<String, String> tuple)
						throws Exception {
					JSONParser parser = new JSONParser();
					JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
					Text uri = new Text((String) jsonObj.get("uri"));
					
					return new Tuple2<Text, Text>(uri, new Text(tuple._2()));
				}
			});
			
			postsFinalRDD.saveAsNewAPIHadoopFile(outputFileName+"-posts", Text.class, Text.class, SequenceFileOutputFormat.class);
			postsFinalRDD.saveAsTextFile(outputFileName+"-posts-text");
			
			JavaPairRDD<String, String> threadsStringRDD = inputStringRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
				
				public Boolean call(Tuple2<String, String> tuple) throws Exception {

					JSONParser parser = new JSONParser();
					JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
					String type = (String) jsonObj.get("a");
					return type.equals("Thread") || type.equals("thread");
				}
			});
			
			
			JavaPairRDD<String, String> threadsStringIntermRDD = threadsStringRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				
				JSONParser parser = new JSONParser();
				String doc = String.valueOf(tuple._2());
				JSONObject jsonObj = (JSONObject) parser.parse(doc);
				String source = null;
				if(jsonObj.containsKey("publisher")){
					source = (String) ((JSONObject) jsonObj.get("publisher")).get("name"); 
				}else{
					source = (String) ((JSONObject) jsonObj.get("provider")).get("name");
				}
				
				String identifier=null;
				
				if(jsonObj.containsKey("identifier") && jsonObj.get("identifier") instanceof JSONArray){
				}
				else if(jsonObj.containsKey("identifier") && jsonObj.get("identifier") instanceof JSONObject){
					identifier = (String) ((JSONObject) jsonObj.get("identifier")).get("name");
				}
				
				return new Tuple2<String, String>(source + "_" + identifier, tuple._2());
			}
			
			});
			
			
			JavaPairRDD<String, String> threadsStringReducedRDD = threadsStringIntermRDD.reduceByKey(new Function2<String, String, String>() {
				
				public String call(String doc1, String doc2) throws Exception {

					JSONParser parser = new JSONParser();
					JSONObject jsonObj1 = (JSONObject) parser.parse(doc1);
					JSONObject jsonObj2 = (JSONObject) parser.parse(doc2);
					String type1 = (String) jsonObj1.get("a");
					String type2 = (String) jsonObj2.get("a");
					System.out.println("\n\n\n");
					FileUtils.writeStringToFile(statsFile, "identifier for doc1 is "+(String) ((JSONObject) jsonObj1.get("identifier")).get("name") + "\n",true);
					FileUtils.writeStringToFile(statsFile, "doc1 is "+doc1 + "\n",true);
					FileUtils.writeStringToFile(statsFile, "identifier for doc2 is "+(String) ((JSONObject) jsonObj2.get("identifier")).get("name") + "\n",true);
					FileUtils.writeStringToFile(statsFile, "doc2 is "+doc2 + "\n",true);
					
					if (type1.equals("Thread") && type2.equals("Thread")) {
						Object posts1 = jsonObj1.get("hasPost");
						Object posts2 = jsonObj2.get("hasPost");
						if (posts1 instanceof JSONArray && posts2 instanceof JSONArray) {
							return (((JSONArray) posts2).size() >= ((JSONArray) posts1).size()) ? doc2 : doc1;

						} 
					}
				
					return doc1;
					}
			},numOfPartitions);
			
			threadsStringReducedRDD.saveAsTextFile(outputFileName+"-threads-reduced-text");
			
			File numberOfLinesFile = new File("numberOfLines.txt");
			
			FileUtils.writeStringToFile(numberOfLinesFile,"Number of line in interm thread is "+ threadsStringIntermRDD.count()+"\n",true);
			FileUtils.writeStringToFile(numberOfLinesFile,"Number of line in reduced thread is "+ threadsStringReducedRDD.count()+"\n",true);
			
			
			JavaPairRDD<Text, Text> threadsFinalRDD = threadsStringReducedRDD.mapToPair(new PairFunction<Tuple2<String,String>, Text, Text>() {
			public Tuple2<Text, Text> call(Tuple2<String, String> tuple) throws Exception {

				JSONParser parser = new JSONParser();
				JSONObject jsonObj = (JSONObject) parser.parse(tuple._2());
				Text uri = new Text((String) jsonObj.get("uri"));
				
				return new Tuple2<Text, Text>(uri, new Text(tuple._2()));
			}
			
			});
			
			threadsFinalRDD.saveAsNewAPIHadoopFile(outputFileName+"-threads", Text.class, Text.class, SequenceFileOutputFormat.class);
			threadsFinalRDD.saveAsTextFile(outputFileName+"-threads-text");
			
		}
	}
	
	
	private static String sha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
         
        return sb.toString();
    }


}
