import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;


public class DeDupPostUsingSpark {
	
	public static void main(String[] args) throws ClassNotFoundException {
		
		String inputFileName = args[0];
		String type=args[1];
		String outputFileName = args[2];
		Integer numOfPartitions = Integer.parseInt(args[3]);
		
		SparkConf conf = new SparkConf().setAppName("deduplicatePosts").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = sc.textFile(inputFileName,numOfPartitions);

		if(type.equals("text")){
			
			PairFunction<String, String, String> sha1function = new PairFunction<String, String, String>() {
				
				public Tuple2<String, String> call(String line) throws Exception {
					JSONParser parser = new JSONParser();
					String url = line.split("\t")[0];
					String doc = line.split("\t")[1];
					
					JSONObject jsonObj = (JSONObject) parser.parse(doc);
					String rawText = (String) jsonObj.get("raw_text");
					
					String sha1Value = sha1(rawText);
					
					return new Tuple2<String, String>(sha1Value, doc);
				}
			};
			JavaPairRDD<String, String> sha1RDD = inputRDD.mapToPair(sha1function);
			sha1RDD.repartition(numOfPartitions);
			sha1RDD.cache();
			
			JavaPairRDD<String, String> reducedRDD = sha1RDD.reduceByKey(new Function2<String, String, String>() {
				
				public String call(String v1, String v2) throws Exception {
					System.out.println("URL1 is ");
					JSONParser parser = new JSONParser();
					JSONObject jsonObj1 = (JSONObject) parser.parse(v1);
					System.out.println(jsonObj1.get("url"));
					long timestamp1 = 1;
					try{
						System.out.println("timestamp1 is "+jsonObj1.get("timestamp"));
						timestamp1 = (Long) jsonObj1.get("timestamp");	
					}catch(ClassCastException e){
						e.printStackTrace();
					}
					JSONObject jsonObj2 = (JSONObject) parser.parse(v2);
					System.out.println("URL2 is ");
					System.out.println(jsonObj2.get("url"));
					long timestamp2=1;
					try{
						System.out.println("timestamp2 is "+jsonObj2.get("timestamp"));
						timestamp2 = (Long) jsonObj2.get("timestamp");
					}catch(ClassCastException e){
						e.printStackTrace();
					}
					if(timestamp1 > timestamp2){
						return jsonObj1.toJSONString();
					}else{
						return jsonObj2.toJSONString();
					}
				}
			},numOfPartitions);
			
			JavaPairRDD<String, String> finalRddwithURL = reducedRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
				public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
					JSONParser parser = new JSONParser();
					String doc = tuple._2();
					JSONObject jsonObj = (JSONObject) parser.parse(doc);
					String url = (String) jsonObj.get("url");
					return new Tuple2<String, String>(url, doc);
				}
			});
			
			finalRddwithURL.saveAsTextFile(outputFileName);
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
