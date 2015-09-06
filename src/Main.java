
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class Main {

	static String listFileName = "Backup\\list.csv";
	static String logFilePath = "Backup\\";
	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setAppName("SportStatistics").setMaster("local[2]")
	             	             .set("spark.executor.memory", "1g");//.setMaster(master);
		JavaSparkContext spark = new JavaSparkContext(conf);
		//c:\\Users\\kppet\\Documents\\GitHub\\java\\workspace\\SparkBetStatistic\\
		JavaRDD<String> textFile = spark.textFile("Backup\\");
		Scanner in = new Scanner(System.in);
		Boolean ans = false;
		Integer choose = 0;
		System.out.println("1. FullMap");
		System.out.println("2. Specified sport/odds");
        while(!ans){
        	System.out.println("Ans =");
        	choose = in.nextInt();
        	ans  = choose == 1 || choose == 2;
        }
        if(choose == 1 )
        	allSport(textFile);
        else
        	oneSport(textFile);
	}
	public static void allSport(JavaRDD<String> textFile) {
		Long startTime = System.nanoTime();
		Map<String,Integer> result1 =// new HashMap<String, Integer>();
		textFile.filter(new Function<String, Boolean>() {
			  public Boolean call(String s) { return (s.contains("ok")||s.contains("x"))&&s.split(",").length>=14; }
		}).map(line->line.split(",")).mapToPair(s -> new Tuple2<String , String[]>(s[0],s)).reduceByKey((x,y)->x).values()
		.mapToPair(s -> new Tuple2<String , Integer>(s[2] + " "+s[13]+" "+(s[s.length-1].contains("ok")?"ok":"x"),1))		
		.reduceByKey((x, y) -> x + y)
		//.mapToPair(p -> new Tuple2<String , Double>(p._1.split(" ")[0]+" "+p._1.split(" ")[1],(double)p._2))
		.collectAsMap()
		
		;
		//System.out.println(result1.size());
		Map<String,Double> result2 = new HashMap<String,Double>();
		for(String desc : result1.keySet() ){
			//System.out.println(desc);
			if(desc.contains("ok")){
				String[] sa = desc.split(" ");
				result2.put(sa[0]+" "+sa[1],(double)result1.get(desc));
			}
		}
		for(String desc : result1.keySet()){
			if(desc.contains("x")){
				String[] sa = desc.split(" ");
				if(result2.containsKey(sa[0]+" "+sa[1])){
					//System.out.println("OLD: "+sa[0]+" "+sa[1]+" : "+result2.get(sa[0]+" "+sa[1]));
					result2.put(sa[0]+" "+sa[1],(double)result2.get(sa[0]+" "+sa[1])/ ((double)result2.get(sa[0]+" "+sa[1])+(double)result1.get(desc)));
					//System.out.println("NEW: "+sa[0]+" "+sa[1]+" : "+result2.get(sa[0]+" "+sa[1]));
				}
				else
					result2.put(sa[0]+" "+sa[1],0d);
			}
		}
		System.out.println("Results: ");
		//System.out.println("Result1: "+result1.size());
		for(String desc : result2.keySet() ){
			System.out.println(desc + " -> "+(result2.get(desc)*100>100?100:result2.get(desc)*100)+"%");
		
		}
		String res= "";
		Long endTime = System.nanoTime();
		Long startTime1=System.nanoTime() ;
		Set<String> eventTypeMap = new TreeSet<String>();
		for(String s : result2.keySet())
		{
			eventTypeMap.add(s.split(" ")[0]);
		}
		for (String sportname: eventTypeMap)				
		{
		
		
		Map<Double, Integer> winnerCountrByOddsMap = new TreeMap<Double, Integer>();
		Map<Double, Integer> sumCountByOddsMap = new TreeMap<Double, Integer>();
		List<String> marketIds = new ArrayList<String>();
		File file = new File(listFileName);
		try {
			Scanner listScanner = new Scanner(file, "UTF-8");
			while(listScanner.hasNextLine())
			{
		
				String line = listScanner.nextLine();
				String filename = logFilePath+line.split(",")[0];					
				System.out.println("filename: "+filename);
				File resultFile = new File(filename);
				Scanner resultScanner = new Scanner(resultFile);
				while(resultScanner.hasNextLine())
				{
				
					
					String resLine = resultScanner.nextLine();

					List<String> resLineList = Arrays.asList(resLine.split(","));
			
					if(resLine.contains(sportname)&&!marketIds.contains(resLineList.get(0))){
						
						String lineEnd = resLineList.get(resLineList.size()-1);
						if(lineEnd.contains("x")||lineEnd.contains("ok"))
						{
							Double odds = 0.;
							try{
							 odds = Double.parseDouble(resLineList.get(13));
							}
							catch(Exception e)
							{
								System.out.println("Oddds incorrect!");
							}
							//összes meccs számlálás
							if(sumCountByOddsMap.containsKey(odds) ){
								sumCountByOddsMap.replace(odds, sumCountByOddsMap.get(odds)+1);
								if(lineEnd.contains("ok"))
									winnerCountrByOddsMap.replace(odds, winnerCountrByOddsMap.get(odds)+1);
							}
							//győztes meccs számlálás 
							else{	
								sumCountByOddsMap.put(odds, 1);
								winnerCountrByOddsMap.put(odds, 0);
								if(lineEnd.contains("ok"))
									winnerCountrByOddsMap.replace(odds, winnerCountrByOddsMap.get(odds)+1);
							}
							marketIds.add(resLineList.get(0));
								
						}
					}
				}
				
				resultScanner.close();
				
			}
			listScanner.close();
			
			
		
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		res+=sportname+"\n";
			
		Integer oddsInIntervalCount = 0;
		Double expectableYieldInInterval=0.;
		Integer matchCountInInterval = 0;
		Double expectableYieldInIntervalWighted=0.;
		Double highestProbability = 1.;
		Double highestProbabilityUpperBorderOdds=1.1;
		Double highestProbabilityLowerBorderOdds=1.1;
		List<String> borders = new ArrayList<String>();
		boolean added = false;
		Integer countForStat = 0;
		for(Double odds : sumCountByOddsMap.keySet()){
			Integer sum = sumCountByOddsMap.get(odds);
			Integer winner = winnerCountrByOddsMap.get(odds);


			res += sportname+ " "+ odds+ " -> "+(double)winner/(double)sum+"\n";
		}

		Long endTime1=System.nanoTime() ;
			System.out.println(res);
			

			System.out.println("Spark Elapsed time = "+(endTime-startTime)/1000+" ns.");
			System.out.println("Native Elapsed time = "+(endTime1-startTime1)/1000+" ns.");
	}
	}
	public static void oneSport(JavaRDD<String> textFile) {
		Long startTime = System.nanoTime();
		JavaRDD<String> sportsAndOdds=// new HashMap<String, Integer>();
		textFile.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ return (s.contains("ok")||s.contains("x"))&&s.split(",").length>=14; }
		})		
		.map(line->line.split(","))
		.map(s ->(s[2] +","+ s[13])).distinct();//.collect();
		
		JavaRDD<String> sports = sportsAndOdds.map(line->line.split(",")).map(s ->(s[0])).distinct();
		List<String> sportList = sports.collect();
		
		
		
		int i = 1;
		for(String s : sportList)
			System.out.println((i++) +" "+s);
		Scanner sc = new Scanner(System.in);
		Integer choose = 0;
		Boolean ans = false;
		while(!ans)
		{
			System.out.println("Ans = ");
			choose = sc.nextInt();
			ans = choose > 0 && choose<= sportList.size();
		}
		String sportname = sportList.get(choose - 1);
		
		List<String> oddsListForsport =
		sportsAndOdds.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.contains(sportname)/*&&s.contains(odds)*/; }
		}).map(line->line.split(",")).map(s->s[s.length-1]).distinct().collect();
		
		
		i = 1;
		for(String s : oddsListForsport)
			System.out.println((i++) +" "+s);
		
		choose = 0;
		ans = false;
		while(!ans)
		{
			System.out.println("Ans = ");
			choose = sc.nextInt();
			ans = choose > 0 && choose<= oddsListForsport.size();
		}
		Double odds = Double.parseDouble( oddsListForsport.get(choose - 1));
		startTime=System.nanoTime() ;
		JavaRDD<String> all =
		textFile.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.length()>=14&& s.contains(sportname)&&s.contains(odds.toString()+",")&&(s.contains("ok")||s.contains("x")); }
		}).map(s->s.split(",")).mapToPair(s -> new Tuple2<String , String>(s[0],Arrays.toString(s))).reduceByKey((x,y)->x).values();
		JavaRDD ok = all.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.contains("ok"); }
		});
		Long endTime=System.nanoTime() ;
		
		
		Long startTime1=System.nanoTime() ;
		
		Map<Double, Integer> winnerCountrByOddsMap = new TreeMap<Double, Integer>();
		Map<Double, Integer> sumCountByOddsMap = new TreeMap<Double, Integer>();
		List<String> marketIds = new ArrayList<String>();
		File file = new File(listFileName);
		try {
			Scanner listScanner = new Scanner(file, "UTF-8");
			while(listScanner.hasNextLine())
			{
		
				String line = listScanner.nextLine();
				String filename = logFilePath+line.split(",")[0];					
				System.out.println("filename: "+filename);
				File resultFile = new File(filename);
				Scanner resultScanner = new Scanner(resultFile);
				while(resultScanner.hasNextLine())
				{
				
					
					String resLine = resultScanner.nextLine();
//					if(logWriter!=null)
//						logWriter.write(resLine+"\n");
					List<String> resLineList = Arrays.asList(resLine.split(","));
					//adott sport + feldolgoztuk -e már
					if(resLine.contains(sportname)&&!marketIds.contains(resLineList.get(0))){
						
						String lineEnd = resLineList.get(resLineList.size()-1);
						if(lineEnd.contains("x")||lineEnd.contains("ok"))
						{
														//összes meccs számlálás
							if(sumCountByOddsMap.containsKey(odds) ){
								sumCountByOddsMap.replace(odds, sumCountByOddsMap.get(odds)+1);
								if(lineEnd.contains("ok"))
									winnerCountrByOddsMap.replace(odds, winnerCountrByOddsMap.get(odds)+1);
							}
							//győztes meccs számlálás 
							else{	
								sumCountByOddsMap.put(odds, 1);
								winnerCountrByOddsMap.put(odds, 0);
								if(lineEnd.contains("ok"))
									winnerCountrByOddsMap.replace(odds, winnerCountrByOddsMap.get(odds)+1);
							}
							marketIds.add(resLineList.get(0));
								
						}
					}
				}
				
				resultScanner.close();
				
			}
			listScanner.close();
	
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
			Integer sum = sumCountByOddsMap.get(odds);
			Integer winner = winnerCountrByOddsMap.get(odds);
			
			Long endTime1 = System.nanoTime();
			System.out.println("Spark: "+sportname+" "+odds.toString() +" "+((double)ok.count()/(double)all.count())+" Elapsed time "+(endTime-startTime)/1000+"ns" );
		
			System.out.println("Native Java: "+sportname+" "+odds.toString() +" "+((double)winner/(double)sum)+" Elapsed time "+(endTime1-startTime1)/1000+"ns.");

	}


}
