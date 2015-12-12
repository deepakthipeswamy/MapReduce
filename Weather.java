import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Weather {
	public static class Map1 extends Mapper < LongWritable, Text, Text, Text > {
		public void map(LongWritable inputKey, Text inputValue, Context inputContext) throws IOException, InterruptedException {

			//Input - : data set file 
			String textAsInput = inputValue.toString();
			
			//STN---,WBAN , YEARMODA,   TEMP,  ,   DEWP,  ,  SLP  ,  ,  STP  ,  , VISIB,  ,  WDSP,  , MXSPD,  GUST, PRCP  ,SNDP , FRSHTT,	
			//690190 13910  20060201_0  51.75     33.0 24  1006.3 24   943.9 24   15.0 24   10.7 24   22.0   28.9    0.00I 999.9  000000 

			boolean valid = true;

			// check if it is a valid line
			if (Character.isDigit(textAsInput.charAt(0))) {
				String toReducer = null;
				String inputArray[] = textAsInput.split("\\s+");
				
				double temp = Double.parseDouble(inputArray[3].toString());
				double dewPoint = Double.parseDouble(inputArray[4].toString());
				double windSpeed = Double.parseDouble(inputArray[12].toString());

				if (temp == 9999.9 || dewPoint == 9999.9 || windSpeed == 999.9) { 
				// ignore missing values
				// whole row is ommited if any of the values considered has missing value to it.
					valid = false;
				}

				if (valid) {
					toReducer = inputArray[0].concat("_").concat(inputArray[2].substring(4, 6));
					String hourStr = inputArray[2].split("_")[1];
					int hour = Integer.parseInt(hourStr);
					
					/*
					The data for each hour starts from 0 so, 5:01am data will be found from 5th hour.
					5:01 am to 11:00 am falls in 5,6,7,8,9,10
					11:01 am to 5:00 pm falls in 11,12,13,14,15,16					
					5:01 pm to 11:00 pm falls in 17,18,19,20,21,22
					11:01 pm to 5:00 am falls in 23,0,1,2,3,4
					
					23 and 0-4 = S1 
					5-10 = S2
					11-17 = S3
					18-22 = S4 
					*/
					
					if (hour >= 5 && hour < 11) { 		  //5:01 am to 11:00 am
						toReducer = toReducer + "_S1";
					} else if (hour >= 11 && hour < 17) { //11:01 am to 5:00 pm
						toReducer = toReducer + "_S2";
					} else if (hour >= 17 && hour < 23){ //5:01 pm to 11:00 pm
						toReducer = toReducer + "_S3";
					} else { 							//11:01 pm to 5:00 am
						toReducer = toReducer + "_S4";
					}

					//OutputKey - : StationId_Month_Section, Output value: Temperature_DewPoint_Wind

					inputContext.write(new Text(toReducer), new Text(temp + "_" + dewPoint + "_" + windSpeed));
				}
			}
		}
	}

	public static class Reduce1 extends Reducer < Text, Text, Text, Text > {
		public void reduce(Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

			//InputKey: StationId_Month_Section, Input value: Temperature_DewPoint_Wind

			Map < String, Double > tempWindDewMap = new HashMap < String, Double > ();

			tempWindDewMap.put("totalTemp", 0.0);
			tempWindDewMap.put("totalWindSpeed", 0.0);
			tempWindDewMap.put("totalDewPoint", 0.0);

			int count = 0;
			double tempOfStation = 0.0;
			double dewPointOfStation = 0.0;
			double windOfStation = 0.0;


			for (Text data: values) {
				count++;
				String dataArray[] = data.toString().split("_");
				
				// keep adding the total counts
				tempWindDewMap.put("totalTemp", tempWindDewMap.get("totalTemp") + Double.parseDouble(dataArray[0].toString()));
				tempWindDewMap.put("totalDewPoint", tempWindDewMap.get("totalDewPoint") + Double.parseDouble(dataArray[1].toString()));
				tempWindDewMap.put("totalWindSpeed", tempWindDewMap.get("totalWindSpeed") + Double.parseDouble(dataArray[2].toString()));
			}

			//OutputKey: StationId_Month_Section, Output value: avgTemperature_avgDewPoint_avgWindSpeed
			// find averages
			double avgTemp = (tempWindDewMap.get("totalTemp") / count);
			double avgDewPoint = (tempWindDewMap.get("totalDewPoint") / count);
			double avgWindSpeed = (tempWindDewMap.get("totalWindSpeed") / count);

			context.write(key, new Text(String.valueOf(avgTemp) + "," + String.valueOf(avgDewPoint) + "," + String.valueOf(avgWindSpeed)));

		}
	}

	public static class Map2 extends Mapper < Text, Text, Text, Text > {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			// Input key: StationId_Month_Section, Input value: avgTemperature_avgDewPoint_avgWindSpeed

			String str = key.toString();

			String[] strList = str.split("_");

			String stationId = strList[0];

			String month = strList[1];

			String newKey = stationId + "_" + month;

			String newValue = strList[2] + "_" + value.toString(); 

			// Output key: StationId_Month, Output value: SectionId_avgTemperature_avgDewPoint_avgWindSpeed
			context.write(new Text(newKey), new Text(newValue));
		}
	}

	public static class Reduce2 extends Reducer < Text, Text, Text, Text > {
		public void reduce(Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

			// Input key: StationId_Month, Input value: SectionId_avgTemperature,avgDewPoint,avgWindSpeed

			String s1 = null;
			String s2 = null;
			String s3 = null;
			String s4 = null;
			int count = 0;
			String section = null;

			for (Text data: values) {
				System.out.println("Reducer 2 : data = " + data.toString());
				count++;
				section = data.toString().split("_")[0];
				if (section.equalsIgnoreCase("S1")) {
					s1 = data.toString();
				} else if (section.equalsIgnoreCase("S2")) {
					s2 = data.toString();
				} else if (section.equalsIgnoreCase("S3")) {
					s3 = data.toString();
				} else if (section.equalsIgnoreCase("S4")) {
					s4 = data.toString();
				}
			}

			String newValue = s1 + "|" + s2 + "|" + s3 + "|" + s4;
			context.write(new Text(key), new Text(newValue));
		}
	}

	public static void main(String[] args) throws Exception {

		try {
			Configuration conf = new Configuration();

			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf, "first");
			job1.setJarByClass(Weather.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setMapperClass(Map1.class);
			job1.setReducerClass(Reduce1.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			//job1.setNumReduceTasks(7);
			//FileInputFormat.setMaxInputSplitSize(job1, 67108864);	
			//67108864 - 64mb
			//134217728 - 128mb
			//33554432 - 32mb
			FileOutputFormat.setOutputPath(job1, new Path("/user/hduser/tmpoutput"));
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			job1.waitForCompletion(true);

			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "second");
			job2.setJarByClass(Weather.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			//job2.setNumReduceTasks(7);
			//FileInputFormat.setMaxInputSplitSize(job2, 67108864);
			FileInputFormat.addInputPath(job2, new Path("/user/hduser/tmpoutput/"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			job2.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}