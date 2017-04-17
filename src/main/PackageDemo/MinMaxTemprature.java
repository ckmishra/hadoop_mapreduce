package main.PackageDemo;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MinMaxTemprature {

	public static String calOutputName = "California";
	public static String nyOutputName = "Newyork";
	public static String njOutputName = "Newjersy";
	public static String ausOutputName = "Austin";
	public static String bosOutputName = "Boston";
	public static String balOutputName = "Baltimore";

	public static class MinMaxMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			float minTemp = Float.MAX_VALUE;
			float maxTemp = Float.MIN_VALUE;
			LinkedList<String> line = new LinkedList<>();
			StringTokenizer st = new StringTokenizer(value.toString(), "\t");
			while (st.hasMoreTokens()) {
				line.add(st.nextToken());
			}

			Text keyOutput = new Text(line.getFirst());
			Text maxTempValueOutput = new Text();
			Text minTempValueOutput = new Text();
			for (int i = 1; i < line.size() - 1; i = i + 2) {
				String time = line.get(i);
				float temp = Float.parseFloat(line.get(i + 1));
				if (temp < minTemp) {
					minTemp = temp;
					minTempValueOutput.set(minTemp + "AND" + time);
				}
				if (temp > maxTemp) {
					maxTemp = temp;
					maxTempValueOutput.set(maxTemp + "AND" + time);
				}

			}
			context.write(keyOutput, minTempValueOutput);
			context.write(keyOutput, maxTempValueOutput);
		}
	}

	public static class MinMaxTempReducer extends Reducer<Text, Text, Text, Text> {

		MultipleOutputs<Text, Text> mos;

		@Override
		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp1 = "";
			String time1 = "";
			String temp2 = "";
			String time2 = "";
			String reducerInputStr[] = null;
			int counter = 0;
			for (Text value : values) {
				reducerInputStr = value.toString().split("AND");
				if (counter == 0) {
					temp1 = reducerInputStr[0];
					time1 = reducerInputStr[1];
				} else {
					temp2 = reducerInputStr[0];
					time2 = reducerInputStr[1];
				}
				counter++;
			}
			String result = "";
			if (Float.parseFloat(temp1) > Float.parseFloat(temp2)) {
				result = "Time: " + time1 + " MaxTemp: " + temp1 + " Time: " + time2 + " MinTemp: " + temp2;
			} else {
				result = "Time: " + time2 + " MaxTemp: " + temp2 + " Time: " + time1 + " MinTemp: " + temp1;
			}

			String fileName = "";
			if (key.toString().substring(0, 2).equals("CA")) {
				fileName = MinMaxTemprature.calOutputName;
			} else if (key.toString().substring(0, 2).equals("NY")) {
				fileName = MinMaxTemprature.nyOutputName;
			} else if (key.toString().substring(0, 2).equals("NJ")) {
				fileName = MinMaxTemprature.njOutputName;
			} else if (key.toString().substring(0, 3).equals("AUS")) {
				fileName = MinMaxTemprature.ausOutputName;
			} else if (key.toString().substring(0, 3).equals("BOS")) {
				fileName = MinMaxTemprature.bosOutputName;
			} else if (key.toString().substring(0, 3).equals("BAL")) {
				fileName = MinMaxTemprature.balOutputName;
			}
			String strArr[] = key.toString().split("_");
			key.set(strArr[1]); // Key is date value
			mos.write(fileName, key, new Text(result));
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// conf.addResource(new
		// Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		// conf.addResource(new
		// Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		// conf.addResource(new
		// Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

		Job job = Job.getInstance(conf, "Wheather Statistics of USA");
		job.setJarByClass(MinMaxTemprature.class);

		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxTempReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, calOutputName, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, nyOutputName, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, njOutputName, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, bosOutputName, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, ausOutputName, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, balOutputName, TextOutputFormat.class, Text.class, Text.class);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(args[2]))) {
			hdfs.delete(new Path(args[2]), true);
		}
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
