package Hadoop_Deg2friend;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Deg2friend {

	//map1
	public static class Map1 extends Mapper<Object, Text, Text, Text>
	{
		private Text map1_key = new Text();
		private Text map1_value = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] eachterm = value.toString().split(",");
			if (eachterm.length != 2) {
				return;
			}
			
			if (eachterm[0].compareTo(eachterm[1]) < 0) {
				map1_value.set(eachterm[0] + "\t" + eachterm[1]);
			}
			else if (eachterm[0].compareTo(eachterm[1]) > 0) {
				map1_value.set(eachterm[1] + "\t" + eachterm[0]);
			}
			
			map1_key.set(eachterm[0]);
			context.write(map1_key, map1_value);
			
			map1_key.set(eachterm[1]);
			context.write(map1_key, map1_value);
		}
	}
	//reduce1
	public static class Reduce1 extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Vector<String> hisFriends = new Vector<String>();
			
			for(Text val : values)
			{
				String[] eachterm = val.toString().split("\t");
				if (eachterm[0].equals(key.toString())) {
					hisFriends.add(eachterm[1]);
					context.write(val, new Text("deg1friend"));
				}
				if (eachterm[1].equals(key.toString())) {
					hisFriends.add(eachterm[0]);
					context.write(val, new Text("deg1friend"));
				}
			}
			
			for(int i = 0; i < hisFriends.size(); i++)
			{
				for(int j = 0; j < hisFriends.size(); j++)
				{
					if (hisFriends.elementAt(i).compareTo(hisFriends.elementAt(j)) < 0) {
						Text reduce_key = new Text(hisFriends.elementAt(i)+"\t"+hisFriends.elementAt(j));
						context.write(reduce_key, new Text("deg2friend"));
					}
				}
			}
		}
	}
	
	//map2
	public static class Map2 extends Mapper<Object, Text, Text, Text>
	{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");
			if (line.length == 3) {
				Text map2_key = new Text(line[0]+"\t"+line[1]);
				Text map2_value = new Text(line[2]);
				context.write(map2_key, map2_value);
			}
					
		}
	}
	
	//reduce2
	public static class Reduce2 extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			boolean isdeg1 = false;
			boolean isdeg2 = false;
			int count = 0;
			
			for(Text val : values)
			{
				if (val.toString().compareTo("deg1friend") == 0) {
					isdeg1 = true;
				}
				if (val.toString().compareTo("deg2friend") == 0) {
					isdeg2 = true;
					count++;
				}
			}
			
			if ((!isdeg1) && isdeg2) {
				context.write(new Text(String.valueOf(count)),key);
			}
		}
	}
	//main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	    if (otherArgs.length != 3) {
			System.err.println("Usage: Deg2friend <in> <temp> <out>");
			System.exit(2);
		}
	    Job job1 = new Job(conf, "Deg2friend");
	    job1.setJarByClass(Deg2friend.class);
	    job1.setMapperClass(Map1.class);
	    job1.setReducerClass(Reduce1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	    
	    if (job1.waitForCompletion(true)) {
			Job job2 = new Job(conf, "Deg2friend");
			job2.setJarByClass(Deg2friend.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			
			System.exit(job2.waitForCompletion(true)? 0 : 1);
			
		}
	    System.exit(job1.waitForCompletion(true)? 0 : 1);
	}
}
