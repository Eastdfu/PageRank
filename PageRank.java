package pagerank;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.io.IOException;


public class PageRank extends Configured implements Tool {
	private static int it = 50;
	private static double B = 0.85;
	private static double n = 0;
	@Override
	public int run(String[] args) throws Exception {
		// count
		Path relation = new Path("relation");
		Path v0 = new Path("midout/v" + 0);
		Path input = new Path(args[0]);
		Path list = new Path("list");
//		Path input = new Path("sample3.txt");
		
        Job jobcount = new Job(getConf(), "PageRank");
        jobcount.setJarByClass(PageRank.class);
        jobcount.setOutputKeyClass(Text.class);
        jobcount.setOutputValueClass(Text.class);
        
        jobcount.setMapperClass(Mapcount.class);
        jobcount.setReducerClass(Reducerelation.class);
        
        jobcount.setInputFormatClass(TextInputFormat.class);
        jobcount.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobcount, input);
        FileOutputFormat.setOutputPath(jobcount, relation);
        
        Job jobinital = new Job(getConf(), "PageRank");
        jobinital.setJarByClass(PageRank.class);
        jobinital.setOutputKeyClass(Text.class);
        jobinital.setOutputValueClass(Text.class);
        
        jobinital.setMapperClass(Mapread.class);
        jobinital.setReducerClass(Reduceprob.class);
        
        jobinital.setInputFormatClass(TextInputFormat.class);
        jobinital.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobinital, relation);
        FileOutputFormat.setOutputPath(jobinital, v0);
        if (jobcount.waitForCompletion(true)) {
        	if(jobinital.waitForCompletion(true)) {
        		for (int i = 0; i < it; i++) {
                	Path v;
            		if (i == 0) {
            			v = v0;
            		} else {
            			v = new Path("midout/v" + i);
            		}
            		Path mid = new Path("mid/" + i);
                	Path out;
                	if (i == it - 1) {
                		out = list;
                	} else {
                		out = new Path("midout/v" + (i + 1));
                	}
                	Job jobcal = new Job(getConf(), "PageRank");
            		jobcal.setJarByClass(PageRank.class);
            		MultipleInputs.addInputPath(jobcal, relation, TextInputFormat.class, Mapread.class);
            		MultipleInputs.addInputPath(jobcal, v, TextInputFormat.class, Mapread.class);
            		jobcal.setReducerClass(Reducecal.class);
            		
            		jobcal.setMapOutputKeyClass(Text.class);
            		jobcal.setMapOutputValueClass(Text.class);
            		
            		jobcal.setOutputKeyClass(Text.class);
            		jobcal.setOutputValueClass(DoubleWritable.class);
            		
            		FileOutputFormat.setOutputPath(jobcal, mid);
            		
            		Job jobsum = new Job(getConf(), "PageRank");
                    jobsum.setJarByClass(PageRank.class);
                    jobsum.setOutputKeyClass(Text.class);
                    jobsum.setMapOutputValueClass(Text.class);
                    jobsum.setOutputValueClass(DoubleWritable.class);
                  
                    jobsum.setMapperClass(Mapsum.class);
                    jobsum.setReducerClass(Reducesum.class);
                  
                    jobsum.setInputFormatClass(TextInputFormat.class);
                    jobsum.setOutputFormatClass(TextOutputFormat.class);
                  
                    FileInputFormat.addInputPath(jobsum, mid);
                    FileOutputFormat.setOutputPath(jobsum, out);
                    if (jobcal.waitForCompletion(true)) {
                    	jobsum.waitForCompletion(true);
                    }
                }
        	}
        }
        Job jobsort = new Job(getConf(), "PageRank");
        jobsort.setJarByClass(PageRank.class);
        jobsort.setOutputKeyClass(Text.class);
        jobsort.setOutputValueClass(DoubleWritable.class);
        jobsort.setMapOutputKeyClass(DoubleWritable.class);
        jobsort.setMapOutputValueClass(Text.class);     
      
        jobsort.setMapperClass(Mapsort.class);
        jobsort.setSortComparatorClass(dec.class);
        jobsort.setReducerClass(Reducesort.class);
        
        jobsort.setInputFormatClass(TextInputFormat.class);
        jobsort.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobsort, list);
        FileOutputFormat.setOutputPath(jobsort, new Path("output/sortedlist"));
        
        jobsort.waitForCompletion(true);
        
        return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		long s = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		long e = System.currentTimeMillis();
		System.out.println((e - s)/1000);
		System.exit(res);
	}
	
	public static class dec extends WritableComparator {     
		public dec() {
			super(DoubleWritable.class, true);
		}
		@Override
	    public int compare(Object a, Object b) {
			return -super.compare(a, b);    	
	    }
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);    	
	    }
	}
	
	public static class Mapsort extends Mapper<LongWritable, Text, DoubleWritable, Text> {     
		@Override
	    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    	String line = value.toString();
	    	if (line.charAt(0) != '#') {
	       		String[] ids = line.split("\\s+");
	       		Text v = new Text(ids[0]);
	       		DoubleWritable k = new DoubleWritable(Double.parseDouble(ids[1]));
	       		context.write(k, v);
	       	}
	    }
	}
	
	public static class Reducesort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}
	
	public static class Mapcount extends Mapper<LongWritable, Text, Text, Text> {     
		@Override
	    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    	String line = value.toString();
	    	if (line.charAt(0) != '#') {
	       		String[] ids = line.split("\\s+");
	       		Text k = new Text(ids[0]);
	       		Text v = new Text(ids[1]);
	       		context.write(k, v);
	       	}
	    }
	}
	public static class Reduceprob extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			double p = 1.0 / n;
			String v = String.valueOf(p);
			context.write(key, new Text(v));
		}
	}
	
	public static class Reducerelation extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			String v = "n";
			for (Text val : values) {
				v += val + " ";
			}
			n++;
			context.write(key, new Text(v));
		}
	}
	
	
	public static class Mapread extends Mapper<LongWritable, Text, Text, Text> {     
		@Override
	    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    	String line = value.toString();
       		String[] ids = line.split("\t");
       		Text k = new Text(ids[0]);
       		Text v = new Text(ids[1]);
       		context.write(k, v);
	    }
	}
	
	public static class Reducecal extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			String[] nodes = null;
			double pr = 0;
			for (Text val : values) {
				String temp = val.toString();
				if (temp.charAt(0) == 'n') {
					nodes = (temp.substring(1)).split("\\s+");
				} else {
					pr = Double.parseDouble(temp);
				}
			}
			if (nodes != null) {
				for (String node : nodes) {
					context.write(new Text(node), new DoubleWritable(pr / nodes.length));
				}
			}		
			context.write(key, new DoubleWritable(0));
		}
	}
	
	
	public static class Mapsum extends Mapper<LongWritable, Text, Text, Text> {     
		@Override
	    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    	String line = value.toString();
       		String[] ids = line.split("\t");
       		Text k = new Text(ids[0]);
       		Text v = new Text(ids[1]);
       		context.write(k, v);
	    }
	}
	
	public static class Reducesum extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			double pr = 0;
			for (Text v : values) {
				pr += Double.parseDouble(v.toString());
			}
			pr = B * pr + (1 - B) / n;
			context.write(key, new DoubleWritable(pr));
		}
	}
}
