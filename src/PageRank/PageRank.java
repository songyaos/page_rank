package PageRank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
	
	public static class Map extends Mapper<Text, Text,Text,Text >{
		
		@Override
		public void map(Text page_id, Text page_info, Context context) 
				throws IOException, InterruptedException{
			
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text page_id, Iterable<Text> page_info_list, Context context) 
				throws IOException, InterruptedException{
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		String O_INPUT_FILE_PATH = "sample_test.txt";
		String FORMAT_INPUT_FILE_PATH = "format_test.txt";
		String OUTPUT_FILE_PATH = "";
		int PAGES_NUM = 5716808;
		float INITIAL_RANK = 1/(float)PAGES_NUM;
		int MAX_ITER = 5;
		
		Configuration conf =  new Configuration();
		Job job = Job.getInstance(conf, "PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		O_INPUT_FILE_PATH = args[0];
		OUTPUT_FILE_PATH = args[1];		
		FileInputFormat.setInputPaths(job, new Path(O_INPUT_FILE_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE_PATH));
		//prepare input file reader
		FileReader fileReader = new FileReader(O_INPUT_FILE_PATH);
		BufferedReader input_buffer = new BufferedReader(fileReader);
		//prepare hadoop file system writer
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path FORMAT_PATH = new Path(FORMAT_INPUT_FILE_PATH);
		if(fs.exists(FORMAT_PATH)){
			fs.delete(FORMAT_PATH, true);
		}
		//FSDataOutputStream fout = fs.create(FORMAT_PATH);
		//BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fout));
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(FORMAT_PATH), SequenceFile.Writer.keyClass(Text.class),
	            SequenceFile.Writer.valueClass(Text.class));

		String line;
		String[] items = null;
		String page_id;
		String neighbors;
		while((line=input_buffer.readLine()) != null){
			items = line.split(":");
			page_id = items[0];
			neighbors = items[1];
			writer.append(new Text(page_id), new Text(INITIAL_RANK + "," + neighbors));
		}
		writer.close();
		fileReader.close();
		input_buffer.close();
		//update page rank loop 5 iterations.
		for(int i= 1;i<=MAX_ITER;i++){
			job = Job.getInstance(conf);
			FileInputFormat.setInputPaths(job, FORMAT_PATH);//read from and write to FORMAT_PATH
			FileOutputFormat.setOutputPath(job, FORMAT_PATH);
			
		}
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
