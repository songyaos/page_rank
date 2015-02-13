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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//import org.apache.log4j.Logger;

public class PageRank {
	private static int PAGES_NUM = 6;//5716808;
	
	public static class Map extends Mapper<Text, Text,Text,Text >{
		//private static final Logger sLogger = Loggr.getLogger(Map.class);
		private String page_info = "";
		private String[] page_info_list = null;
		private String[] neighbors = null;
		private double this_page_current_rank = 0;
		private double out_page_rank = 0;
		@Override
		public void map(Text page_id_text, Text page_info_text, Context context) 
				throws IOException, InterruptedException{
			context.write(page_id_text,page_info_text);
			page_info = page_info_text.toString().trim();
			page_info_list = page_info.split("-");
			this_page_current_rank = Double.parseDouble(page_info_list[0]);
			neighbors = page_info_list[1].split("\\s+");//split by space
			
			
			out_page_rank = this_page_current_rank/neighbors.length;
			
			for (String neighbor: neighbors){
				context.write(new Text(neighbor), new Text(Double.toString(out_page_rank)));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private double sum = 0;
		private String output_page_info = "";
		private String neighbors = "";
		private static double DAMPING = 0.85; //damping factor
		private double out_page_rank = 0; //damping factor
		@Override
		public void reduce(Text page_id, Iterable<Text> page_info_list, Context context) 
				throws IOException, InterruptedException{
			sum = 0;
			for(Text page_info_text : page_info_list){
				String[] page_info = page_info_text.toString().split("-");
				if (page_info.length > 1){
					neighbors = page_info[1].trim();
				}
				sum += Double.parseDouble(page_info[0]);
			}
			out_page_rank = DAMPING*sum + (1-DAMPING)/PAGES_NUM;
			output_page_info = Double.toString(out_page_rank) + "-" + neighbors;
			
			context.write(page_id,new Text(output_page_info));
		}
	}
	
	public static void main(String[] args) throws Exception{
		String O_INPUT_FILE_PATH = "";// initial input path
		String INNER_FILE_PATH = "";
		String F_OUTPUT_FILE_PATH = "";//final output path
		
		double INITIAL_RANK = 1/(double)PAGES_NUM;
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
		
		//get file path from arguments
		O_INPUT_FILE_PATH = args[0];
		F_OUTPUT_FILE_PATH = args[1];
		INNER_FILE_PATH = args[2];
		
		//set initial file path
		FileInputFormat.setInputPaths(job, new Path(INNER_FILE_PATH + "00"));
		FileOutputFormat.setOutputPath(job, new Path(INNER_FILE_PATH + "0"));//1st inner path
		
		//prepare input file reader
		FileReader fileReader = new FileReader(O_INPUT_FILE_PATH);
		BufferedReader input_buffer = new BufferedReader(fileReader);
		//prepare hadoop file system writer
		//FileSystem fs = FileSystem.get(job.getConfiguration());
		
		//FSDataOutputStream fout = fs.create(FORMAT_PATH);
		//BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fout));
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
											SequenceFile.Writer.file(new Path(INNER_FILE_PATH + "00")), 
											SequenceFile.Writer.keyClass(Text.class),
											SequenceFile.Writer.valueClass(Text.class));
		String line;
		String[] items = null;
		String page_id;
		String neighbors;
		while((line=input_buffer.readLine()) != null){
			items = line.split(":");
			page_id = items[0].trim();
			neighbors = items[1].trim();
			writer.append(new Text(page_id), new Text(INITIAL_RANK + "-" + neighbors));
		}
		writer.close();
		fileReader.close();
		input_buffer.close();
		job.waitForCompletion(true);//1st job
		//update page rank loop 5 iterations.
		for(int i= 1;i<=MAX_ITER;i++){
			job = Job.getInstance(conf);
			job.setJarByClass(PageRank.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path(INNER_FILE_PATH + Integer.toString(i-1)));//read from and write to FORMAT_PATH
			if (i==MAX_ITER)//final output path
			{
				FileOutputFormat.setOutputPath(job, new Path(F_OUTPUT_FILE_PATH));
			}
			else //inner loop path
			{
				FileOutputFormat.setOutputPath(job, new Path(INNER_FILE_PATH + Integer.toString(i)));
			}
			
			job.waitForCompletion(true);//job iterations
		}
		
		
		
	}
}
