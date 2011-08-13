package org.hackreduce.streetmappers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class StreetMappers extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		if(args.length!=2){
			System.err.println("Usage:"+getClass().getName()+"<input><output>");
			System.exit(2);
		}
		
		//create the MapReduce job: denom 
		Job jDenom=new Job(conf);
		jDenom.setJarByClass(getClass());
		jDenom.setJobName(getClass().getName());
		
		
		//job denormalization
		 jDenom.setInputFormatClass();//C: hdfs writables
		 
		
		
	}//end run
	
}//end  class StreeMappers
