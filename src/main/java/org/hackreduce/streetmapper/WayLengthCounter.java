package org.hackreduce.streetmapper;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.streetmapper.model.NodeId;
import org.hackreduce.streetmapper.model.NodeRecord;
import org.hackreduce.streetmapper.model.OsmRecord;
import org.hackreduce.streetmapper.model.ResolvedWay;
import org.hackreduce.streetmapper.model.ResolvedWayNode;
import org.hackreduce.streetmapper.model.WayId;
import org.hackreduce.streetmapper.model.WayNodeRecord;
import org.hackreduce.streetmapper.model.WayRecord;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

/**
 * This MapReduce job will count the total number kilometers of ways in the data dump.
 */
public class WayLengthCounter extends Configured implements Tool {

	public enum Count {
		TOTAL_RECORDS,
		WAY_RECORDS,
		NODE_RECORDS,
		WAY_METERS
	}

	/**
	 * Explodes {@link WayRecord}s into many {@link WayNodeRecord}.
	 * For each {@link WayRecord} read, writes one {@link WayNodeRecord} for each node it references.
	 * For each {@link NodeRecord} read, writes only one {@link WayNodeRecord}.
	 * All {@link WayNodeRecord} written are keyed by {@link NodeId}.  
	 */
	public static class WayExplosionMapper extends WayMapper<NodeId, WayNodeRecord> {

		@Override
		protected void map(OsmRecord record, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			if (record instanceof WayRecord) {
				context.getCounter(Count.WAY_RECORDS).increment(1);
				WayRecord wayRecord = (WayRecord) record;
				for (Object ref : wayRecord.getNodeRefs()) {
					context.write(((NodeId) ref), new WayNodeRecord(wayRecord));
				}
			} else if (record instanceof NodeRecord) {
				context.getCounter(Count.NODE_RECORDS).increment(1);
				NodeRecord nodeRecord = (NodeRecord) record;
				context.write(nodeRecord.getId(), new WayNodeRecord(nodeRecord));
			}
		}
	}

	/**
	 * Creates pairs of ({@link WayRecord}, {@link NodeRecord}) as {@link ResolvedWayNode}, which is more useful
	 * than the original {@link WayNodeRecord}, which were pairs of ({@link WayRecord}, {@link NodeId}).
	 * <p>
	 * Output is keyed by way ID, so the next MapReduce job can merge into a single ResolvedWay
	 * all its {@link ResolvedWayNode}s.
	 */
	public static class WayNodeReducer extends Reducer<NodeId, WayNodeRecord, WayId, ResolvedWayNode> {

		@Override
		protected void reduce(NodeId nodeId, Iterable<WayNodeRecord> records, Context context) throws IOException, InterruptedException {
			ArrayList<NodeRecord> nodes = new ArrayList<NodeRecord>();
			ArrayList<WayRecord> ways = new ArrayList<WayRecord>();
			
			// First pass: populate maps
			for (WayNodeRecord record : records) {
				if (record.get() instanceof NodeRecord) {
					NodeRecord node = (NodeRecord) record.get();
					nodes.add(node);
				} else {
					WayRecord way = (WayRecord) record.get();
					ways.add(way);
				}
			}
			
			// Second pass: create pairs
			for (WayRecord way : ways) {
				for (NodeRecord node : nodes) {
					int nodeIndex = way.getNodeIndex(node.getId());
					ResolvedWayNode resolved = new ResolvedWayNode(way, node, nodeIndex);
					context.write(resolved.getWay().getId(), resolved);
				}
			}
		}
	}

	public static class SecondMapper extends Mapper<WayId, ResolvedWayNode, WayId, ResolvedWayNode> {
		protected void map(WayId key, ResolvedWayNode resolvedWayNode, Mapper<WayId,ResolvedWayNode,WayId,ResolvedWayNode>.Context context)
				throws IOException, InterruptedException {
			// Nothing to do; bulk is done by the reducer.
			context.write(key, resolvedWayNode);
		};
	}

	public static class SecondReducer extends Reducer<WayId, ResolvedWayNode, WayId, ResolvedWay> {
		protected void reduce(WayId wayId, Iterable<ResolvedWayNode> resolvedWayNodes, Reducer<WayId,ResolvedWayNode,WayId,ResolvedWay>.Context context)
				throws IOException, InterruptedException {
			
			context.getCounter(Count.WAY_RECORDS).increment(1);
			ResolvedWay resolvedWay = new ResolvedWay(context.getConfiguration(), resolvedWayNodes);
			context.write(wayId, resolvedWay);
			
			int length = getWayLengthInMeters(resolvedWay);
			context.getCounter(Count.WAY_METERS).increment(length);
		}

		private int getWayLengthInMeters(ResolvedWay resolvedWay) {
			int length = 0;
			LatLng previousPoint = null;
			NodeRecord previousNode = null;
			for (Object nodeObj : resolvedWay.getNodes().get()) {
				NodeRecord node = (NodeRecord) nodeObj;
				LatLng point = new LatLng(node.getLat().get(), node.getLon().get());
				if (previousPoint != null) {
					double distance = LatLngTool.distance(previousPoint, point, LengthUnit.METER);
//					System.out.println("Way " + resolvedWay.getWay().getId() + " segment from " + previousNode.getId() + " " + previousPoint + " to " + node.getId() + " " + point + ": " + distance + " meters");
					length += distance;
				}
				previousNode = node;
				previousPoint = point;
			}
			return length;
		};
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // First job: resolve
        int resolverExitCode = resolveJob(new Path(args[0]), new Path(args[1] + "Tmp"), conf);
        if (resolverExitCode != 0) return resolverExitCode;
        
        // Second job: measure ways
        return measureJob(new Path(args[1] + "Tmp"), new Path(args[1]), conf);
	}

	/**
	 * First MapReduce job that resolves nodes referenced by ways.
	 */
	private int resolveJob(Path input, Path output, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        
        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(WayExplosionMapper.class);
		job.setReducerClass(WayNodeReducer.class);

		// The OpenStreetMap datasets are XML files with each way and node information enclosed within
		// the <node></node> and <way></way> tags
		job.setInputFormatClass(XMLInputFormat.class);
		XMLMultiRecordReader.setTags(job, "way,node");

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(NodeId.class);
		job.setMapOutputValueClass(WayNodeRecord.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(WayId.class);
		job.setOutputValueClass(ResolvedWayNode.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, input);

		// Preparing the output folder by first deleting it if it exists
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private int measureJob(Path input, Path output, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        
        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(SecondMapper.class);
		job.setReducerClass(SecondReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(WayId.class);
		job.setMapOutputValueClass(ResolvedWayNode.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(WayId.class);
		job.setOutputValueClass(ResolvedWay.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Setting the input folder of the job 
		SequenceFileInputFormat.addInputPath(job, input);

		// Preparing the output folder by first deleting it if it exists
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new WayLengthCounter(), args);
		System.exit(result);
	}

}
