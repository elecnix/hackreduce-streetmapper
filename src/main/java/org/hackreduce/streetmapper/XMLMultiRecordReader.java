/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hackreduce.streetmapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hackreduce.mappers.XMLRecordReader;

public class XMLMultiRecordReader extends XMLRecordReader {

	public static final String TAGS = "hadoop.mapred.xmlrecordreader.tag";
	private byte[][] cpats;//byte check of tags 
	private int matchingMark;
	private boolean[] mayMarks;
	
	public class Mark {

		String startMarker;
		String endMarker;

		public Mark(String startMarker, String endMarker) {
			this.startMarker = startMarker;
			this.endMarker = endMarker;
		}

	}

	private ArrayList<Mark> marks = new ArrayList<Mark>();

	public void addMark(Mark mark) {
		marks.add(mark);
	}

	public void addMark(String tagname) {
		marks.add(new Mark("<" + tagname + ">", "</" + tagname + ">"));
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		super.initialize(split, context);

		Configuration conf = context.getConfiguration();
		String tags = conf.get(TAGS);
		for (String tag : tags.split(",")) {
			addMark(tag);
		}

		//setup for fastReadUntilMatch
		//1. save all tags' byte value into cpats
		cpats = new byte[marks.size()*2][];//start and end tag
		for (int i = 0; i < marks.size()*2; i++) {
			String marker = ((i^1)==0) ? marks.get(i).startMarker : marks.get(i).endMarker;
			cpats[i] = marker.getBytes("UTF-8");
		}

		mayMarks = new boolean[marks.size()];//start tag only 
		Arrays.fill(mayMarks, true);
		matchingMark = -1;

	}

	@Override
	protected boolean readUntilMatchBegin() throws IOException {
		return fastReadUntilMatch(true, null);
	}

	@Override
	protected boolean readUntilMatchEnd(DataOutputBuffer buf) throws IOException {
		return fastReadUntilMatch(false, buf);
	}

	protected boolean fastReadUntilMatch(boolean matchBeginMark, DataOutputBuffer outBufOrNull) throws IOException {

		boolean cMatch=false;
		boolean match = false;
		int LL = 120000 * 10;
		int m=0;


		_bufferedInputStream.mark(LL); // large number to invalidate mark
		while (true) {
			int b = _bufferedInputStream.read();
			if (b == -1) break;

			byte c = (byte) b; // this assumes eight-bit matching. OK with UTF-8

			cMatch = false;
			if(matchingMark==-1){
				for (int k = 0; k < mayMarks.length; k++) {//looping only for start tag
					if (mayMarks[k]) {
						if (c == cpats[k*2][m]|| (cpats[k*2][m] == '>' && c == ' ')) {
							m++;
							cMatch = true;
							if (m == cpats[k*2].length) {
								match = true;
								matchingMark = k*2;
								Arrays.fill(mayMarks, true);//for next round
								break;
							}
						} else {
							mayMarks[k] = false;
						}
					}
				}
			}//end LOOPING to search for start tag
			else{
				if(c==cpats[matchingMark+1][m]){//end tag
						m++;
						cMatch=true;
						if(m==cpats[matchingMark+1].length){
							match=true;
							matchingMark=-1;
							break;
						}
				}
			}//end searching for end tag
			
			if (!cMatch) {
				_bufferedInputStream.mark(LL); // rest mark so we could jump back if we found a match
				if (outBufOrNull != null) {
					// we're looking for the end tag
					outBufOrNull.write(cpats[matchingMark], 0, m);
					outBufOrNull.write(c);
					_pos += m + 1;
				} else {
					// we're looking for the start tag
					_pos += m + 1;
					if (_pos >= _end) {
						break;
					}
				}
				m = 0;
			}
		}
		if (matchBeginMark && match) {
			_bufferedInputStream.reset();
		} else if (outBufOrNull != null) {
			outBufOrNull.write(cpats[matchingMark]);
			_pos += cpats[matchingMark].length;
		}
		return match;
	}

	public static void setTags(Job job, String taglist) {
		job.getConfiguration().set(TAGS, taglist);
	}

}
