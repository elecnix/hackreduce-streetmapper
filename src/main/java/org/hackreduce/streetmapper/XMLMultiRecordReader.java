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
	}

	@Override
    protected boolean readUntilMatchBegin() throws IOException {
        return fastReadUntilMatch(false, null);
    }

	@Override
    protected boolean readUntilMatchEnd(DataOutputBuffer buf) throws IOException {
      return fastReadUntilMatch(true, buf);
    }

	protected boolean fastReadUntilMatch(boolean isEnd, DataOutputBuffer outBufOrNull) throws IOException {
		byte[][] cpats = new byte[marks.size()][];
		for (int i = 0; i < marks.size(); i++) {
			String marker = isEnd ? marks.get(i).endMarker : marks.get(i).startMarker;
			cpats[i] = marker.getBytes("UTF-8");
		}
		
		
		int m = 0;
		boolean match = false;
		int msup = cpats[0].length;
		int LL = 120000 * 10;

		_bufferedInputStream.mark(LL); // large number to invalidate mark
		while (true) {
			int b = _bufferedInputStream.read();
			if (b == -1) break;

			byte c = (byte) b; // this assumes eight-bit matching. OK with UTF-8
			if (c == cpats[0][m]) {
				m++;
				if (m == msup) {
					match = true;
					break;
				}
			} else {
				_bufferedInputStream.mark(LL); // rest mark so we could jump back if we found a match
				if (outBufOrNull != null) {
					outBufOrNull.write(cpats[0], 0, m);
					outBufOrNull.write(c);
					_pos += m + 1;
				} else {
					_pos += m + 1;
					if (_pos >= _end) {
						break;
					}
				}
				m = 0;
			}
		}
		if (!isEnd && match) {
			_bufferedInputStream.reset();
		} else if (outBufOrNull != null) {
			outBufOrNull.write(cpats[0]);
			_pos += msup;
		}
		return match;
	}

	public static void setTags(Job job, String taglist) {
    	job.getConfiguration().set(TAGS, taglist);
    }

}
