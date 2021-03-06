/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;

import java.util.List;

/**
 * Synchronous read/write task.
 */
public class RWTaskSync extends RWTask {

	public RWTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(client, args, counters, keyStart, keyCount);	
	}

	protected void largeListUpdate(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeListUpdate(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeListUpdate(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}

	private void largeListUpdate(Key key, Value value, long timestamp) throws AerospikeException {
		// Update entry
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker");
		
		//System.out.println("LLIST CONFIG: *********** " + list.getConfig());
		//System.out.println("LLIST SIZE: ++++++++++++" + list.size());
		try {
			list.update(value);
		} catch (AerospikeException e) {
			System.out.println(e.getMessage());
		}
	}
	
	protected void largeListGet(Key key, int count) throws AerospikeException {
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker");
		List<?> results = null;
		
		/*long begin = System.currentTimeMillis();
		if (counters.read.latency != null) {
			try {
				results = list.range(Value.get(1000), Value.get(begin));
			} catch (AerospikeException e) {
				System.out.println(e.getMessage());
			}
			
			
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			try {
				results = list.range(Value.get(1000), Value.get(begin));
			} catch (AerospikeException e) {
				System.out.println(e.getMessage());
			}
		}*/
		results = list.findFirst(count);
		//System.out.println("LLIST CONFIG: *********** " + list.getConfig());
		processLargeRead(key, results);
	}
}