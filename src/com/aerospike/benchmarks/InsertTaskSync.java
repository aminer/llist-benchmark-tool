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

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;


public final class InsertTaskSync extends InsertTask {

	private final AerospikeClient client; 

	public InsertTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
	}

	protected void largeListAdd(Key key, Bin[] value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeListAdd(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeListAdd(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}

	private void largeListAdd(Key key, Bin[] values, long timestamp) throws AerospikeException {
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker");
		
		try {
			for (int i = 0; i < args.itemCount; i++) {
				if (DBObjectSpec.type == 'M') {
					Map<String, Value> entry = new HashMap<String, Value>();
					entry.put("key", Value.get(keyStart + i));
					
					for (int j = 0; j < DBObjectSpec.mapValCount; j++) {
						entry.put("value" + j, values[i].value);
					}
					//System.out.println("***********" + entry.size());
					list.add(Value.get(entry));
				}
				else {
					list.add(values[i].value);
				}
			}
			
		} catch (AerospikeException e) {
			System.out.println(e.getMessage());
		}
		
		list.setPageSize(args.pageSize);
		//System.out.println("LLIST CONFIG: " + list.size() + "*********** " + list.getConfig());
		//System.out.println("LLIST CONFIG: *********** " + list.getConfig());
	}
}