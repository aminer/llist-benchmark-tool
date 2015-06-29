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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.util.Util;

public abstract class InsertTask implements Runnable {

	final Arguments args;
	final int keyStart;
	final int keyCount;
	final CounterStore counters;
	
	public InsertTask(Arguments args, CounterStore counters, int keyStart, int keyCount) {
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
	}

	public void run() {
		try {			
			RandomShift random = new RandomShift();
			Bin[] bins;
			for (int i = 0; i < keyCount; i++) {
				for (int j = 0; j < args.itemCount; j++) {
					try {
						bins = args.getBins(random, true);
						
						
						//System.out.println("type: " + DBObjectSpec.type);
						//System.out.println("size " + objarr[1].substring(0, objarr[1].length() - 1));
						//System.out.println("type " + objarr[1].charAt(objarr[1].length() - 1));
				
						Key key = new Key(args.namespace, args.setName, keyStart + i);
						
						if (DBObjectSpec.type == 'M') {
							// Add entry
							Map<String, Value> entry = new HashMap<String, Value>();
							entry.put("key", bins[j].value);
				        	entry.put("value", bins[j].value);
				        	System.out.println("******* Item " + j + " Inserting: " + Value.get(entry));
				        	largeListAdd(key, Value.get(entry)); 
						}
						else {
							largeListAdd(key, bins[j].value); 
						}
					}
					catch (AerospikeException ae) {
						writeFailure(ae);
					}	
					catch (Exception e) {
						writeFailure(e);
					}
					
					// Throttle throughput
					if (args.throughput > 0) {
						int transactions = counters.write.count.get();
						
						if (transactions > args.throughput) {
							long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();                                        

							if (millis > 0) {
								Util.sleep(millis);
							}
						}
					}
				}
			}
		}
		catch (Exception ex) {
			System.out.println("Insert task error: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	protected void writeFailure(AerospikeException ae) {
		if (ae.getResultCode() == ResultCode.TIMEOUT) {		
			counters.write.timeouts.getAndIncrement();
		}
		else {			
			counters.write.errors.getAndIncrement();
			
			if (args.debug) {
				ae.printStackTrace();
			}
		}
	}

	protected void writeFailure(Exception e) {
		counters.write.errors.getAndIncrement();
		
		if (args.debug) {
			e.printStackTrace();
		}
	}
	
	protected abstract void largeListAdd(Key key, Value value) throws AerospikeException;
}