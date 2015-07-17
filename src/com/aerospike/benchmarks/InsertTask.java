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
				/*if (i % 100 == 0) {
					System.out.println("insert task i = " + i);
				}*/
				
				
				//for (int j = 0; j < args.itemCount; j++) {
					try {
						//System.out.println("type: " + DBObjectSpec.type);
						//System.out.println("size " + objarr[1].substring(0, objarr[1].length() - 1));
						//System.out.println("type " + objarr[1].charAt(objarr[1].length() - 1));
				
						Key key = new Key(args.namespace, args.setName, keyStart + i);
						
						// Add a fixed number of values (using DBObjectSpec.mapValCount).
						if (DBObjectSpec.type == 'M') {
							// Add entry
							bins = args.getLDTBins(random, true);
							
							
							Map<String, Value> entry = new HashMap<String, Value>();
							entry.put("key", Value.get(keyStart + i));
							//entry.put("value" + i, bins[j].value);
							
							/*for (int k = 0; k < DBObjectSpec.mapValCount; k++) {
								bins = args.getLDTBins(random, true);
								entry.put("value" + k, bins[j].value);
							}*/
				        	
				        	//System.out.println("******* Item " + j + " Inserting: " + Value.get(entry));
				        	largeListAdd(key, bins); 
						}
						else {
							bins = args.getLDTBins(random, true);
							//System.out.println("******* Item " + j + " Inserting: " + bins[j].value);
							largeListAdd(key, bins); 
							//System.out.println("+++++++++++Key:" + (keyStart + i));
						}
					}
					catch (AerospikeException ae) {
						writeFailure(ae);
					}	
					catch (Exception e) {
						writeFailure(e);
					}
					
					// Throttle throughput.
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
			//}
			//System.out.println("Thread finished");
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
	
	//protected abstract void largeListAdd(Key key, Value value) throws AerospikeException;
	protected abstract void largeListAdd(Key key, Bin[] value) throws AerospikeException;
}