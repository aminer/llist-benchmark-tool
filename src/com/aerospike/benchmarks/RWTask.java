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
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

/**
 * Random Read/Write workload task.
 */
public abstract class RWTask implements Runnable {

	final AerospikeClient client;
	final Arguments args;
	final CounterStore counters;
	final RandomShift random;
	final WritePolicy writePolicyGeneration;
	ExpectedValue[] expectedValues;
	final int keyStart;
	final int keyCount;
	volatile boolean valid;
	
	public RWTask(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		this.client = client;
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
		this.valid = true;
		
		random = new RandomShift();
				
		writePolicyGeneration = new WritePolicy();
		writePolicyGeneration.timeout = args.writePolicy.timeout;
		writePolicyGeneration.maxRetries = args.writePolicy.maxRetries;
		writePolicyGeneration.sleepBetweenRetries = args.writePolicy.sleepBetweenRetries;
		writePolicyGeneration.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;		
	}	

	public void run() {
		while (valid) {
			try {
				readUpdate();
			} 
			catch (Exception e) {
				if (args.debug) {
					e.printStackTrace();
				}
				else {
					System.out.println("Exception - " + e.toString());
				}
			}		 

			// Throttle throughput
			if (args.throughput > 0) {
				int transactions = counters.write.count.get() + counters.read.count.get();
				
				if (transactions > args.throughput) {
					long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();                                        

					if (millis > 0) {
						Util.sleep(millis);
					}
				}
			}
		}
	}
	
	public void stop() {
		valid = false;
	}
	
	private void readUpdate() {
		int key = random.nextInt(keyCount + args.itemCount);
		if (random.nextInt(100) < args.readPct) {
			boolean isMultiBin = random.nextInt(100) < args.readMultiBinPct;
			doRead(key, isMultiBin);
		}
		else {
			boolean isMultiBin = random.nextInt(100) < args.writeMultiBinPct;
			// Single record write.
			doWrite(key, isMultiBin);
		}		
	}
	
	/**
	 * Write the key at the given index
	 */
	protected void doWrite(int keyIdx, boolean multiBin) {
		Key key;
		Bin[] bins = args.getBins(random, multiBin);
		key = new Key(args.namespace, args.setName, keyStart + keyIdx);
		
		try {
			if (args.updateOne) {
				if (DBObjectSpec.type == 'M') {
					// Update 1 item picked randomly.
					// Create entry
					Map<String, Value> entry = new HashMap<String, Value>();
					entry.put("key", Value.get(random.nextInt(keyCount + args.itemCount)));
					
					for (int i = 0; i < DBObjectSpec.mapValCount; i++) {
						bins = args.getBins(random, multiBin);
						entry.put("value" + i, bins[random.nextInt(args.itemCount)].value);
					}
		        	
		        	System.out.println("******* Item " + " Inserting: " + Value.get(entry));
		        	largeListUpdate(key, Value.get(entry)); 
				}
				else {
					largeListUpdate(key, bins[random.nextInt(args.itemCount)].value); 
				}
			}
			else {
				// Pick '%' items and update each one.
				for (int i = 0; i < (int) Math.ceil((args.itemCount * args.updatePct) / 100.0); i++) {
					if (DBObjectSpec.type == 'M') {
						// Update 1 item picked randomly.
						// Create entry
						Map<String, Value> entry = new HashMap<String, Value>();
						entry.put("key", Value.get(random.nextInt(keyCount + args.itemCount)));
						
						for (int j = 0; j < DBObjectSpec.mapValCount; j++) {
							bins = args.getBins(random, multiBin);
							entry.put("value" + j, bins[random.nextInt(args.itemCount)].value);
						}
			        	
			        	System.out.println("******* Item " + " Inserting: " + Value.get(entry));
			        	largeListUpdate(key, Value.get(entry)); 
					}
					else {
						bins = args.getBins(random, multiBin);
						largeListUpdate(key, bins[random.nextInt(args.itemCount)].value); 
					}
				}
			}
		}
		catch (AerospikeException ae) {
			writeFailure(ae);
		}	
		catch (Exception e) {
			writeFailure(e);
		}
	}
		
	/**
	 * Read the key at the given index.
	 */
	protected void doRead(int keyIdx, boolean multiBin) {
		try {
			Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);
			largeListGet(key);
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}
		catch (Exception e) {
			readFailure(e);
		}
	}

	protected void processLargeRead(Key key, List<?> list) {
		if (list == null || list.size() == 0) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
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
	
	protected void readFailure(AerospikeException ae) {
		if (ae.getResultCode() == ResultCode.TIMEOUT) {		
			counters.read.timeouts.getAndIncrement();
		}
		else {			
			counters.read.errors.getAndIncrement();
			
			if (args.debug) {
				ae.printStackTrace();
			}
		}
	}

	protected void readFailure(Exception e) {
		counters.read.errors.getAndIncrement();
		
		if (args.debug) {
			e.printStackTrace();
		}
	}

	protected abstract void largeListUpdate(Key key, Value value) throws AerospikeException;
	protected abstract void largeListGet(Key key) throws AerospikeException;
}