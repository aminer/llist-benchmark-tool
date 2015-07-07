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

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Arguments {
	public String namespace;
	public String setName;
	public Workload workload;
	public DBObjectSpec objectSpec;
	public Policy readPolicy;
	public WritePolicy writePolicy;
	public int readPct;
	public int readMultiBinPct;
	public int writeMultiBinPct;
	public int throughput;
	public long transactionLimit;
	public boolean debug;
	public Bin[] fixedBins;
	public Bin[] fixedBin;
	public int pageSize;
	public int itemCount;
	public int itemSize;
	public int updatePct;
	public boolean updateOne;

	public void setFixedBins() {
		// Fixed values are used when the extra random call overhead is not wanted
		// in the benchmark measurement.
		RandomShift random = new RandomShift();
		fixedBins = getBins(random, true);
		fixedBin = new Bin[] {fixedBins[0]};
	}

	public Bin[] getBins(RandomShift random, boolean multiBin) {
		//System.out.println("HERE::::");
		if (fixedBins != null) {
		    return (multiBin) ? fixedBins : fixedBin;
		}
		
		int binCount = (multiBin) ? itemCount : 1;
		Bin[] bins = new Bin[binCount];
		
		for (int i = 0; i < binCount; i++) {
			String name = Integer.toString(i);
			Value value = genValue(random, objectSpec);
			bins[i] = new Bin(name, value);
		}
		return bins;
	}
    
	private static Value genValue(RandomShift random, DBObjectSpec spec) {
		StringBuilder sb;
		switch (DBObjectSpec.type) {
		case 'I':
			return Value.get(random.nextInt());
			
		case 'S':
			sb = new StringBuilder(DBObjectSpec.size);
            for (int i = 0; i < DBObjectSpec.size; i++) {
            	// Append ASCII value between ordinal 33 and 127.
                sb.append((char)(random.nextInt(94) + 33));
            }
            System.out.println("yyyy Returning: "+ Value.get(sb.toString()));
			return Value.get(sb.toString());
		case 'M':
			switch (DBObjectSpec.mapDataType) {
			case 'I':
				return Value.get(random.nextInt());
				
			case 'S':
				sb = new StringBuilder(DBObjectSpec.mapDataSize);
				for (int j = 0; j < DBObjectSpec.mapDataSize; j++) {
					// Append ASCII value between ordinal 33 and 127.
		            sb.append((char)(random.nextInt(94) + 33));
				}
				return Value.get(sb.toString());

			default:
				return Value.getAsNull();
			}
			
		default:
			return Value.getAsNull();
		}
	}	
}