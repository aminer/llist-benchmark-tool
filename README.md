Aerospike Large Ordered List Benchmark Tool
================================

This project contains the files necessary to build the benchmark tool. 
This program is used to insert data and generate load. 
The source code can be imported into your IDE and/or built using Maven.

    mvn package

The command line usage can be obtained by:

    ./run_benchmarks -u

Some sample arguments are:

    # Connect to localhost:3000 using test namespace.
    # Read 10% and write 90% of the time using 20 concurrent threads.
    # Use 100000000 integer keys (starting at "1") and 50 character string values.
    # Set the page size to 4KiB.
    ./run_benchmarks -h 127.0.0.1 -p 3000 -n test -k 100000000 -S 1 -o S:50 -PS 4096 -w RU,10 -z 20

    # Connect to localhost:3000 using test namespace.
    # Read 80% and write 20% of the time using 8 concurrent threads.
    # Use 10000000 integer keys and 1400 bytes values using a single bin.
    # Set item count to 100.
    # Timeout after 50ms for reads and writes.
    # Restrict transactions/second to 2500.
    ./run_benchmarks -h 127.0.0.1 -p 3000 -n test -k 10000000 -b 1 -o B:1400 -IC 100 -w RU,80 -g 2500 -T 50 -z 8