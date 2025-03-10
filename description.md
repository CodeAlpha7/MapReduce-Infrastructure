This project implements a simplified version of MapReduce influenced by the original [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) paper.


### About MapReduce

MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines. The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication.

### Learning outcomes
  - MapReduce Infrastructure

### What's simplified here (comparing from the original paper)
1. **`The MapReduce library in the user program first splits the input files into M pieces of typically 16 megabytes to 64 megabytes (MB) per piece (controllable by the user via an optional parameter). It then starts up many copies of the program on a cluster of machines.`**
 - Instead of writing new split files, we just store the offsets from the original file as the file shards.
 - **Sharding**: The configuration file contains a variable `map_kilobytes` which specifies the approximate shard size, and will be used to calculate the number of shards `M`. For example, if `map_kilobytes` in the config file is 16 and we are given 3 input files of sizes 20KB, 15KB, 20KB then our **calculated** number of shards `M` should be `ceil(55/16) = 4KB`.

    - So in the example above, we will use 4 mappers. The first 3 of these mappers will process approximately 16 KB of data each, and the last mapper will process all remaining data. So, in general the first M-1 mappers would process approximately `map_kilobytes`, and the last mapper would process the remaining.
    - Note that each mapper processes an approximate amount of data because we need to split input to the closest next line (i.e. `\n`) for the first M-1 mappers. This is because lines consist of different strings of words and thus cannot have a homogeneous length. Then, in the above example the shards will approximately look like:
         - shard 1 - ((file: file1, offsets: 0-16kb))
         - shard 2 - ((file: file1, offsets: 16-20kb), (file: file2, offsets: 0-12kb))
         - shard 3 - ((file: file2, offsets: 12-15kb), (file: file3, offsets: 0-13kb))
         - shard 4 - ((file: file3, offsets: 13-20kb))

    - Further, we want your input shards to have complete record entries, for example, when our framework is running a word count program, then you should not be splitting in the middle of a word. For this reason and to make it simple, you should align your shard on '\n', i.e. new line which is roughly closest to the shard's end offset, calculated by above logic. We are free to choose to bring our end offset to the previous '\n' or the next '\n'.
  - Instead of the map reduce library starting worker processes, we will be manually starting them up before running the main binary.
  - Instead of running worker programs on different machines, we will start our development on a single machine by starting multiple instances of our worker program as processes on the same machine listening to different ports. 


2. **`One of the copies of the program is special – the master. The rest are workers that are assigned work by the master. There are M map tasks and R reduce tasks to assign. The master picks idle workers and assigns each one a map task or a reduce task.`**
- In a normal MapReduce setup, the master would run as a separate process. However, in this project the master is going to be part of the main function and will not run as an independent process. Instead, after you create the mapreduce object instance and call the run function, the master will be spawned automatically and start assigning tasks to the workers (i.e., this will be managed for you automatically in the provided code).
  - Once we have M input shards created, our master is supposed to assign each shard to one of the available workers.
 - Your master can read the worker process addresses (ip:port) from the MapReduce specification structure provided by the framework.
 - Your master will essentially be maintaining a worker pool, taking care of various things such as tracking how many workers are there in the system, what is the state of each worker: `AVAILABLE, BUSY(doing map task/doing reduce task, etc.)`, when to assign what task to a worker, knowing when a worker is done.
 - `The communication of relevant instructions/data/results to/from the workers will be done through GRPC calls`. For example, the master can inform a mapper worker about the shard that it will be processing by passing the shard info in a grpc message. Who will be a client and who will be the server? What kind of message signature you will use? You will need to define your own proto files and master-worker (GRPC client-server) interfaces here.
  - Our master is also given the number R, i.e., number of output files from the config. For simplicity, we can `start our reduce phase when ALL of the map tasks are done`.
 
 
3. **`A worker who is assigned a map task reads the contents of the corresponding input split. It parses records out of the input data and passes each record to the user-defined Map function. The intermediate key/value pairs produced by the Map function are buffered in memory.`**
 - Once a worker is assigned a 'map' task by the master, how should it process the input shard assigned to it? 
 - Your application logic (Word count - counting the number of occurences for each word in the input, or finding mean-max temperature for each month from the given record of temperatures, etc.) should be written in BaseMapper's implementation(UserMapper: see the provided code for more details).
 - You should call 'map' from the user's mapper (UserMapper's map()) on each record one-by-one. We will assume that the records are separated by '\n'.
 - What is the output of map function? A `key, value` pair. 
 - How will you pass these key-value pairs to the reducers? You should create intermediate files on the disk.
 - Some useful questions to think about: 
   - How many intermediate files should you create per mapper worker? Why should this matter? 
   - How will you decide which `key, value` pair is written to which intermediate file? Do you need to hash the keys?


4. **`Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function. The locations of these buffered pairs on the local disk are passed back to the master, who is responsible for forwarding these locations to the reduce workers.`**
 - Once the mappers have written the output `key, value` pairs to the intermediate files, they need to pass the following information back to the master: `Indicate that the map task is done`, `Where the intermediate files are located`.
 - Your master is then supposed to start the reducer workers and then assign the respective intermediate files to each of the reducer worker. 
 - A useful question to think about: 
  - Which files should get assigned to which reducer?
 
  
5. **`When a reduce worker is notified by the master about these locations, it uses remote procedure calls to read the buffered data from the local disks of the map workers. When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together. The sorting is needed because typically many different keys map to the same reduce task.`**
 - Theoretically, in real distributed environment, the intermediate files lie on local disks of mapper workers and reducers need to make remote file reads to get the data into their own local memory. However, for simplicity, you can have the intermediate files on the same file system as of your reducer worker, and hence you can read them through local file read system calls.
 - We take user's reducer logic (UserReducer's reduce()) in consideration, similar to how you handled user's mapper logic/algorithm.
 - We have one more constraint on the final output though. We make sure that the final output is sorted on its keys.

6. **`The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user’s Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition.`**
 - This would be done the same way in our implementation. We make sure each reducer has its own file (create in the user given output directory) to which it appends its results.
 **The output of the reducer must be of the format:
 key< space >value\n  
 i.e., we will use a single space as a delimiter between the key and the value.**  

7. **`When all map tasks and reduce tasks have been completed, the master wakes up the user program. At this point, the MapReduce call in the user program returns back to the user code.`**
 - This would be done the same way in our implementation. Waking up is simply the return from the function call.

### Table of Contents:

  ```
      README.md - Contains project description and changes made to simplify the MapReduce Architecture.
      src/
      |- CMakeLists.txt           
      |- GenerateProtos.cmake       
      |                       
      |- masterworker.proto - containing the grpc specification between master and worker.
      |- master.h           - containing the source code for master management.\
      |                       Note that you can add optional supporting files for master if you want.
      |- worker.h           - containing the source code for worker management.\
      |                       Note that you can add optional supporting files for worker if you want.
      |- mr_tasks.h         - containing the source code for Mapper/Reducer Internal implementation. \
      |                       All your data structure and logic for internal mapper/reducer \
      |                       should be in this file only, no supporting files.
      |- file_shard.h       - containing the source code for file splitting logic.\
      |                       All your data structure and logic for file sharding \
      |                       should be in this file only, no supporting files.
      |- mapreduce_spec.h   - containing the source code for map reduce specification.\
      |                       All your data structure and logic for map reduce specification \
      |                       should be in this file only, no supporting files.
      |- Other unmodified files already present in the src

  ```
