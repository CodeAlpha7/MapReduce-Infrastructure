# CS 6210 Project4 - MapReduce Infrastructure

### How to setup the project  

Setup grpc similar to project 3.
Clone this repo

In home directory, run the following commands:
```
cmake .
make
```
this will set all required binaries

Next, copy input data to ```bin/``` and run all 6 workers are following:
```
./mr_worker localhost:50051 & ./mr_worker localhost:50052 & ./mr_worker localhost:50053 & ./mr_worker localhost:50054 & ./mr_worker localhost:50055 & ./mr_worker localhost:50056 &
```
Running this will set them in the background. Hit enter and move to the next command in the same terminal:
```
./mrdemo config.ini
```
This will successfully run the mapreduce task on provided input files and paste all output to ```output/``` directory. An intermediate directory is also created with stores all intermediate files.

Now, kill all workers using the ```fg``` command which will each worker to the foreground, which can then be killed using ```Ctrl+C```


## File structure
1. intermediate
2. output

## Files Description and the logic

### file_shard.h

1. We use fstream library offered by C++ to perform sharding.
2. DataStructures used:
  ```
  struct info {
    string filename;
    int start_bytes;	// exclusive
    int end_bytes;   // inclusive
  };
  
  struct FileShard {
	int shard_id;
	vector<info> file_info_list;
};
  ```
  info: This datastructure is used to store information of a single file in a shard. It has "filename", "start_bytes" and "end_bytes". Start bytes is not included in the shard, where as end bytes is inclusive.
  
  FileShard: This datastructure has list of info used for storing information of each shard. 
  shard_id : used for tracking number of shards
  file_info_list: has list of files, start_bytes, end_bytes in the file.
  
#### Logic

1. Loop through each input file.
2. Keep updating "info" with start and end bytes values.
3. Also, we check how much more data is present in a file everytime, and compare it with data needed to fill the shard, and accordingly update the "FileShard".
4. Then go until '\n' so that we are not breaking any characters.
5. Repeat above for all files.
6. Put remaining info into a shard even if thats less than "map_kilobytes" as it would be the final shard.

Corner cases:

1. We check until "needed_bytes - 1" for every file, as last character can be \n. Whenever we do tellg, and seekg for that character, we get the next character. So, we go one character less. Then check the last character.
```
   file.seekg(start + num_bytes_needed_from_curr_file - 1);

   // check if we are stopping at last char as \n in the shard
   // so that we are good to go
   file.get(t);
   if(t =='\n'){
   cout << "stopped at the \n" << endl;
   }
```

### mr_tasks.h

**Mapper:**

Mapper has few methods added to interact with worker.h and return responses accordingly. Here we add methods to get and set intermediate file names, pass number of output files to get the hash.

Each mapper has intermediate files equal to number of output files.

Hash makes sure that a key goes into a correct intermediate file. So that a specific key always goes to a specific intermediate file. So that for all mappers, a key always lies in a specific file, and each reducer picks its corresponding file only.

**Reducer:**

Reducer clss gets the list of intermediate files it has to operate from worker.h. STores all the keys and and a vector of values in an ordered map, which handles sorting keys in asceding order.


### mr_spec.h

This file takes care of parsing the config file. We use fstream and split each line at delimiter "=" and write the code to map all the configurations. Also, we check length and type of each value stored in mrspec in terms of numerical and strings. 


### worker.h
A worker can be assigned either a map or reduce task. When assigned map, the map() is called on each record one by one, then it reads the shard data, in our implementation the starting and ending offsets, parses and passes input data to the user-defined map function. Once map task is completed, intermediate key/value pairs are produced and buffered in memory for the reduce tasks to pick up. On an overall level, the following steps take place:
1. Map task assigned to worker - produces intermediate file having key/value pairs stored on disk
2. The local disk is partitioned into R regions by the partitioning function.
3. Locations of these intermediate KV pairs are passed to master, used for assigning reduce tasks.
4. Master assigns reduce task to worker with location for intermediate files
5. Reducer worker uses RPC calls to read intermediate KV pairs from local disk.
6. Once all intermediate data is accumulated, it sorts it by using intermediate keys so that all occurences of the same key are grouped together.
7. Reducer worker iterates over sorted intermediate data and passes each unique intermediate key and its value to the reduce function.
8. Output of reduce function is appended to final output file.
9. When all map and reduce tasks are completed, master wakes up the user program and it returns back control.

In our implementation, the CallData class handles state and logic needed to handle a request. We implement an asynchronous server for async communication with grpc.
Once we initialize all the workers by setting status as PROCESS, move to assigning it a task. Initially, we obviously only to map tasks.

```
if(request_.worker_type()=="map"){
     std::vector<std::string> intermediate_files = this->map_worker(request_);
```
If task is a map task, we invoke the map_worker() function. This creates intermediate files, reads shards line by line from starting offset to ending offset and writes to those files. Each worker receives from the master, one shard which may contain data from multiple files. It breaks down the shard wrt each line. During the time the keys are buffered in memory, the word count is aggregated to produce a word count. This continues until all files are mapped, which is when everything can be dumped to the intermediate files.
```
std::vector<std::string> map_worker(WorkRequest request){
	for (auto out_file:request.file_names()){
				out_files.push_back(out_file);
				cout << out_file << endl;
			}
	auto my_mapper = get_mapper_from_task_factory(request.user_id());
	my_mapper->impl_->file_name_setter(out_files);
	my_mapper->impl_->set_intermediate_files_count(request.num_reducers());

	std::string line;

	for (auto info: request.file_infos()){
		std::string file_name = info.file_name();
		int start_bytes = info.start_bytes();
		int end_bytes = info.end_bytes();
		int num_intermediate_files = request.num_reducers();
		std::ifstream file(file_name, std::ios::in);

		file.seekg(start_bytes);
		while(file.tellg()<=end_bytes && !file.eof() && file.tellg()!=-1){
			getline(file, line);
			my_mapper->map(line);
		}
		file.close();
		}
		return my_mapper->impl_->get_intermediate_files();
	}

```

Similarly, for reduce task, we read from intermediate files, push keys and values in a map where the keys are strings and values are a vector of strings. Finally, sort the final file.
```
std::string key;
std::string value;
int pos = line.find(",");
key = line.substr(0, pos);
value = line.substr(pos+2);
if (key_to_value_map.find(key) == key_to_value_map.end()) {
	std::vector<std::string>values;
	values.push_back(value);
	key_to_value_map[key]=values;
}
else {
	key_to_value_map[key].push_back(value);
}
}
file.close();
```

Once calldata is finished, we need to let the grpc runtime know. We do this using the memory address of this instance as the uniquely identifying tag for the event. Once status is set to FINISH, we deallocate calldata.

The worker.h file also contains functions to handle async rpcs whenever a new data is called.
hash in emit appends the file number in the end


### master.h
The master program is a special program among several other workers, which are assigned work by the master. The job of the master is to identify idle workers by maintaining a worker pool and assign them either map or reduce tasks.
So, once M shards have been created, the master assigns one shard to each available worker. Finally, when all map tasks are done, the reduce phase starts.
The communication between the master and workers is handled through GRPC calls. 

Here, we use the same async grpc calls we used for project 3.
In this program, we have 3 main tasks:
1. Identify and manage idle workers
2. Assign map tasks
3. Assign reduce tasks

Once we initialize the master, we setup rpc and maintain a worker queue.
We then handle creation of directories - output and intermediate - along with checks to see if they exists, empty them if they do, and if not create new ones and set appropriate permissions.
When assigning map tasks, we form a request by specifying the output directory, userid, the shard start and end offsets. On we have this, we identify idle workers and assign tasks. Similarly, for reduce work assignment.

Master is responsible for all bookkeeping operations.
The number of intermediate files is determined by the number of reducer workers, where both are equal and this info is also sent to map workers for file creation and naming. The latter is decided by the user id and index of the reducer task and created by the map worker.


## Worker failure handling
Once tasks are assigned to users, we set a timeout deadline of 120 ms after which tasks are reassigned by calling the map_tasks() function. This deadline time is synched with the systems internal clock to avoid any drifts due to program slow downs. So, once deadline hits, we reassign job to free worker and continue doing so until all jobs are complete.


  
  
  
  

