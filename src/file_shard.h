#pragma once

# include <math.h>
# include <vector>
# include <iostream>
# include <string>

using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::ifstream;
using std::min;

#include "mapreduce_spec.h"

struct info {
	string filename;
	int start_bytes;	// exclusive
	int end_bytes;		// inclusive
};


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	int shard_id;
	vector<info> file_info_list;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {

     vector<string> input_files = mr_spec.input_files;
     int map_kilobytes = mr_spec.map_kilobytes;  
     int map_bytes = map_kilobytes*1000;
	
	// to store first shard
	FileShard shard;
	
	// to store first fileinfo
     info curr_file_data;

     int curr_shard_bytes = 0;
     int shard_id = 0;
     int bytes_left_in_shard = map_bytes;
     int num_bytes_needed_from_curr_file;
     int bytes_left_in_file;
    
     // variables to store start and end bytes for current file
     // start is not included
     // end is included
     int start;
     int end;
	
     // mainly to track \n after we get the required bytes from a file
     char t;

     for(int i =0; i<input_files.size(); i++) {
          
          string filename = input_files[i];
          // cout << filename << endl;
                    
          ifstream file(filename);
                    
          // get the size of current file
          file.seekg(0, file.end);
          int curr_file_size = file.tellg();  
          file.seekg(0, file.beg);
               
          // loop until we have some bytes left in current file
          bytes_left_in_file=curr_file_size;
               
          while(bytes_left_in_file>0)
          {
               if(curr_shard_bytes==0) shard.file_info_list.clear();
                    
               // get the start, which is end of prev shard or beginning of current file
               start = file.tellg();
                    
               // get the number of bytes left in current shard
               bytes_left_in_shard = map_bytes - curr_shard_bytes;
                    
               // get the bytes needed from current file
               num_bytes_needed_from_curr_file = min(bytes_left_in_shard, bytes_left_in_file); 
               
               
               file.seekg(start + num_bytes_needed_from_curr_file - 1);

               // check if we are stopping at last char as \n in the shard
               // so that we are good to go
               file.get(t);
               if(t =='\n'){
                    cout << "stopped at the \n" << endl;
               }
                    
               // if we still have some characters left in file, go until end of the line
               if(num_bytes_needed_from_curr_file != bytes_left_in_file && t!='\n'){
                    
                    // do
                    file.get(t);
                         
                    // to know if we reached end of the file
                    int k = file.tellg();
                         
                    // while
                    while(t!='\n' &&  k<curr_file_size)
                    {
                         k++;
                         file.get(t);
                    }
                    
                    // if we reached end of the file seek to the end
                    if (file.eof()){
                         file.seekg(0, file.end);
                    }
                         
               }

               end = file.tellg();
               curr_file_data.filename = filename;
               curr_file_data.start_bytes = start;
               curr_file_data.end_bytes = end;
               shard.file_info_list.push_back(curr_file_data);
                    
               cout << "shard id " << shard_id << endl;
               cout << "file number " << i << endl;
               cout << "start " << start << endl;
               cout << "end " << end << endl;
               cout << endl ;
          
               curr_shard_bytes += end - start;
               bytes_left_in_file -= (end - start);
               
               info curr_file_data;
               
               if(curr_shard_bytes >= map_bytes){
                    shard.shard_id = shard_id;
                    fileShards.push_back(shard);
                    FileShard shard;
                    shard_id++;
                    curr_shard_bytes = 0;
               }
          }
          
          file.close();    
     }
     shard.shard_id = shard_id;
     fileShards.push_back(shard);
	return true;
}
