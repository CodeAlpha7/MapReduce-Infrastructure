#pragma once

#include <filesystem>
# include<iostream>
# include<vector>
#include <string>
#include <unistd.h>
#include <memory>
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <chrono>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::ServerAsyncResponseWriter;
using grpc::Status;

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Channel;

using masterworker::WorkRequest;
using masterworker::WorkResponse;
using masterworker::file_info;
using masterworker::MapReduceService;


using std::string;
using std::to_string;
using std::cout;
using std::endl;
using std::vector;

using namespace std;

bool createDirectory(const std::string& directoryPath) {
    if (mkdir(directoryPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0) {
        std::cout << "Directory created successfully." << std::endl;
        return true;
    } else {
        std::cerr << "Error creating directory." << std::endl;
        return false;
    }
}

bool directoryExists(const std::string& directoryPath) {
    struct stat info;
    return stat(directoryPath.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
}

class WorkerClient {
    public: 
        explicit WorkerClient(std::shared_ptr<Channel> channel)
	    : stub_(MapReduceService::NewStub(channel)) {}

        // std::string WorkerClient::PerformWork(WorkRequest request) {
        std::string PerformWork(WorkRequest request) {
//             int timer_threshold = 120;
//             std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(timer_threshold);

            Status status;
            CompletionQueue cq;
            WorkResponse reply;
            ClientContext context;
//             context.set_deadline(deadline);

            std::unique_ptr<ClientAsyncResponseReader<WorkResponse>> rpc(
		 stub_->PrepareAsyncPerformWork(&context, request, &cq));
            rpc->StartCall();

            rpc->Finish(&reply, &status, (void*)1);
            void* got_tag;
            bool ok = false;

            GPR_ASSERT(cq.Next(&got_tag, &ok));
            GPR_ASSERT(got_tag == (void*)1);
            GPR_ASSERT(ok);
		
// 		Status status = stub_->PerformWork(&context, request, &reply);

            if (status.ok()) {
				if(reply.success()){
					return "suceeded";
				}
	    		else{
	    			return "failed";
	    		}
	    	}
            else {
                return "RPC Failure!" + status.error_message();
            }

        }

    private:
    std::unique_ptr<MapReduceService::Stub> stub_;
};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        MapReduceSpec mrspec;
        std::vector<WorkerClient*> worker_queue;
        std::vector<vector<std::string>> map_files;
        std::vector<std::string> reduce_files;
        std::vector<FileShard> shards;
		std::vector<bool>worker_is_idle;
		int get_idle_worker();
		void assign_map_work(int i);
		void assign_reduce_work(int i);
};



/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
    mrspec = mr_spec;
    shards = file_shards;
    for(int i=0; i<mrspec.ip_addresses.size(); i++){
        WorkerClient *client = new WorkerClient(grpc::CreateChannel(mrspec.ip_addresses[i], grpc::InsecureChannelCredentials()));
		worker_queue.push_back(client);
    }
	for(int i=0; i<mrspec.n_workers; i++){
		worker_is_idle.push_back(true);
	} 
	std::string directoryPath = mrspec.output_dir;
    cout << directoryPath << endl;
    // Check if the directory exists
    // if (std::system(("test -d " + directoryPath).c_str()) == 0) {
    //     std::cout << "Directory already exists.\n";
    // } else {
    //     // Create the directory
    //     if (std::system(("mkdir -p " + directoryPath).c_str()) == 0) {
    //         std::cout << "Output Directory created successfully.\n";

    //         // Set permissions to 0777
    //         if (std::system(("chmod 0777 " + directoryPath).c_str()) == 0) {
    //             std::cout << "Permissions set to 0777.\n";
    //         } else {
    //             std::cerr << "Failed to set permissions.\n";
    //         }
    //     } else {
    //         std::cerr << "Failed to create directory.\n";
    //     }
    // }
	if(std::filesystem::exists(directoryPath)) {
		std::filesystem::remove_all(directoryPath);
	}
	std::filesystem::create_directory(directoryPath);

	std::string interimDirectoryPath = "intermediate";
	// if (std::system(("test -d " + interimDirectoryPath).c_str()) == 0) {
    //     std::cout << "Directory already exists.\n";
    // } else {
    //     // Create the directory
    //     if (std::system(("mkdir -p " + interimDirectoryPath).c_str()) == 0) {
    //         std::cout << "Intermediate Directory created successfully.\n";

    //         // Set permissions to 0777
    //         if (std::system(("chmod 0777 " + interimDirectoryPath).c_str()) == 0) {
    //             std::cout << "Permissions set to 0777.\n";
    //         } else {
    //             std::cerr << "Failed to set permissions.\n";
    //         }
    //     } else {
    //         std::cerr << "Failed to create interim directory.\n";
    //     }
    // }
	if(std::filesystem::exists(interimDirectoryPath)) {
		std::filesystem::remove_all(interimDirectoryPath);
	}
	std::filesystem::create_directory(interimDirectoryPath);

    // std::string command = "rm -rf " + directoryPath + "/*";
    // int result = std::system(command.c_str());
	
	// std::string InterimCommand = "rm -rf " + interimDirectoryPath + "/*";
    // int resultInterim = std::system(InterimCommand.c_str());

    // if (result == 0) {
    //     std::cout << "All files in the directory have been removed." << std::endl;
    // } else {
    //     std::cerr << "Error removing files. Command returned non-zero exit code." << std::endl;
    // }
	
	// if (resultInterim == 0) {
    //     std::cout << "All files in the Interrim Directory have been removed." << std::endl;
    // } else {
    //     std::cerr << "Error removing files. Command returned non-zero exit code." << std::endl;
    // }

	if (directoryExists(directoryPath)) {
		std::cout << "Directory exists." << std::endl;
	} else {
		std::cout << "Directory does not exist." << std::endl;
	}

	for(int i =0; i<mrspec.ip_addresses.size(); i++){
		vector<std::string>curr_files;
		for(int j =0; j<mrspec.n_output_files; j++){
			std::string file = interimDirectoryPath + "/" + mrspec.user_id + "_" + "map" + "_" + to_string(i) + "_" + to_string(j) + ".txt";
			curr_files.push_back(file);
			cout << file << endl;
		
			std::ofstream f(file);
			f.close();
			if (access(file.c_str(), F_OK) == 0) {
			//    std::cout << "File exists: " << file << std::endl;
			} else {
			std::cout << "File does not exist: " << file << std::endl;
			}
			// Set permissions using the chmod command
			std::string chmodCommand = "chmod 0777 " + file;
			int r = std::system(chmodCommand.c_str());

			if (r == 0) {
				std::cout << "File permissions set to 777." << std::endl;
			} else {
				std::cerr << "Error setting file permissions." << std::endl;
			}
		}
		map_files.push_back(curr_files);
		curr_files.clear();
	}
	for(int i=0; i<mrspec.n_output_files; i++){
		std::string file =   mrspec.output_dir + "/" + "final" + to_string(i) + ".txt";
		reduce_files.push_back(file);
		cout << file << endl;
		std::ofstream f(file);
		f.close();

			if (access(file.c_str(), F_OK) == 0) {
			//    std::cout << "File exists: " << file << std::endl;
			} else {
			   std::cout << "File does not exist: " << file << std::endl;
			}
			        // Set permissions using the chmod command
        std::string chmodCommand = "chmod 0777 " + file;
        int r = std::system(chmodCommand.c_str());

        if (r == 0) {
            std::cout << "File permissions set to 777." << std::endl;
        } else {
            std::cerr << "Error setting file permissions." << std::endl;
        }
	}
}

int Master::get_idle_worker(){
	int num_machines = mrspec.n_workers;
	while(1){
		for(int i=0; i<num_machines; i++){
			if(worker_is_idle[i]){
				return i;
			}
			if(i==num_machines-1){
				i=0; // start from beginning again
			}
		}

	}
}

// WorkRequest Master::get_map_request(int index, string worker_id){
// 	auto curr_shard = shards[index];
// 	WorkRequest wreq;
// 	wreq.set_worker_type("map");
// 	wreq.set_user_id(mrspec.user_id);
// 	wreq.set_num_reducers(mrspec.n_output_files);
// 	wreq.set_output_directory(mrspec.output_dir);
// 	wreq.set_worker_id = worker_id;
// 	for(auto const& finfo:curr_shard.file_info_list){
// 		file_info* file_info = wreq.add_file_infos();
// 		file_info->file_name = finfo.filename;
// 		file_info->start_bytes = finfo.start_bytes;
// 		file_info->end_bytes = finfo.end_bytes;
// 	}
// 	return wreq;
// }

// WorkRequest Master::get_reduce_request(int index, string worker_id){
// 	WorkRequest wreq;
// 	wreq.set_worker_type("reduce");
// 	wreq.set_user_id(mrspec.user_id);
// 	wreq.set_num_reducers(mrspec.n_output_files);
// 	wreq.set_output_directory(mrspec.output_dir);
// 	wreq.set_worker_id = worker_id;
// 	for(int i=0; i<mrspec.n_workers; i++){
// 		// to do handle dead worker
// 		wreq.add_file_names(map_files[i][index]);
// 	}
// 	return wreq;
// }

void Master::assign_map_work(int i){
	int idle_worker = get_idle_worker();
	auto curr_shard = shards[i];
	WorkRequest wreq;
	wreq.set_worker_type("map");
	wreq.set_user_id(mrspec.user_id);
	wreq.set_num_reducers(mrspec.n_output_files);
	wreq.set_output_directory(mrspec.output_dir);
	wreq.set_worker_id(to_string(idle_worker));
	for(int j=0; j<curr_shard.file_info_list.size(); j++){
		info finfo=curr_shard.file_info_list[j];
		file_info* x = wreq.add_file_infos();
		x->set_file_name(finfo.filename);
		x->set_start_bytes(finfo.start_bytes);
		x->set_end_bytes(finfo.end_bytes);
	}
	for(int j=0; j<map_files[idle_worker].size();j++){
		wreq.add_file_names(map_files[idle_worker][j]);
	}
	worker_is_idle[idle_worker] = false;
	std::string response = worker_queue[idle_worker]->PerformWork(wreq);
	worker_is_idle[idle_worker] = true;
	// to do failure handling
	cout<< response << endl;
	cout<< "working on mapper: "<< idle_worker << " shard: " << i <<endl;
}

void Master::assign_reduce_work(int i){
	int idle_worker = get_idle_worker();
	WorkRequest wreq;
	wreq.set_worker_type("reduce");
	wreq.set_user_id(mrspec.user_id);
	wreq.set_num_reducers(mrspec.n_output_files);
	// using output directory to pass out put file for reducer
	wreq.set_output_directory(reduce_files[i]);
	wreq.set_worker_id(to_string(idle_worker));
	for(int j=0; j<mrspec.n_workers; j++){
		// to do handle dead worker
		wreq.add_file_names(map_files[j][i]);
	}
	worker_is_idle[idle_worker] = false;
	std::string response = worker_queue[idle_worker]->PerformWork(wreq);
	worker_is_idle[idle_worker] = true;
	// to do failure handling
	cout<< response << endl;
	cout<< "working on reducer: "<< idle_worker << " filenumber: " << i <<endl;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	
	std::cout<<"master is working"<<endl;
	for (int i=0; i< shards.size(); i++){
		assign_map_work(i);
	}
	// to do  handle failure cases
	// Wait on all map callbacks
	// while (_mapped_files.size() < _file_shards.size()) {}
	for (int i=0; i<mrspec.n_output_files; i++){
		assign_reduce_work(i);
	}
	return true;
}
