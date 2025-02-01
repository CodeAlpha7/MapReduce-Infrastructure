#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <map>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

#include <grpcpp/grpcpp.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;

using masterworker::WorkRequest;
using masterworker::WorkResponse;
using masterworker::MapReduceService;

using std::string;
using std::getline;
using std::endl;
using std::vector;
using std::ifstream;
using std::unique_ptr;

using namespace std;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

// hash in emit appends the file number in the end
// string construct_file_name(string userid,string directory,string map_or_reduce,string worker_id){
// 	return directory + "/" + userid + "_" + map_or_reduce +"_"+ worker_id + "_" ;									
// }

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();
	std::string ip_address;

        ~Worker(){
            worker_server_->Shutdown();
            cq_->Shutdown();      
        }	
	
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
// 		std::string ip_address;

        // Class encompasing the state and logic needed to serve a request.
        class CallData {
        public:

            // Take in the "service" instance (in this case representing an asynchronous
            // server) and the completion queue "cq" used for asynchronous communication
            // with the gRPC runtime.
            CallData(MapReduceService::AsyncService* service, ServerCompletionQueue* cq)
                : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE){
            // Invoke the serving logic right away.'
            cout << "I am at call data" << endl;
            Proceed();
            }

            void Proceed() {
            if (status_ == CREATE) {
                // Make this instance progress to the PROCESS state.
                status_ = PROCESS;

                cout << "worker performing work" << endl;
                service_->RequestPerformWork(&ctx_, &request_, &responder_, cq_, cq_, this);
            }

        else if (status_ == PROCESS) {
            cout << "worker is in process" <<endl;
            cout << request_.worker_type() << endl;
            new CallData(service_, cq_);
            cout << "worker deciding map or reduce "<<endl;
            // The actual processing.
            if(request_.worker_type()=="map"){
                cout << "worker is a mapper "<<endl;
                std::vector<std::string> intermediate_files = this->map_worker(request_);
                for(int i=0; i>intermediate_files.size(); i++){
                    reply_.add_output_files(intermediate_files[i]);
                }
                reply_.set_success(true);
            }
            else{
                std::string output_file = this->reduce_worker(request_);
                reply_.set_success(true);
                reply_.add_output_files(output_file);
            }

            // And we are done! Let the gRPC runtime know we've finished, using the
            // memory address of this instance as the uniquely identifying tag for
            // the event.
            status_ = FINISH;
            responder_.Finish(reply_, Status::OK, this);
            }
            else {
                GPR_ASSERT(status_ == FINISH);
                // Once in the FINISH state, deallocate ourselves (CallData).
                delete this;
            }
        }
        private:
            MapReduceService::AsyncService* service_;
            ServerCompletionQueue* cq_;
            ServerContext ctx_;
            WorkRequest request_;
            WorkResponse   reply_;
            ServerAsyncResponseWriter<WorkResponse> responder_;
            enum CallStatus { CREATE, PROCESS, FINISH };
            CallStatus status_;

		std::vector<std::string> map_worker(WorkRequest request){
			std::cout << "got mapper request:"<<std::endl;
			std::vector<std::string>intermediate_files;
			std::string user_id = request.user_id();
			std::string output_dir = request.output_directory();
			std::ofstream log("log.txt", std::ios::app);
// 			string out_file   = construct_file_name(user_id, output_dir, request.worker_type(), request.worker_id());
			std::vector<std::string>out_files;
			std::cout << "here are my interemdeiate files " << std::endl;
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
				std::cout<<"opened input file name: " << file_name << endl;
// 				std::ifstream file(file_name);
				std::ifstream file(file_name, std::ios::in);
				string start_line ="",last_line="";
				if(!file.is_open()){
					std::cerr<<"Unable to open below file: "<<endl<<file_name<<endl;
				}
                file.seekg(start_bytes, file.beg);
				while(file.tellg()<=end_bytes && !file.eof() && file.tellg()!=-1){
					getline(file, line);
					my_mapper->map(line);
				}
				log << "File: " << file_name << "\n";
				log << "Start line: " << start_line << std::endl;
				log << "last line: " << last_line << std::endl;
				file.close();
			}
			return my_mapper->impl_->get_intermediate_files();
		}

		std::string reduce_worker(WorkRequest request){
			std::vector<std::string>output_files;
			std::map<std::string, std::vector<std::string>> key_to_value_map;
			std::string user_id = request.user_id();
// 			string output_dir = request.output_directory();
// 			string out_file   = construct_file_name(user_id, output_dir, request.worker_type(), request.worker_id());
			std::string out_file = request.output_directory();
			auto my_reducer = get_reducer_from_task_factory(request.user_id());
			my_reducer->impl_->file_name_setter(out_file);
			
			std::string line;
			for (auto file_name: request.file_names()){
// 				std::ifstream file(file_name);
				std::ifstream file(file_name, std::ios::in);
				if(!file.is_open()){
					std::cerr<<"Unable to open below file: "<<std::endl<<file_name<<std::endl;
				}
				while(getline(file, line)){
					
                    if(file.tellg()==-1){
                        break;
                    }
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
			}
            		std::vector<std::string> keys;
			for(auto key: key_to_value_map)
			{
				keys.push_back(key.first);
			}
			std::sort(keys.begin(), keys.end());


            for(auto iter = keys.begin(); iter!= keys.end(); iter++){
                my_reducer->reduce(*iter, key_to_value_map[*iter]);
                // my_reducer->reduce(iter->first, iter->second);
            }
            return "files";
		}
};

        void HandleRpcs(){
                // Spawn a new CallData instance to serve new clients.
                new CallData(&service_, cq_.get());
                void* tag;  // uniquely identifies a request.
                bool ok;
                while (true) {
                // Block waiting to read the next event from the completion queue. The
                // event is uniquely identified by its tag, which in this case is the
                // memory address of a CallData instance.
                // The return value of Next should always be checked. This return value
                // tells us whether there is any kind of event or cq_ is shutting down.
                GPR_ASSERT(cq_->Next(&tag, &ok));
                GPR_ASSERT(ok);
                static_cast<CallData*>(tag)->Proceed();
                }    
        }
        unique_ptr<ServerCompletionQueue> cq_;
        MapReduceService::AsyncService service_;
        unique_ptr<Server> worker_server_;
};

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	ip_address = ip_addr_port;
	// to do worker id
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	std::string server_address(ip_address);
	ServerBuilder builder;
	builder.AddListeningPort(ip_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    worker_server_ = builder.BuildAndStart();

    std::cout << "Worker server listening on :" << server_address << std::endl;
    HandleRpcs();
	return true;
}
