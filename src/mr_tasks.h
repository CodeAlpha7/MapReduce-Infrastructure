#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <fstream>

using std::to_string;
using std::string;
using std::vector;
using std::cout;
using std::endl;


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		// to store which file to write to
		// add the final hash string to undeerstand partioning 
		vector<std::string> file_names;
		// method to set the file_name
		void file_name_setter(vector<std::string> fn);
		int total_num_intermediate_files;
		void set_intermediate_files_count(const int num);
		vector<std::string> get_intermediate_files();
};

/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

inline void BaseMapperInternal::file_name_setter(vector<std::string> fn){
	cout << "MR TASKS: here are my intermediate files" << endl;
	for(auto f:fn){
		file_names.push_back(f);
		cout << f << endl;
	};

}

inline void BaseMapperInternal::set_intermediate_files_count(const int num){
	total_num_intermediate_files=num;
	cout << "MR TASKS: " << "Set the number of intermediate files" << endl;
}

inline vector<std::string> BaseMapperInternal::get_intermediate_files(){
// 	vector<std::string>files;
// 	for(int i = 0; i<total_num_intermediate_files; i++){
// 		files.push_back(file_name + to_string(i) + ".txt");
// 	}
// 	return files;
	return file_names;
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	int hash = std::hash<std::string>{}(key) % total_num_intermediate_files;
	std::string new_file_name = file_names[hash];
	std::ofstream stream(new_file_name, std::ios::app);
	cout << "MR TASKS: here is my emit" << endl;
	cout << "hashed filename " << new_file_name << endl;
	if(!stream.is_open()){
		std::cerr << "Could not open the filename with name below filename for mapper: "<< std::endl << new_file_name << std::endl;
	}
	stream << key << ", " <<  val << std::endl;
	cout << key << " " << val << " "<<stream.end << endl;
	stream.close();
	// std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
}

/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string file_name;
		void file_name_setter(const std::string fn);
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}

inline void BaseReducerInternal::file_name_setter(const std::string fn){
	file_name = fn;
}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::ofstream stream(file_name, std::ios::app);
	if(!stream.is_open()){
		std::cerr << "Could not open the filename with name below filename for reducer: "<< std::endl << file_name << std::endl;
	}
	stream << key << " " << val << std::endl;
	stream.close();
	// std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
