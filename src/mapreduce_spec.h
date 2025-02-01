#pragma once

#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>


using std::getline;
using std::ifstream;
using std::stoi;
using std::string;
using std::vector;
using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;              // number of workers
	int map_kilobytes;          // memory of each shard to be mapped
	string output_dir;          // output directory path for final and intermediate files
	string user_id;             // user running the map reduce
	int n_output_files;         // number of output files
	vector<string>ip_addresses; // list of ip addresses for workers
	vector<string>input_files;  // comma separated input files
	
};

inline vector<string>debug_me_please(string& input, char delimiter) {
    vector<string>list_of_files;
    istringstream input_stream(input);
    string token;

    while (getline(input_stream, token, delimiter)) {
        list_of_files.push_back(token);
	// cout << token <<endl;
    }

    return list_of_files;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {

	
	// Open the file
    	std::fstream config_file;
	config_file.open(config_filename, std::fstream::in);

    if (!config_file.is_open()) {
        std::cerr << "Unable to open the file." << std::endl;
        return false;
    }
	
	std::string line;
    while (getline(config_file, line)) {

        // Find the position of '='
        size_t pos = line.find('=');

        if (pos != string::npos) {

            // Extract the key and value
            string key   = line.substr(0, pos);
            string value = line.substr(pos + 1);
	    cout << key << endl;
	    cout << value << endl;
	    cout << key.size() << endl;
	    cout << value.size() << endl;
		key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
		key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
		value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
		value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);
			if (key == "n_workers"){
				mr_spec.n_workers = stoi(value);
			}

			else if (key == "map_kilobytes"){
				mr_spec.map_kilobytes = stoi(value);
				// mr_spec.map_kilobytes = 1;
			}

			else if (key == "n_output_files"){
				mr_spec.n_output_files = stoi(value);
			}

			else if (key == "user_id"){
				mr_spec.user_id = value;
			}

			else if (key == "worker_ipaddr_ports"){
				mr_spec.ip_addresses = debug_me_please(value, ',');
			}

			else if (key == "input_files"){
				mr_spec.input_files = debug_me_please(value, ',');
				// vector<string>t;
				// t.push_back("input/temp.txt");
				// mr_spec.input_files = t;
			}

			else {
				mr_spec.output_dir = value;
			}
        }
    }

    // Close the file
    config_file.close();

	return true;

}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	cout << "workers " << mr_spec.n_workers << endl;
	cout << "num ip addresses " <<mr_spec.ip_addresses.size() << endl;
	cout << "map kilobytes" <<mr_spec.map_kilobytes << endl;
	cout << "output dir" <<mr_spec.output_dir <<endl;
	cout << "input files" << mr_spec.input_files.size() << endl;
	cout << "output files" << mr_spec.n_output_files << endl;
	for (int i=0; i<mr_spec.input_files.size();i++){
		cout << mr_spec.input_files[i]<<endl;
	};
 	if (mr_spec.ip_addresses.size() != mr_spec.n_workers) return false;
	cout << "here 2" << endl;
	if (mr_spec.n_workers == 0 || mr_spec.map_kilobytes == 0 || mr_spec.n_output_files == 0) return false;
	cout << "here 3" << endl;
	if (mr_spec.output_dir.size() == 0 || mr_spec.input_files.size() == 0 || mr_spec.user_id.size() == 0) return false;
	cout << "here 4" << endl;
	return true;
}
