#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <climits>
#include <map>
#include "b+tree.h"
using namespace BPlusTree;
using namespace std;

#define KEY_TYPE unsigned long long int
#define PAYLOAD_TYPE unsigned long long int

template <class T>
bool load_binary_data(T data, int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str(), std::ios::binary | std::ios::in);
  if (!is.is_open()) {
    return false;
  }
  is.read(reinterpret_cast<char*>(data), std::streamsize(length * sizeof(T)));
  is.close();
  return true;
}

template <class T>
bool load_text_data(T array, int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str());
  if (!is.is_open()) {
    return false;
  }
  int i = 0;
  std::string str;
  while (std::getline(is, str) && i < length) {
    std::istringstream ss(str);
    ss >> array[i];
    i++;
  }
  is.close();
  return true;
}

void shuffleArray(unsigned long long int* arr, long long int n) {
  unsigned seed = 1;
	srand(seed);
	random_shuffle(arr, arr + n);
}



void to_uppercase(string &s) 
{
  size_t i;
  for (i = 0; i < s.size(); i++) {
    if (s[i] >= 'a' && s[i] <= 'z') {
      s[i] = s[i] + 'A' -'a';
    }
  }
}

int main(int argc, char **argv)
{
  Tree<KEY_TYPE, PAYLOAD_TYPE> t;
  Tree<KEY_TYPE, PAYLOAD_TYPE>::iterator it;
  Tree<KEY_TYPE, PAYLOAD_TYPE>::reverse_iterator rit;
  vector <PAYLOAD_TYPE> sv;
  istringstream ss;
  string prompt, s, l;
  PAYLOAD_TYPE val;
  KEY_TYPE key;
  vector <PAYLOAD_TYPE> vals;
  // vector <KEY_TYPE> keys;

  size_t i,size;

  auto keys_file_type = "binary";
	auto keys_file_path = "lognormal-190M.bin.data";
	auto total_num_keys = 1000000;

	auto keys = new KEY_TYPE[total_num_keys];
	if (keys_file_type == "binary") {
		load_binary_data(keys, total_num_keys, keys_file_path);
	} else if (keys_file_type == "text") {
		load_text_data(keys, total_num_keys, keys_file_path);
	} else {
		std::cout<< "--keys_file_type must be either 'binary' or 'text'"
				<< std::endl;
		
		// std::cerr << "--keys_file_type must be either 'binary' or 'text'"
		//           << std::endl;
		return 1;
	}

  PAYLOAD_TYPE sum1 = 0;
	for(long long int i = 0;i<total_num_keys;i++){
		t[keys[i]] = keys[i];
    sum1 += keys[i];
    // std::cout<<keys[i]<<endl;
	}
  double avg1 = (double) sum1 / total_num_keys;
  
  t.get_keys();
  cout<<"Number of levels for "<<total_num_keys<<" is "<<counter+1<<endl;

  shuffleArray(keys, total_num_keys);
	
  auto start = std::chrono::high_resolution_clock::now();
	map<KEY_TYPE, PAYLOAD_TYPE> mapper;
  PAYLOAD_TYPE sum2 = 0;
  for(long long int i = 0;i<total_num_keys;i++){
		// std::cout<<"Searching for the key "<<keys[i]<<" ";
		it = t.find(keys[i]);
    if(it == t.end()) {
      // std::cout<<"Not found" <<endl;
    }
    else {
      // std::cout<<"Found" <<endl;
      sum2 += it.get_val();
      mapper[keys[i]] = it.get_val();
    }
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed = end - start;
	
  double avg2 = (double) sum2 / (total_num_keys - 10);

  cout<<"Average "<<avg1<<endl;
  cout<<"Average after fetching "<<avg2<<endl;
	cout<<"Search time for "<<total_num_keys<<" keys is "<<elapsed.count()<<endl;




  t.clear();
  cout<<"Memory Occupied for "<<total_num_keys<<" keys is "<<clearedMemory/(1024*1024)<<" MB"<<endl;
  cout<<"Total node count for "<<total_num_keys<<" keys is "<<counter2<<endl;
}





