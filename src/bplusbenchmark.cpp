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



void print_commands(FILE *f)
{
  fprintf(f, "A tool program for creating the B+tree,\n");
  fprintf(f, "\n");

  fprintf(f, "INSERT/I key val ...   - Insert a (key, val)\n");
  fprintf(f, "ERASE/E key ...        - Erase the record given a key\n");
  fprintf(f, "FIND/F key ...         - Print the pair given a key\n");
  fprintf(f, "SIZE/S                 - Print the size of tree\n");
  fprintf(f, "KEYS/K                 - Print keys\n");
  fprintf(f, "VALS/V                 - Print vals\n");
  fprintf(f, "TRAVERSE/T A|D         - Traverse the tree and print the pair. A|D is to in ascending or descending order\n");
  fprintf(f, "LB key ...             - Print the pair whose key >= the given key\n");
  fprintf(f, "UB key ...             - Print the pair whose key >  the given key\n");
  fprintf(f, "CLEAR/C                - Clear the tree\n");

 
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







  // if (argc > 2 || (argc == 2 && strcmp(argv[1], "--help") == 0)) {
  //   fprintf(stderr, "usage: B+Tree [prompt]\n\n");
  //   print_commands(stderr);
  //   exit(1);
  //   }

  // if (argc == 2) {
  //   prompt = argv[1];
  //   prompt += " ";
  // }


  
  // while (1) {
  //   if (prompt != "") printf("%s", prompt.c_str());
  //   if (!getline(cin, l)) return 0;
  //   sv.clear();
  //   ss.clear();
  //   ss.str(l);
  //   while (ss >> s) sv.push_back(s);

    
  //   size = sv.size();
  //   if(size != 0) to_uppercase(sv[0]); 

  //   if (size == 0) {
  //   } else if (sv[0] == "?") {
  //     print_commands(stdout);
  //   } else if (sv[0] == "Q") {
  //     exit(0);

  //   } else if (sv[0] == "INSERT" || sv[0] == "I") {
  //     if (size < 3 || (size - 1) % 2 != 0) {
  //       printf("usage: INSERT/I key1 val1 key2 val2 ....\n");
  //     } else {
  //       for (i = 0; i < (size - 1) / 2; i++) {
  //         val = sv[2+i*2];

  //         if(sscanf(sv[1+i*2].c_str(), "%lf", &key) != 1) {
  //           printf("(%s, %s) is not a valid record\n", sv[1+i*2].c_str(), sv[2+i*2].c_str());
  //         } else {
  //           t[key] = val;
  //         }
  //       } 
  //     }

  //   } else if (sv[0] == "ERASE" || sv[0] == "E") {
  //     if (size < 2) {
  //       printf("usage: ERASE/E key1 key2 ...\n");
  //     } else {
  //       for (i = 1; i < size; i++) {
  //         if (sscanf(sv[i].c_str(), "%lf", &key) != 1) {
  //           printf("%s is not a valid key\n", sv[i].c_str());
  //         } else if (!t.contains(key)) {
  //           printf("key %s doesn't exist\n", sv[i].c_str());
  //         } else {
  //           t.erase(key);
  //         }
  //       }
  //     } 

  //   } else if (sv[0] == "FIND" || sv[0] == "F") {
  //     if (size < 2) {
  //       printf("usage: FIND/F key1 key2 ...\n");
  //     } else {

  //       for (i = 1; i < size; i++) {
  //         if (sscanf(sv[i].c_str(), "%lf", &key) != 1) {
  //           printf("%s is not a valid key\n", sv[i].c_str());
  //         } else {
  //           it = t.find(key);
  //           if (it == t.end()) {
  //             printf("key %s doesn't exist\n", sv[i].c_str());
  //           } else {
  //             printf("%s -> %s\n", sv[i].c_str(), it.get_val().c_str());
  //           }
  //         }
  //       }
  //     } 

  //   } else if (sv[0] == "SIZE" || sv[0] == "S") {
  //     printf("size: %zu\n",t.size());

  //   } else if (sv[0] == "KEYS" || sv[0] == "K") {
  //     keys = t.get_keys();
  //     for (i = 0; i < keys.size(); i++) printf("%.2lf ",keys[i]);
  //     printf("\n");

  //   } else if (sv[0] == "VALS" || sv[0] == "V") { 
  //     vals = t.get_vals();
  //     for (i = 0; i < vals.size(); i++) printf("%s ",vals[i].c_str());
  //     printf("\n");

  //   } else if (sv[0] == "TRAVERSE" || sv[0] == "T") {
  //     if (size != 2 || (sv[1] != "A" && sv[1] != "D")) {
  //       printf("usage: TRAVERSE/T A|D\n");
  //     } else {
  //       if (sv[1] == "A") {
  //         for (it = t.begin(); it != t.end(); it++) {
  //           printf("%.2lf -> %s\n", it.get_key(), it.get_val().c_str());
  //         }
  //       } else {
  //         for (rit = t.rbegin(); rit != t.rend(); rit++) {
  //           printf("%.2lf -> %s\n", rit.get_key(), rit.get_val().c_str());
  //         }
  //       }
  //     }

  //   } else if (sv[0] == "LB") {
  //     if (size < 2) {
  //       printf("usage: LB key1 key2 ...\n");
  //     } else {
  //       for (i = 1; i < size; i++) {
  //         if (sscanf(sv[i].c_str(), "%lf", &key) != 1) {
  //           printf("%s is not a valid key\n", sv[i].c_str());
  //         } else {
  //           it = t.lower_bound(key);
  //           if (it == t.end()) {
  //             printf("key %s doesn't have a lower_bound\n", sv[i].c_str());
  //           } else {
  //             printf("%s lower_bound: %.2lf -> %s\n", sv[i].c_str(), it.get_key(), it.get_val().c_str());
  //           }
  //         }
  //       }
  //     }
  //   } else if (sv[0] == "UP") {
  //     if (size < 2) {
  //       printf("usage: UP key1 key2 ...\n");
  //     } else {
  //       for (i = 1; i < size; i++) {
  //         if (sscanf(sv[i].c_str(), "%lf", &key) != 1) {
  //           printf("%s is not a valid key\n", sv[i].c_str());
  //         } else {
  //           it = t.upper_bound(key);
  //           if (it == t.end()) {
  //             printf("key %s doesn't have a upper_bound\n", sv[i].c_str());
  //           } else {
  //             printf("%s upper_bound: %.2lf -> %s\n", sv[i].c_str(), it.get_key(), it.get_val().c_str());
  //           }
  //         }
  //       }
  //     }
  //   } else if (sv[0] == "CLEAR" || sv[0] == "C") {
  //     t.clear();
  //   }

  // } // end of while 

}





