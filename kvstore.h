#pragma once
#include <list>
#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
struct SSKVWrapper
{
	uint64_t key;
	string value;
	uint32_t timestamp;
	SSKVWrapper(){}
	SSKVWrapper(uint64_t K,const string &V,uint32_t T):key(K),value(V),timestamp(T){}
};

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	uint32_t timestamp;
	Memtable memtable;
	vector<vector<SStable>> sstableLevels;
	vector<uint32_t> maxOrder;
	
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	std::string getReal(uint64_t key);

	bool del(uint64_t key) override;

	void reset() override;

	void writeToLevel(uint32_t level,std::vector<SSKVWrapper> &allKV);
	
	void compaction();

};
