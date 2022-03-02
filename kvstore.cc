#include "kvstore.h"
#include "MurmurHash3.h"
#include <string>
#include <fstream>
#include <iostream>
#include <list>
#include <queue>
#include <set>
#include <algorithm>
#include <sstream>
#include <cstring>
using std::set;
using std::ofstream;
using std::ifstream;
using std::list;
using std::priority_queue;
using namespace std;
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	vector<string> pths;
	vector<string> files;
	utils::scanDir("./data/",pths);
	uint64_t level=0,order;
	for(auto &pth:pths)
	{
		if(pth.substr(0,6)!="level-")
			continue;
		pth.erase(0,6);
		if(level<std::stoul(pth))
			level=std::stoul(pth);
	}
	maxOrder.resize(level+1);
	sstableLevels.resize(level+1);
	for(auto &pth:pths)
	{
		level=std::stoul(pth);
		files.clear();
		utils::scanDir("./data/level-"+std::to_string(level)+"/",files);
		for(auto &file:files)
		{
			SStable sstable;
			ifstream f;
			uint64_t i=0;
			char sym;
			string a="./data/level-"+std::to_string(level)+"/"+file;
			stringstream ss;
			ss.str(file);
			ss>>sstable.level;
			ss>>sym;
			ss>>sstable.order;
			f.open(a.c_str());
			f.read((char*)(&(sstable.header.timestamp)),sizeof(sstable.header.timestamp));
			f.read((char*)(&(sstable.header.keynum)),sizeof(sstable.header.keynum));
			f.read((char*)(&(sstable.header.minkey)),sizeof(sstable.header.minkey));
			f.read((char*)(&(sstable.header.maxkey)),sizeof(sstable.header.maxkey));
			f.read((char*)(&(sstable.bloomfilter)),sizeof(sstable.bloomfilter));
			sstable.index.resize(sstable.header.keynum+1);
			for(i=0;i<sstable.header.keynum;i++)
			{
				f.read((char*)(&(sstable.index[i].key)),sizeof(sstable.index[i].key));
				f.read((char*)(&(sstable.index[i].offset)),sizeof(sstable.index[i].offset));
			}
			f.seekg(sstable.index[i-1].offset,ios::beg);
			char lst;
			sstable.index[i].offset=sstable.index[i-1].offset;
			while(1)
			{
				f.read((char*)&lst,1);
				sstable.index[i].offset++;
				if(lst=='\0')
					break;
			}
			sstableLevels[level].emplace_back(sstable);
			f.close();
		}
	}
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(!memtable.put(key,s))//向memtable中插入失败，需转为sstable再插
	{
		if(!utils::dirExists("./data/level-0"))
			utils::mkdir("./data/level-0");
		vector<string> files;
		sstableLevels[0].emplace_back(memtable,("./data/level-0/0-"+std::to_string(maxOrder[0])));
		sstableLevels[0].rbegin()->level=0;
		sstableLevels[0].rbegin()->order=maxOrder[0];
		maxOrder[0]++;
		compaction();
		memtable.clean();
		memtable.put(key,s);
	}
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::getReal(uint64_t key)
{
	string result=memtable.get(key);
	if(result!="")
		return result;
	uint64_t maxTime=0;
	uint32_t level=0;
	for(auto &sstableLevel:sstableLevels)
	{
		for(auto &sstable:sstableLevel)
		{
			uint32_t hash[4];
			bool inSStable=true;
        	MurmurHash3_x64_128(&key,8,1,hash);
			if(sstable.header.minkey>key||sstable.header.maxkey<key)
				continue;
			for(int i=0;i<4;i++)
				if(!sstable.bloomfilter[hash[i]%BFsize])
					inSStable=false;
			if(!inSStable)
				continue;
			if(sstable.header.timestamp<maxTime)
				continue;
			uint64_t l=0,r=sstable.header.keynum-1,m;
			while(l<=r)
			{
				m=(l+r)/2;
				if(key<sstable.index[m].key)
					r=m-1;
				else if(key>sstable.index[m].key)
					l=m+1;
				else
					break;
			}
			if(key!=sstable.index[m].key)
				continue;
			uint32_t offset=sstable.index[m].offset;
			uint32_t readlength=sstable.index[m+1].offset-offset;
			fstream file;
			file.open("./data/level-"+std::to_string(level)+"/"+std::to_string(level)+"-"+std::to_string(sstable.order));
			char* value=new char[readlength];
			file.seekg(offset,std::ios::beg);
			file.read(value,readlength);
			file.close();
			maxTime=sstable.header.timestamp;
			result=value;
			delete []value;
		}
		level++;
	}
	return result;
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string result=getReal(key);
	if(result=="~DELETED~")
		return "";
	return result;

}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	if(memtable.del(key))
	{
		put(key,"~DELETED~");
		return true;
	}
	string val=getReal(key);
	if(val==""||val=="~DELETED~")
	{
		put(key,"~DELETED~");
		return false;
	}
	put(key,"~DELETED~");
	return true;
}


void delElementbyIndex(vector<SStable> &vec,vector<uint64_t> idx)
{
	std::sort(idx.begin(),idx.end());
	while(!idx.empty())
	{
		uint64_t i=*(idx.rbegin());
		idx.pop_back();
		auto ele=vec.begin()+i;
		uint64_t level=ele->level;
		uint64_t order=ele->order;
		utils::rmfile(("./data/level-"+std::to_string(level)+"/"+std::to_string(level)+"-"+std::to_string(order)).c_str());
		vec.erase(ele);
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	vector<uint64_t> index;
	vector<string> pths;
	size_t size;
	for(auto &sstableLevel:sstableLevels)
	{
		index.clear();
		size=sstableLevel.size();
		for(size_t i=0;i<size;i++)
			index.push_back(i);
		delElementbyIndex(sstableLevel,index);
	}
	utils::scanDir("./data/",pths);
	for(auto &pth:pths)
		utils::rmdir(("./data/"+pth).c_str());
	memtable.clean();
}


void getallKV(SStable &sstable,vector<SSKVWrapper> &allKV)
{
	allKV.clear();
	fstream file;
	file.open("./data/level-"+std::to_string(sstable.level)+"/"+std::to_string(sstable.level)+"-"+std::to_string(sstable.order));
	size_t keynum=sstable.header.keynum;
	for(size_t i=0;i<keynum;i++)
	{
		uint32_t offset=sstable.index[i].offset;
		uint32_t readlength=sstable.index[i+1].offset-offset;
		char* value=new char[readlength];
		file.seekg(offset,std::ios::beg);
		file.read(value,readlength);
		allKV.emplace_back(sstable.index[i].key,string(value),sstable.header.timestamp);

		delete []value;
	}
	file.close();
}

void dupRemove(vector<SSKVWrapper> &l)
{
	uint64_t i=0,j,KVnum=l.size();
	while(i<KVnum)
	{
		while (true)
		{
			j=i+1;
			if(j==KVnum)
				break;
			if(l[i].key!=l[j].key)
				break;
			if(l[i].timestamp<l[j].timestamp)
			{
				l[i].value=l[j].value;
			}
			l.erase(l.begin()+j);
				KVnum--;
			break;
		}
		i++;
	}
}

void mergesort(list<vector<SSKVWrapper>> &key_value)
{
	while(key_value.size()>1)
	{
		auto array1=key_value.back();
		key_value.pop_back();
		auto array2=key_value.back();
		uint64_t i=0,j=0,k=0,array1_size=array1.size(),array2_size=array2.size();
		key_value.rbegin()->resize(array1_size+array2_size);
		auto &temp=*(key_value.rbegin());
		temp.resize(array1_size+array2_size);
		while(i<array1_size&&j<array2_size)
		{
			if(array1[i].key<=array2[j].key)
				temp[k++]=array1[i++];
			else
				temp[k++]=array2[j++];
		}
		while(i<array1_size)
			temp[k++]=array1[i++];
		while(j<array2_size)
			temp[k++]=array2[j++];
	}	
}

void KVStore::writeToLevel(uint32_t level,vector<SSKVWrapper> &allKV)
{
	if(allKV.empty())
		return;

	if(sstableLevels.size()<level+1)
		sstableLevels.emplace_back();
	if(!utils::dirExists("./data/level-"+std::to_string(level)))
		utils::mkdir(("./data/level-"+std::to_string(level)).c_str());
	uint64_t i=0,j,k,KVnum=allKV.size();
	
	while(i<KVnum)
	{
		ofstream file;
		SStable sstable;
		sstable.level=level;
		sstable.order=maxOrder[level];
		sstable.header.timestamp=allKV[i].timestamp;
		sstable.header.minkey=sstable.header.maxkey=allKV[i].key;
		sstable.index.clear();
		string a="./data/level-"+std::to_string(level)+"/"+std::to_string(level)+"-"+std::to_string(maxOrder[level]);
		file.open(a);
		maxOrder[level]++;
		uint64_t byte=sizeof(sstable.header.timestamp)+
		 			  sizeof(sstable.header.keynum)+
            		  sizeof(sstable.header.minkey)+
           			  sizeof(sstable.header.maxkey)+
           			  BFsize;
		j=i;//从i开始向后搜索直到达到文件大小的限度
		while(j!=KVnum)
		{
			uint32_t hash[4];
			//长度达到一个文件的限度则跳出
			if(byte+8+(allKV[j].value.length()+1)*sizeof(char)>SStableMaxSize)
				break;
			byte+=sizeof(uint32_t)+sizeof(uint64_t)+(allKV[j].value.length()+1)*sizeof(char);

			//更新最大键、最小键、布隆过滤器、时间戳
			if(allKV[j].key>sstable.header.maxkey)
				sstable.header.maxkey=allKV[j].key;
			else if(allKV[j].key<sstable.header.minkey)
				sstable.header.minkey=allKV[j].key;
        	MurmurHash3_x64_128(&(allKV[j].key),8,1,hash);
        	for(int k=0;k<4;k++)
            	sstable.bloomfilter[hash[k]%BFsize]=true;
			if(sstable.header.timestamp<allKV[j].timestamp)
				sstable.header.timestamp=allKV[j].timestamp;
			j++;
		}
		sstable.header.keynum=j-i;
		sstable.index.resize(sstable.header.keynum+1);	
		//计算文件中第一个string的偏移量
		uint32_t offset=sizeof(sstable.header.timestamp)+
		 			  	sizeof(sstable.header.keynum)+
            		 	sizeof(sstable.header.minkey)+
           			  	sizeof(sstable.header.maxkey)+
           			  	BFsize+
           			  	(sizeof(uint64_t)+sizeof(uint32_t))*sstable.header.keynum;
		
	    file.write((char*)(&(sstable.header.timestamp)),sizeof(sstable.header.timestamp));
		file.write((char*)(&(sstable.header.keynum)),sizeof(sstable.header.keynum));
		file.write((char*)(&(sstable.header.minkey)),sizeof(sstable.header.minkey));
		file.write((char*)(&(sstable.header.maxkey)),sizeof(sstable.header.maxkey));
		file.write((char*)(&(sstable.bloomfilter)),sizeof(sstable.bloomfilter));
		//写入偏移量
		for(k=i;k<j&&k<KVnum;k++)
		{
			SStableIndex idx(allKV[k].key,offset);
			sstable.index[k-i]=idx;
			file.write((char*)(&idx.key),sizeof(idx.key));
			file.write((char*)(&idx.offset),sizeof(idx.offset));
			offset+=allKV[k].value.length()+1;
		}
		//构造尾index方便最后一个元素通过offset差计算其长度
		sstable.index[k-i]=SStableIndex(-1u,offset);

		//写入字符串
		for(k=i;k<j&&k<KVnum;k++)
		{
			file.write((allKV[k].value).c_str(),sizeof(char)*((allKV[k].value).length()+1));
		}
		sstableLevels[level].push_back(sstable);
		file.close();
		i=k;
	}
}

void KVStore::compaction()
{
	//不需要归并则退出
	if(sstableLevels[0].size()<=2)
		return;
	set<uint64_t> keys; //归并涉及到的所有键
	vector<uint64_t> nextSStables; //index of sstables in next layer which will be compaction.
	list<vector<SSKVWrapper>> keys_values; //归并涉及的所有键值对（未归并）
	vector<SSKVWrapper> allKV;//某个容器的所有键值对

	//0层全部取出
	for(auto &sstable:sstableLevels[0])
	{
		getallKV(sstable,allKV);
		keys_values.push_back(allKV);
		for(auto ssindex=sstable.index.begin();ssindex!=sstable.index.end()-1;ssindex++)
			keys.insert(ssindex->key);
		utils::rmfile(("./data/level-0/0-"+std::to_string(sstable.order)).c_str());
	}
	
	sstableLevels[0].clear();
	maxOrder[0]=0;

	//如果第一层存在，查找所有与第一层中键有交集的sstable
	if(sstableLevels.size()>1)
	{
		for(auto iter=sstableLevels[1].begin();
			iter!=sstableLevels[1].end();
			iter++)
		{
			for(auto ssindex=iter->index.begin();ssindex!=iter->index.end()-1;ssindex++)
				if(keys.count(ssindex->key))
				{
					nextSStables.push_back(iter-sstableLevels[1].begin());
					break;
				}
		}
	}
	else
	{
		maxOrder.push_back(0);
	}

	//将所有涉及到的表的键值对放入容器
	for(auto idx:nextSStables)
	{
		getallKV(sstableLevels[1][idx],allKV);
		keys_values.push_back(allKV);
	}
	//归并排序
	mergesort(keys_values);

	vector<SSKVWrapper> &kvHead=*(keys_values.begin());
	//去重
	dupRemove(kvHead);
	//写入
	writeToLevel(1,kvHead);

	sstableLevels[0].clear();
	delElementbyIndex(sstableLevels[1],nextSStables);
	uint32_t currentLevel=1;
	int32_t delta;

	//(delta=sstableLevels[currentLevel].size()-(1<<(currentLevel+1)))>0
	while((delta=(sstableLevels[currentLevel].size())-(1<<(currentLevel+1)))>0)
	{
		//clear key-value stored before
		keys_values.clear();
		keys.clear();
		//if the nextlayer doesn 't exist
		//level-0 ==
		//level-1 ====   currentLevel=1
		//level-2 ========  needSize=3>currentLevel+2
		if(sstableLevels.size()<currentLevel+2)
			sstableLevels.emplace_back();
		if(maxOrder.size()<currentLevel+2)
			maxOrder.push_back(0);
		sstableLevels[currentLevel+1].reserve(1<<(currentLevel+2));
		//get sstables to be compaction
		nextSStables.clear();
		priority_queue<SStablePart,vector<SStablePart>,std::greater<SStablePart>> pq;

		for(uint32_t i=0;i<sstableLevels[currentLevel].size();i++)
			pq.emplace(i,sstableLevels[currentLevel][i].header.minkey,sstableLevels[currentLevel][i].header.timestamp);

		while(delta>0)
		{
			uint64_t idx=pq.top().idx;
			pq.pop();
			getallKV(sstableLevels[currentLevel][idx],allKV);
			keys_values.push_back(allKV);
			for(auto index=sstableLevels[currentLevel][idx].index.begin();
					 index!=sstableLevels[currentLevel][idx].index.end()-1;
					 index++)
				keys.insert(index->key);
			nextSStables.push_back(idx);
			delta--;
		}
		delElementbyIndex(sstableLevels[currentLevel],nextSStables);
		//clear info stored before
		nextSStables.clear();
		//search all table in next levels
		for(auto iter=sstableLevels[currentLevel+1].begin();
			iter!=sstableLevels[currentLevel+1].end();
			iter++)
			for(auto index=iter->index.begin();
				index!=iter->index.end()-1;
				index++)
				if(keys.count(index->key))
				{
					nextSStables.push_back(iter-sstableLevels[currentLevel+1].begin());
					break;
				}
		
		//get all KVs from these tables
		for(auto idx:nextSStables)
		{
			getallKV(sstableLevels[currentLevel+1][idx],allKV);
		}
			
		mergesort(keys_values);
		auto &KVHead1=*(keys_values.begin());
		dupRemove(KVHead1);
		//Remove tables;
			delElementbyIndex(sstableLevels[currentLevel+1],nextSStables);
		//write to this level
			writeToLevel(currentLevel+1,KVHead1);
		currentLevel++;
	}

}
