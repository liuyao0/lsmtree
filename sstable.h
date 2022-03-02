#pragma once
#include <vector>
#include "memtable.h"
using std::vector;
const uint32_t BFsize=81920;
struct SStableHeader
{
public:    
    static uint32_t currentTimeStamp;
    uint32_t timestamp;
    uint64_t keynum;
    uint64_t minkey;
    uint64_t maxkey;
    SStableHeader(){};
    SStableHeader(uint32_t T,uint64_t K,uint64_t Min,uint64_t Max):
                timestamp(T),keynum(K),minkey(Min),maxkey(Max){};
};

class SStablePart
{
public:
    uint64_t idx;
    uint64_t minkey;
    uint32_t timestamp;
    SStablePart(uint64_t I,uint64_t Min,uint32_t T):idx(I),minkey(Min),timestamp(T){}
    bool operator>(const SStablePart &s) const
    {
        if(timestamp>s.timestamp)
            return true;    
        if(timestamp==s.timestamp)
        {
            if(minkey>s.minkey)
                return true;
            else
                return false;
        }
        return false;
    }
    
};
struct SStableIndex
{
    uint64_t key;
    uint32_t offset;
    SStableIndex(){};
    SStableIndex(uint64_t K,uint32_t O):key(K),offset(O){};
};


class SStable
{
public:
    uint64_t level;
    uint64_t order;
    SStableHeader header;
    bool bloomfilter[BFsize];
    vector<SStableIndex> index;
public:
    SStable();
    SStable(const SStable &s);
    SStable(Memtable &memtable,string fileName);
    void writetoFile(char* filename,Memtable &memtable);
    ~SStable();
};