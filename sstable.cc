#include "sstable.h"
#include "MurmurHash3.h"
#include <fstream>
#include <iostream>
using std::ofstream;
uint32_t SStableHeader::currentTimeStamp=0;
SStable::SStable()
{
    for(int i=0;i<BFsize;i++)
        bloomfilter[i]=false;
        
}

SStable::SStable(const SStable &s): level(s.level),order(s.order),header(s.header)
{
    index=s.index;
    for(int i=0;i<BFsize;i++)
        bloomfilter[i]=s.bloomfilter[i];
}
SStable::SStable(Memtable &memtable,string fileName)
{
    uint32_t offset=0;
    for(int i=0;i<BFsize;i++)
        bloomfilter[i]=false;
    ofstream f;
    f.open(fileName);
    
    Node<uint64_t,string>* p=memtable.gethead(),*q;

    while(p->down)
        p=p->down;
    
    header.timestamp=SStableHeader::currentTimeStamp++;
    header.keynum=memtable.size();
    header.minkey=memtable.minkey();
    header.maxkey=memtable.maxkey();
    f.write((char*)(&(header.timestamp)),sizeof(header.timestamp));
    f.write((char*)(&(header.keynum)),sizeof(header.keynum));
    f.write((char*)(&(header.minkey)),sizeof(header.minkey));
    f.write((char*)(&(header.maxkey)),sizeof(header.maxkey));
    index.resize(header.keynum+1);
    for(q=p->right;q!=NULL;q=q->right)
    {
        uint32_t hash[4];
        MurmurHash3_x64_128(&(q->key),8,1,hash);
        for(int j=0;j<4;j++)
            bloomfilter[hash[j]%BFsize]=1;
    }

    f.write((char*)(bloomfilter),BFsize);
    offset+=sizeof(header.timestamp)+
            sizeof(header.keynum)+
            sizeof(header.minkey)+
            sizeof(header.maxkey)+
            BFsize+
            (sizeof(uint64_t)+sizeof(uint32_t))*(header.keynum);
            
    uint32_t i=0;
    for(q=p->right;q!=NULL;q=q->right)
    {
        SStableIndex idx(q->key,offset);
        index[i++]=idx;
        f.write((char*)(&idx.key),sizeof(idx.key));
        f.write((char*)(&idx.offset),sizeof(idx.offset));
        offset+=q->val.length()+1;
    }

    index[i]=SStableIndex(-1u,offset);

    for(q=p->right;q!=NULL;q=q->right)
    {
        f.write(q->val.c_str(),sizeof(char)*(q->val.length()+1));
    }
    f.close();
}

SStable::~SStable()
{
}
