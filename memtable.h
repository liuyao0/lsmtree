#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__
#include <string>
#include <vector>
#include "utils.h"
using std::string;
using std::vector;
const uint64_t SStableMaxSize=2*1024*1024;
const uint64_t KeySize=8;
const uint64_t OffsetSize=8;
template <typename K, typename V>
struct Node
{
    Node<K, V> *right, *down;
    K key;
    V val;
    Node(Node<K, V> *right, Node<K, V> *down, K key, V val) : right(right), down(down), key(key), val(val) {}
    Node() : right(nullptr), down(nullptr) {}
};

template <typename K, typename V>
class Skiplist
{
public:
    Node<K, V> *head;
    Skiplist()
    {
        head = new Node<K, V>();
    }

    uint64_t size()
    {
        Node<K, V> *p = head;
        uint64_t s = 0;
        while (p->down)
            p = p->down;
        while (p->right)
        {
            p = p->right;
            s++;
        }
        return s;
    }

    Node<K,V>* get(const K &key)
    {
        Node<K, V> *p = head;
        while (p)
        {
            while(p->right&&p->right->key<key)
                p=p->right;
            if(p->right&&p->right->key==key)
                return p->right;
            p = p->down;
        }
        return nullptr;
    }

    bool put(const K &key, const V &val)//False:change True:put
    {
        vector<Node<K, V>*> pathList;    //从上至下记录搜索路径
        Node<K, V> *p = head;
        while(p){
            while(p->right && p->right->key < key){ 
                p = p->right;
            }
            if(p->right&&p->right->key==key)
            {
                p=p->right;
                while(p)
                {
                    p->val=val;
                    p=p->down;
                }
                return false;
            }
            pathList.push_back(p);
            p = p->down;
        }

        bool insertUp = true;
        Node<K, V>* downNode= nullptr;
        while(insertUp && pathList.size() > 0){   //从下至上搜索路径回溯，50%概率
            Node<K, V> *insert = pathList.back();
            pathList.pop_back();
            insert->right = new Node<K, V>(insert->right, downNode, key, val); //add新结点
            downNode = insert->right;    //把新结点赋值为downNode
            insertUp = (rand()&1);   //50%概率
        }
        if(insertUp){  //插入新的头结点，加层
            Node<K, V> * oldHead = head;
            head = new Node<K, V>();
            head->right = new Node<K, V>(NULL, downNode, key, val);
            head->down = oldHead;
        }
        return true;
    }
    

    bool remove(const K &key)
    {
        if (!(head->right))
            return false;
        Node<K, V> *p;
        V objV;
        p = head;
        while (p)
        {
            while (p->right && p->right->key < key)
            {
                p = p->right;
            }
            if (p->right && p->right->key == key)
            {
                objV = p->right->val;
                break;
            }

            p = p->down;
        }
        if (!p)
            return false;
        do
        {
            while (p->right && (p->right->key < key || p->right->val != objV))
                p = p->right;
            Node<K, V> *tmp = p->right;
            p->right = p->right->right;
            delete tmp;
            p = p->down;
        } while (p);
        while (head->right == nullptr && head->down)
        {
            p = head->down;
            delete head;
            head = p;
        }
        return true;
    }

    void clean()
    {
        Node<K,V> *p=head,*q,*r;
        while(p)
        {
            q=p->right;
            while(q)
            {
                r=q->right;
                delete q;
                q=r;
            }
            q=p;
            p=p->down;  
            delete q;
        }
        head=new Node<K,V>();
    }

    K* minkey()
    {
        Node<K, V> *p = head;
        while (p->down)
            p = p->down;
        if (p->right)
            return &(p->right->key);
        return nullptr;
    }

    K* maxkey()
    {
        Node<K, V> *p = head;
        while (p->down)
            p = p->down;
        if(!p->right)
            return nullptr;
        while (p->right)
            p = p->right;
        return &(p->key);
    }

    Node<K,V>* gethead()
    {
        return head;
    }

    ~Skiplist()
    {
        Node<K,V> *p=head,*q,*r;
        while(p)
        {
            q=p->right;
            while(q)
            {
                r=q->right;
                delete(q);
                q=r;
            }
            q=p;
            p=p->down;
            delete q;
        }
    }

    
};

class Memtable
{
private:
    Skiplist<uint64_t,string> skiplist;
public:
    Memtable(){}
    bool put(uint64_t key,const string &s)
    {
        Node<uint64_t,string>* node=skiplist.get(key);
        uint64_t sizeToSStable=getsizeToSStable();
        if(node) //key exists
        {
            if(sizeToSStable+sizeof(char)*(s.length()-node->val.length())<=SStableMaxSize) //enough to insert new value
            {
                node->val=s;
                while(node->down)
                {
                    node=node->down;
                    node->val=s;
                }
                return true;
            }
        }
        else if(sizeToSStable+sizeof(char)*(s.length()+1)+KeySize+OffsetSize<=SStableMaxSize) //key doesn't exists,enough space to insert
        {
            skiplist.put(key,s);
            return true;
        }
        return false;
    }

    string get(uint64_t key)
    {
        Node<uint64_t,string>* node=skiplist.get(key);
        if(!node)
            return "";  
        else
            return node->val;
    }
    
    bool del(uint64_t key)
    {
        Node<uint64_t,string>* node=skiplist.get(key);
        if(node==nullptr)
            return false;
        if(node->val=="~DELETED~")
            return false;
        return true;
    }
    
    void clean()
    {
        skiplist.clean();
    }
    uint64_t size()
    {
        return skiplist.size();
    }

    uint64_t minkey()
    {
        uint64_t* min=skiplist.minkey();
        if(min)
            return *min;
        else
            return 0;
    }

    uint64_t maxkey()
    {
        uint64_t* max=skiplist.maxkey();
        if(max)
            return *max;
        else
            return 0;
    }

    Node<uint64_t,string>* gethead()
    {
        return skiplist.gethead();
    }

    uint64_t getsizeToSStable()
    {
        auto* p=skiplist.gethead();
        while(p->down)
            p=p->down;
        uint64_t size=28+81920;
        while(p->right)
        {
            p=p->right;
            size+=8+8+sizeof(char)*(p->val.length()+1);
        }
        return size;
    }
};
#endif