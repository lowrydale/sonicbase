/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#ifdef _WIN32
#include "stdafx.h"
#endif

#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <iomanip>
#include <map>
#include <iterator>
#include <vector>
#include <algorithm>
#include <cstring>
#include <locale>
#include <future>
#include <math.h>
#include <cstring>
//#include "utf8.h"
#include "BigDecimal.h"
//#include <lzo/lzoconf.h>
//#include <lzo/lzo1x.h>
#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <list>
#include "ctpl_stl.h"
#include "blockingconcurrentqueue.h"

#include <jni.h>     
#include "com_sonicbase_index_NativePartitionedSkipListMap.h"

#include "sonicbase.h"
#include "ObjectPool.h"

#include "Comparator.h"
#include "Comparable.h"
using namespace skiplist;
#include "Iterator.h"
#include "Iterable.h"
using namespace skiplist;
#include "AbstractCollection.h"
#include "Collection.h"
#include "Set.h"
#include "Map.h"
#include "AbstractSet.h"
#include "AbstractMap.h"
#include "SortedMap.h"
#include "NavigableSet.h"
#include "NavigableMap.h"
#include "ConcurrentMap.h"
#include "ConcurrentNavigableMap.h"
#include "AtomicJava.h"
#include "ConcurrentSkipListMap.h"
#include "countdownlatch.hpp"

//#include <pthread.h>

#ifdef _WIN32
#include <Windows.h>
#else
#include <sys/time.h>
#include<pthread.h>
#include<semaphore.h>
#endif


#define SB_DEBUG 0


//extern "C" {void *__dso_handle = NULL; } extern "C" {void *__cxa_atexit = NULL; }

#define concurentThreadsSupported std::thread::hardware_concurrency()
const int PARTITION_COUNT = concurentThreadsSupported;//(int)fmax(1, (int)concurentThreadsSupported / 4);
//736
//(concurentThreadsSupported * 8)
 // 64 for 17mil tailBlock reads/sec

//const int NUM_RECORDS_PER_PARTITION =  32 + 2;
//const int BLOCK_SIZE = (PARTITION_COUNT * NUM_RECORDS_PER_PARTITION);

class QueueEntry {
public:
	Key *key;
	MyValue *value;
	long ret;
	ConcurrentSkipListMap<Key*, MyValue*>* map;
	clatch::countdownlatch *l;

	QueueEntry() :key(0), value(0), ret(-1), map(0), l(0) {

	}

	QueueEntry(const QueueEntry &rhs) : key(rhs.key), value(rhs.value), ret(rhs.ret), map(rhs.map), l(rhs.l) {
	}

	QueueEntry& operator= (const QueueEntry& f) {
	 	this->key = f.key;
	 	this->value = f.value;
	 	this->ret = f.ret;
	 	this->map = f.map;
	 	this->l = f.l;
	 	return *this;
	}

};

void processQueueRequests(moodycamel::BlockingConcurrentQueue<QueueEntry> *queue) {
	while (true) {
		QueueEntry entry;
		queue->wait_dequeue(entry);

		MyValue *v = entry.map->put(entry.key, entry.value);
		if (v != NULL) {
			long value = v->value;
			//map->valuePool->free(v);
			entry.ret = value;
		}
		else {
			entry.ret = -1;
		}

		entry.l->count_down();
	}
}

class PartitionedConcurrentSkipListMap {
public:
    long id = 0;
    ConcurrentSkipListMap<Key*, MyValue*> **maps = (ConcurrentSkipListMap<Key*, MyValue*>**)malloc(PARTITION_COUNT * sizeof(void*));
    KeyComparator *comparator = 0;
    PooledObjectPool<Key*> *keyPool = 0;
    PooledObjectPool<KeyImpl*> *keyImplPool = 0;
	PooledObjectPool<MyValue*> *valuePool = 0;
	moodycamel::BlockingConcurrentQueue<QueueEntry> **queues = (moodycamel::BlockingConcurrentQueue<QueueEntry> **)malloc(PARTITION_COUNT * sizeof(moodycamel::BlockingConcurrentQueue<QueueEntry>*));
	std::thread **threads = (std::thread **)malloc(PARTITION_COUNT * sizeof(std::thread *));
	int *dataTypes = 0;
	int fieldCount = 0;

	~PartitionedConcurrentSkipListMap() {
		delete comparator;
		delete[] dataTypes;
	}

    PartitionedConcurrentSkipListMap(JNIEnv * env, long indexId, jintArray dataTypes) {
    	id = indexId;

		fieldCount = env->GetArrayLength(dataTypes);
		jint *types = env->GetIntArrayElements(dataTypes, 0);
		this->dataTypes = new int[fieldCount];
		memcpy( (void*)this->dataTypes, (void*)types, fieldCount * sizeof(int));
		comparator = new KeyComparator(env, indexId, fieldCount, this->dataTypes);

    	keyPool = new PooledObjectPool<Key*>(new Key());
    	keyImplPool = createKeyPool(env, this->dataTypes, fieldCount);
		valuePool = new PooledObjectPool<MyValue*>(new MyValue());

        for (int i = 0; i < PARTITION_COUNT; i++) {
            maps[i] = new ConcurrentSkipListMap <Key*, MyValue *>(this, keyPool, keyImplPool, valuePool, this->dataTypes);
            queues[i] = new moodycamel::BlockingConcurrentQueue<QueueEntry>(100000);
			//threads[i] = new std::thread(processQueueRequests, queues[i]);
        }

		env->ReleaseIntArrayElements(dataTypes, types, 0);
    }

	PartitionedConcurrentSkipListMap(long indexId, int* dataTypes, int fieldCount) {
		id = indexId;
		for (int i = 0; i < PARTITION_COUNT; i++) {
			maps[i] = 0;
		}

		this->dataTypes = new int[fieldCount];
		memcpy((void*)this->dataTypes, (void*)dataTypes, fieldCount * sizeof(int));
		comparator = new KeyComparator(NULL, indexId, fieldCount, this->dataTypes);

        //keyImplPool = createKeyPool(0, this->dataTypes, fieldCount);
		//keyPool = new PooledObjectPool(new Key());
		//valuePool = new PooledObjectPool(new MyValue());

	}
};

std::mutex partitionIdLockPartitionedSkipListMap;

PartitionedConcurrentSkipListMap **allMapsPartitionedSkipListMap = new PartitionedConcurrentSkipListMap*[10000];
uint64_t highestMapIdPartitionedSkipListMap = 0;

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_initIndexSkipListMap
  (JNIEnv * env, jobject obj, jintArray dataTypes) {
	  {
	  		printf("init index - begin\n");
	  		fflush(stdout);
		  std::lock_guard<std::mutex> lock(partitionIdLockPartitionedSkipListMap);

		  if (highestMapIdPartitionedSkipListMap == 0) {
			 for (int i = 0; i < 10000; i++) {
				 allMapsPartitionedSkipListMap[i] = 0;
			}
		  }

		  uint64_t currMapId = highestMapIdPartitionedSkipListMap;
		  PartitionedConcurrentSkipListMap *newMap = new PartitionedConcurrentSkipListMap(env, currMapId, dataTypes);
		  allMapsPartitionedSkipListMap[highestMapIdPartitionedSkipListMap] = newMap;
		  highestMapIdPartitionedSkipListMap++;

	  		printf("init index - end\n");
	  		fflush(stdout);
		  return currMapId;
	  }
}


PartitionedConcurrentSkipListMap *getIndexSkipListMap(JNIEnv *env, jlong indexId) {
	PartitionedConcurrentSkipListMap* map = allMapsPartitionedSkipListMap[(int)indexId];
	if (map == 0) {
		throwException(env, "Invalid index id");
	}
	return map;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_put__J_3_3Ljava_lang_Object_2_3J_3J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues, jlongArray jRetValues) {


	if (SB_DEBUG) {
		printf("batch put begin\n");
		fflush(stdout);
	}

		PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
		if (map == 0) {
			return;
		}

		int len = env->GetArrayLength(jKeys);
		jlong *values = env->GetLongArrayElements(jValues, 0);
		jlong *retValues = env->GetLongArrayElements(jRetValues, 0);

		clatch::countdownlatch latch(len);

		for (int i = 0; i < len; i++) {
			jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(jKeys, i);

			Key * key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

			int partition = abs(key->hashCode(map->dataTypes) % PARTITION_COUNT);

			MyValue *vvalue = (MyValue*)map->valuePool->allocate();
				//printf("after alloc\n");
				//fflush(stdout);;

			vvalue->value = values[i];

			QueueEntry queueEntry;
			queueEntry.key = key;
			queueEntry.value = vvalue;
			queueEntry.map = map->maps[partition];
			queueEntry.l = &latch;

			if (map->queues[partition]->enqueue(queueEntry)) {
			//	printf("queuing\n");
			}
			else {
				latch.count_down();
				//countFailedToQueue++;

				MyValue *v = map->maps[partition]->put(key, vvalue);
				if (v != NULL) {
					long value = v->value;
					map->valuePool->free(v);
					//ret = value;
				}
			}
//			delete queueEntry.l;



//			MyValue *v = map->map->put(key, new MyValue(values[i]));
//			if (v != NULL) {
//				delete v;
//			}
//			deleteKey(env, map->dataTypes, key);
		}

		latch.await();

		env->ReleaseLongArrayElements(jValues, values, 0);
		env->ReleaseLongArrayElements(jRetValues, retValues, 0);

  	if (SB_DEBUG) {
  		printf("batch put end\n");
  		fflush(stdout);
  	}

}

std::atomic<int> callCount;
std::atomic<int> countFailedToQueue;


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_put__J_3Ljava_lang_Object_2J
  (JNIEnv * env, jobject obj, jlong indexId, jobjectArray jstartKey, jlong value) {
		if (SB_DEBUG) {
			printf("put begin\n");
			fflush(stdout);
		}
	uint64_t retValue = -1;
	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return -1;
	}

//	printf("before get key\n");
//	fflush(stdout);;

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jstartKey, map->comparator, map->comparator->fieldCount);
//	printf("after get key\n");
//	fflush(stdout);;

	if (key == 0) {
		printf("put, null key");
		fflush(stdout);
		return -1;
	}

	bool replaced = false;
		//printf("before hash key\n");
    	//fflush(stdout);;

	int partition =abs(key->hashCode(map->dataTypes) % PARTITION_COUNT);
		//after hash key\n");
    	//fflush(stdout);;

	MyValue *vvalue = (MyValue*)map->valuePool->allocate();
		//printf("after alloc\n");
    	//fflush(stdout);;

	vvalue->value = value;

	long ret = -1;

	if (false) {

		QueueEntry queueEntry;
		queueEntry.key = key;
		queueEntry.value = vvalue;
		queueEntry.map = map->maps[partition];


		if (map->queues[partition]->enqueue(queueEntry)) {
			queueEntry.l->await();
			ret = queueEntry.ret;
		}
		else {
			countFailedToQueue++;

			MyValue *v = map->maps[partition]->put(key, vvalue);
			if (v != NULL) {
				long value = v->value;
				map->valuePool->free(v);
				ret = value;
			}
		}
		delete queueEntry.l;
	}
	else {
		MyValue *v = map->maps[partition]->put(key, vvalue);
		if (v != NULL) {
			long value = v->value;
			map->valuePool->free(v);
			ret = value;
		}
	}

	//printf("after put key\n");
	//fflush(stdout);

	if (SB_DEBUG) {
		printf("put end\n");
		fflush(stdout);
	}
	return ret;
}


class SortedListNodeSkipListMap {
public:
	SortedListNodeSkipListMap * next = 0;
	SortedListNodeSkipListMap *prev = 0;
//	head_t *head = 0;
	Key *key = 0;
	uint64_t value = 0;
	int partition;
};


class PartitionResultsSkipListMap {
  public:
    Key **keys;
	uint64_t* values;
    int count = -1;
    int posWithinPartition = 0;
    Key *key = 0;
    uint64_t value = 0;
    int partition = 0;
	SortedListNodeSkipListMap sortedListNode;

	PartitionResultsSkipListMap() {
	}

	~PartitionResultsSkipListMap() {
		delete[] values;
		delete[] keys;
	}

    void init(int numRecordsPerPartition) {
    	values = new uint64_t[numRecordsPerPartition];
    	keys = new Key*[numRecordsPerPartition];

  //      key = new void*[2];
        for (int i = 0; i < numRecordsPerPartition; i++) {
            keys[i] = 0;
            values[i] = 0;
        }
    }
    Key* getKey() {
      return  key;
    }

	uint64_t getValue() {
      return value;
    }

    int getPartition() {
      return partition;
    }
  };

class SortedListSkipListMap {
 	SortedListNodeSkipListMap *head = 0;
 	SortedListNodeSkipListMap *tail = 0;
	bool ascending = true;
	PartitionedConcurrentSkipListMap *map = 0;

	SortedListNodeSkipListMap **pools = 0;
	int currPoolOffset = 0;
	int poolCount = 0;
	int poolSize = 0;;
	std::mutex l;

 	public:

 	SortedListSkipListMap(PartitionedConcurrentSkipListMap *map, bool ascending, int size) {
 		this->map = map;
 		this->ascending = ascending;
		//poolSize = size;
		//pools = new SortedListNodeSkipListMap*[1];
		//pools[0] = new SortedListNodeSkipListMap[size];
		//poolCount = 1;
 	}

	~SortedListSkipListMap() {/*
		SortedListNodeSkipListMap *curr = head;
		while (curr != 0) {
			SortedListNodeSkipListMap *next = curr->next;
			delete curr;
			curr = next;
		}
		*/

		
		//for (int i = 0; i < poolCount; i++) {
			//delete[] pools[i];
		//}
		//delete[] pools;
		
	}

	SortedListNodeSkipListMap *allocateNode() {
		//return new SortedListNodeSkipListMap();
		std::lock_guard<std::mutex> lock(l);

		currPoolOffset++;
		if (currPoolOffset > poolSize) {
			//printf("called resize");
			//fflush(stdout);

			SortedListNodeSkipListMap **newPools = new SortedListNodeSkipListMap*[++poolCount];
			memcpy(newPools, pools, (poolCount - 1) * sizeof(SortedListNodeSkipListMap*));
			newPools[poolCount - 1] = new SortedListNodeSkipListMap[poolSize];
			delete[] pools;
			pools = newPools;
			currPoolOffset = 1;
		}
		return &pools[poolCount - 1][currPoolOffset - 1];
	}

	int size() {
		int ret = 0;
		SortedListNodeSkipListMap *curr = head;
		while (curr != 0) {
			curr = curr->next;
			ret++;
		}
		return ret;
	}
 	void push(SortedListNodeSkipListMap *node) {
 		if (ascending) {
 			pushAscending(node);
 		}
 		else {
 			pushDescending(node);
 		}
	}

	void pushAscending(SortedListNodeSkipListMap *node) {
		if (tail == 0 || head == 0) {
			head = tail = node;
			return;
		}
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
		SortedListNodeSkipListMap *curr = tail;
		SortedListNodeSkipListMap *last = 0;
		int compareCount = 0;
		while (curr != 0 /*|| (!ascending && curr->next != 0)*/) {
			compareCount++;
			int cmp = node->key->compareTo(curr->key);//map->comparator->compare(node->key, curr->key);
			if (cmp > 0) {
				SortedListNodeSkipListMap *next = curr->next;
				if (next != 0) {
					node->next = next;
					next->prev = node;
				}
				if (curr == tail) {
				  tail->next = node;
				  node->prev = tail;
				  tail = node;
				}
				curr->next = node;
				node->prev = curr;
//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}
				return;
			}
			else {
				curr = curr->prev;
			}
		}

//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}

        head->prev = node;
        node->next = head;
        head = node;
 	}

	void pushDescending(SortedListNodeSkipListMap *node) {
		if (tail == 0 || head == 0) {
			head = tail = node;
			return;
		}
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
		SortedListNodeSkipListMap *curr = head;
 		SortedListNodeSkipListMap *last = 0;

		int compareCount = 0;
		while (curr != 0/*|| (!ascending && curr->next != 0)*/) {
			compareCount++;
			int cmp = node->key->compareTo(curr->key);//map->comparator->compare(node->key, curr->key);
			if (cmp < 0) {
				SortedListNodeSkipListMap *prev = curr->prev;
				if (prev != 0) {
					node->prev = prev;
					prev->next = node;

				}
				if (curr == head) {
				  head->prev = node;
				  node->next = head;
				  head = node;
				}
				curr->prev = node;
				node->next = curr;

				return;
			}
			else {
				curr = curr->next;
			}
		}

        tail->next = node;
        node->prev = tail;
        tail = node;


 		curr = head;
 	}

 	SortedListNodeSkipListMap *pop() {
 		if (ascending) {
 			return popAscending();
 		}
 		return popDescending();
	}

	SortedListNodeSkipListMap *popAscending() {
 		if (head == 0) {
 			return 0;
 		}

 		SortedListNodeSkipListMap *ret = 0;

		ret = head;

		SortedListNodeSkipListMap *next = head->next;

		if (next != 0) {
			next->prev = 0;
		}

		head = next;
		if (next == 0) {
			head = tail = 0;
		}

		return ret;
	}

	SortedListNodeSkipListMap *popDescending() {
// 		printf("pop\n");
// 		fflush(stdout);
 		if (tail == 0) {
 			return 0;
 		}
		SortedListNodeSkipListMap *ret = tail;
		SortedListNodeSkipListMap *prev = tail->prev;

		if (prev != 0) {
			prev->next = 0;
		}
		tail = prev;
		if (prev == 0) {
			head = tail = 0;
		}

		return ret;
 	}
 };


int nextEntriesSkipListMap(PartitionedConcurrentSkipListMap* map, int count, int numRecordsPerPartition, int partition, Key** keys, uint64_t* values, Key *currKey, bool tail) {

    int currOffset = 0;
    int countReturned = 0;
    {

    	ConcurrentNavigableMap<Key*, MyValue*> *navMap = 0;
        if (tail) {
        	navMap = map->maps[partition]->tailMap(currKey);
        }
        else {
        	navMap = map->maps[partition]->headMap(currKey)->descendingMap();
		}

		typename Map<Key*,MyValue*>::template Entry<Key*,MyValue*> *entry = map->maps[partition]->allocateEntry();
		Iterator<Map<Key*, MyValue*>::Entry<Key*, MyValue*>*> *i = ((ConcurrentSkipListMap<Key*, MyValue*>::SubMap<Key*, MyValue*>*)navMap)->entryIterator();

        bool found = 0;
		while (i != 0 && i->hasNext()) {

			i->nextEntry(entry);

//            printf("found=%ld", (uint64_t)p->key[0]);
//            fflush(stdout);

			//printf("key: %llu | %llu\n", *(uint64_t*)p->key->key[0], *(uint64_t*)p->key->key[1]);
			//fflush(stdout);

            keys[currOffset] = entry->getKey();
            values[currOffset] = entry->getValue()->value;
            //delete p;
            currOffset++;
            countReturned++;

            if (currOffset >= numRecordsPerPartition) {
                break;
            }
//		  	if (!found) {
//		  		printf("next not found");
//            	fflush(stdout);
//		  	}

        }

//        printf("count returned=%d\n", countReturned);
    }
    return countReturned;
}

void getNextEntryFromPartitionSkipListMap(
        PartitionedConcurrentSkipListMap* map, SortedListSkipListMap *sortedList, SortedListNodeSkipListMap *node, PartitionResultsSkipListMap* currResults,
        int count, int numRecordsPerPartition,
        PartitionResultsSkipListMap* entry, int partition, Key *currKey, jboolean first, bool tail) {

	if (entry->count == 0) {
//        	printf("count=0\n");
//        	fflush(stdout);
		entry->key = 0;
		entry->value = 0;
		entry->partition = partition;
		entry->posWithinPartition = 0;
		return;
	}

    if (entry->count == -1 || entry->posWithinPartition >= entry->count) {
        entry->posWithinPartition = 0;

        entry->count = nextEntriesSkipListMap(map, count, numRecordsPerPartition, partition, entry->keys, entry->values, currKey, tail);
//        printf("count=%d\n", entry->count);
//        fflush(stdout);
        if (entry->count == 0) {
//        	printf("count=0\n");
//        	fflush(stdout);
            entry->key = 0;
            entry->value = 0;
            entry->partition = partition;
            entry->posWithinPartition = 0;
            return;
        }


        if (!first /*|| !tail*/) {
			//printf("##################### !first");
            if (map->comparator->compare((void**)entry->keys[0], currKey) == 0)   {
                entry->posWithinPartition = 1;
            }
        }

        if (entry->posWithinPartition >= entry->count) {
//        	printf("count=0\n");
//        	fflush(stdout);
            entry->key = 0;
            entry->value = 0;
            entry->partition = partition;
            entry->posWithinPartition = 0;
            return;
        }
    }
    entry->key = entry->keys[entry->posWithinPartition];
    entry->keys[entry->posWithinPartition] = 0;

    entry->value = entry->values[entry->posWithinPartition];
//    printf("key=%ld, partition=%d, pos=%d", (uint64_t)entry->key[0], partition, entry->posWithinPartition);
//    fflush(stdout);
    entry->posWithinPartition++;
    entry->partition = partition;
	node->key = entry->key;
	node->value = entry->value;
	node->partition = partition;
	node->next = 0;
	node->prev = 0;
	sortedList->push(node);
}

bool next(PartitionedConcurrentSkipListMap* map, SortedListSkipListMap *sortedList, PartitionResultsSkipListMap* currResults,
	int count, int numRecordsPerPartition, Key **keys, uint64_t* values,
    int* posInTopLevelArray, int* retCount, bool tail) {

    Key *currKey = 0;
	uint64_t currValue = 0;
    int lowestPartition = 0;

    SortedListNodeSkipListMap *node = sortedList->pop();
	//printf("popped: %llu | %llu\n", *(uint64_t*)node->key->key[0], *(uint64_t*)node->key->key[1]);
	//fflush(stdout);

    if (node == 0) {
    	return false;
    }
    lowestPartition = node->partition;

    *posInTopLevelArray = lowestPartition;

    currKey = node->key;
    currValue = node->value;
    getNextEntryFromPartitionSkipListMap(map, sortedList, node, currResults, count, numRecordsPerPartition, &currResults[*posInTopLevelArray],
        *posInTopLevelArray, currKey, false, tail);

    if (currKey == 0) {
        return false;
    }
//    printf("curr_key=%ld\n", (long)currKey[0]);
//    fflush(stdout);
    keys[(*retCount)] = currKey;
    values[(*retCount)] = currValue;
    (*retCount)++;

	//delete node;

    return true;
}

jboolean JNICALL tailHeadlockArraySkipListMap
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray jstartKey, jint count, jboolean first, bool tail, jbyteArray bytes, jint len) {
	if (SB_DEBUG) {
		printf("tailHeadBlockArray begin\n");
		fflush(stdout);
	}

	int numRecordsPerPartition = (int)ceil((double)count / (double)PARTITION_COUNT) + 2;

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return true;
	}

	int posInTopLevelArray = 0;

	Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jstartKey, map->comparator, map->comparator->fieldCount);

	SortedListSkipListMap sortedList(map, tail, count);

	PartitionResultsSkipListMap* currResults = new PartitionResultsSkipListMap[PARTITION_COUNT];

	for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		currResults[partition].init(numRecordsPerPartition);
		getNextEntryFromPartitionSkipListMap(map, &sortedList, &currResults[partition].sortedListNode, currResults, count, numRecordsPerPartition,
			&currResults[partition], partition, startKey, first, tail);
	}

	Key **keys = new Key*[count];
	for (int i = 0; i < count; i++) {
		keys[i] = 0;
	}
	uint64_t* values = new uint64_t[count];
	int retCount = 0;
	bool firstEntry = true;
	while (retCount < count) {
		if (!next(map, &sortedList, currResults, count, numRecordsPerPartition, keys, values, &posInTopLevelArray, &retCount, tail)) {
			break;
		}
		if (firstEntry && (!first /*|| !tail*/)) {
			if (map->comparator->compare((void**)keys[0], startKey) == 0) {
				retCount = 0;
			}
			firstEntry = false;
		}
	}

	try
	{
		jboolean isCopy;
		jbyte* rawjBytes = env->GetByteArrayElements(bytes, &isCopy);

		jboolean fit = true;
		int *offset = new int[1]{ 0 };
		if (!writeLong(retCount, rawjBytes, offset, len)) {
			fit = false;
		}
		else {
			for (int i = 0; i < retCount; i++) {
				fit = serializeKeyValue(env, map->dataTypes, rawjBytes, len, offset, keys[i], values[i]);
				if (!fit) {
					break;
				}
			}
		}
		delete[] offset;


		//printf("wrote bytets %i\n", len);

		/*
		lzo_uint outLen = 0;
		lzo_bytep out = allocOut(vector.size(), outLen);
		lzo_uint newLen = 0;
		lzo_bytep newBytes = lzoCompress(jBytes, vector.size(), out, outLen, newLen);
		*/

		//jbyte *bytes = new jbyte[newLen];
		//for (int i = 0; i < newLen; i++) {
			//bytes[i] = newBytes[i];
		//}

//		jbyteArray bArray = env->NewByteArray(newLen);

		env->ReleaseByteArrayElements(bytes, rawjBytes, 0);
		//env->SetByteArrayRegion(bytes, 0, newLen, (jbyte*)jBytes);// newBytes);

		//delete[] newBytes;
		//delete[] jBytes;

		delete[] keys;
		delete[] values;
		deleteKey(env, map->dataTypes, startKey, map->keyPool, map->keyImplPool);
		delete[] currResults;

		if (SB_DEBUG) {
			printf("tailHeadBlockArray end\n");
			fflush(stdout);
		}

		return fit;
	}
	catch (...) {
		/* Oops I missed identifying this exception! */
		jclass jc = env->FindClass("java/lang/Error");
		if (jc) env->ThrowNew(jc, "Unidentified exception => "
			"Improve rethrow_cpp_exception_as_java_exception()");
	}
	return true;
}
JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_getResultsObjects
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jobjectArray jKeys, jlongArray jValues) {

	jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

	for (int i = 0; i < count; i++) {
		uint64_t value = 1000 + i;
		jobjectArray ret = env->NewObjectArray(1, gObject_class, NULL);
		jobject obj = env->CallStaticObjectMethod(gLong_class, gLong_valueOf_mid, *((uint64_t*)&value));
		env->SetObjectArrayElement(ret, 0, obj);
	
		env->SetObjectArrayElement(jKeys, i, ret);
		valuesArray[i] = value;
	}

	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

	return count;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_getResultsBytes
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first)
{
	std::vector<jbyte> vector;

	for (int i = 0; i < 8 * 2 * count; i++) {
		vector.push_back(i);
	}

	jbyteArray bArray = env->NewByteArray(vector.size());
	jbyte *jBytes = env->GetByteArrayElements(bArray, 0);
	for (int i = 0; i < 8 * 2 * count; i++) {
		jBytes[i] = vector[i];
	}
	env->ReleaseByteArrayElements(bArray, jBytes, 0);

	return bArray;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_headBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jbyteArray bytes, jint len) {
  return tailHeadlockArraySkipListMap(env, obj, indexId, startKey, count, first, false, bytes, len);
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_tailBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jbyteArray bytes, jint len) {
  return tailHeadlockArraySkipListMap(env, obj, indexId, startKey, count, first, true, bytes, len);
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_remove
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
	
	if (SB_DEBUG) {
		printf("remove begin\n");
		fflush(stdout);
	}

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return -1;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
	uint64_t retValue = -1;
    int partition = hashKey(env, map->dataTypes, key) % PARTITION_COUNT;
	MyValue *ret = map->maps[partition]->remove(key);
	if (ret != 0) {
		retValue = ret->value;
		map->valuePool->free(ret);
	}
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);
	
	if (SB_DEBUG) {
		printf("remove end\n");
		fflush(stdout);
	}

    return retValue;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_clear
  (JNIEnv *env, jobject obj, jlong indexId) {
 
	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return;
	}

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		map->maps[partition]->clear(); //todo: free keys
    }
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_delete
(JNIEnv *env, jobject obj, jlong indexId) {

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return;
	}

	Java_com_sonicbase_index_NativePartitionedSkipListMap_clear(env, obj, indexId);
	allMapsPartitionedSkipListMap[(int)indexId] = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		delete map->maps[partition];
    }
    delete map->keyPool;
    delete map->keyImplPool;
	delete map->valuePool;
	delete map;
}


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_get
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
	uint64_t offset = 0;

	if (SB_DEBUG) {
		printf("get begin\n");
		fflush(stdout);
	}

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

    int partition = abs(startKey->hashCode(map->dataTypes) % PARTITION_COUNT);

	uint64_t retValue = -1;
	MyValue *ret = map->maps[partition]->get(startKey);
	if (ret != 0) {
		retValue = ret->value;
	}
    deleteKey(env, map->dataTypes, startKey, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("get end\n");
		fflush(stdout);
	}
	return retValue;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("higherEntry begin\n");
		fflush(stdout);
	}
	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);


    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
			p = map->maps[partition]->higherEntry(key);
			if (lowest == 0) {
				lowest = p;
			}
			else if (p != 0 && map->comparator->compare(p->getKey(), lowest->getKey()) < 0) {
				lowest = p;
			}
		}
	}
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (lowest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest->getKey()));
    	valuesArray[0] = lowest->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

	   	return true;
	}

	if (SB_DEBUG) {
		printf("higherEntry end\n");
		fflush(stdout);
	}
	return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {
    
	if (SB_DEBUG) {
		printf("lowerEntry begin\n");
		fflush(stdout);
	}
	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *highest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
			p = map->maps[partition]->lowerEntry(key);

            if (highest == 0) {
            	highest = p;
            }
            else if (p != 0 && map->comparator->compare(p->getKey(), highest->getKey()) > 0) {
            	highest = p;
            }
        }
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (highest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, highest->getKey()));
		valuesArray[0] = highest->getValue()->value;

		env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		if (SB_DEBUG) {
			printf("lowerEntry end\n");
			fflush(stdout);
		}
		return true;
    }

	if (SB_DEBUG) {
		printf("lowerEntry end\n");
		fflush(stdout);
	}
	return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_floorEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("floorEntry end\n");
		fflush(stdout);
	}
	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *highest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {

		p = map->maps[partition]->floorEntry(key);

		if (highest == 0) {
			highest = p;
		}
		else if (p != 0) {
			if (map->comparator->compare(p->getKey(), highest->getKey()) > 0) {
				highest = p;
			}
		}
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (highest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, highest->getKey()));
		valuesArray[0] = highest->getValue()->value;

		env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		if (SB_DEBUG) {
			printf("floorEntry end\n");
			fflush(stdout);
		}

		return true;
    }

	if (SB_DEBUG) {
		printf("floorEntry end\n");
		fflush(stdout);
	}
	return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_ceilingEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("ceilingEntry begin\n");
		fflush(stdout);
	}

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);


    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
			p = map->maps[partition]->ceilingEntry(key);
            if (lowest == 0) {
            	lowest = p;
            }
            else if (p != 0 && map->comparator->compare(p->getKey(), lowest->getKey()) < 0) {
            	lowest = p;
            }
        }
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (lowest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest->getKey()));
		valuesArray[0] = lowest->getValue()->value;

		env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		if (SB_DEBUG) {
			printf("ceilingEntry end\n");
			fflush(stdout);
		}
		return true;
    }

	if (SB_DEBUG) {
		printf("ceilingEntry end\n");
		fflush(stdout);
	}
	return 0;
}


JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_lastEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {
    
	if (SB_DEBUG) {
		printf("lastEntry2 begin\n");
		fflush(stdout);
	}

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *highest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
			p = map->maps[partition]->lastEntry();

			if (highest == 0) {
				highest = p;
			}
			else if (p != 0 && map->comparator->compare(p->getKey(), highest->getKey()) > 0) {
				highest = p;
			}
        }
    }

    if (highest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, highest->getKey()));
		valuesArray[0] = highest->getValue()->value;

		env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		if (SB_DEBUG) {
			printf("lastEntry2 end\n");
			fflush(stdout);
		}
		return true;
    }

	if (SB_DEBUG) {
		printf("lastEntry2 end\n");
		fflush(stdout);
	}
	return 0;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_firstEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("firstEntry2 begin\n");
		fflush(stdout);
	}

	PartitionedConcurrentSkipListMap* map = getIndexSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *p = 0;
    Map<Key*,MyValue*>::Entry<Key*,MyValue*> *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
			p = map->maps[partition]->firstEntry();

			if (lowest == 0) {
				lowest = p;
			}
			else if (p != 0 && map->comparator->compare(p->getKey(), lowest->getKey()) < 0) {
				lowest = p;
			}
        }
    }

    if (lowest != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest->getKey()));
		valuesArray[0] = lowest->getValue()->value;

		env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		if (SB_DEBUG) {
			printf("firstEntry2 end\n");
			fflush(stdout);
		}
		return true;
    }

	if (SB_DEBUG) {
		printf("firstEntry2 end\n");
		fflush(stdout);
	}

   	return 0;
}

void binarySortSkipListMap(PartitionedConcurrentSkipListMap *map, Key **a, size_t lo, size_t hi, size_t start, bool ascend) {

	if (start == lo)
		start++;
	for (; start < hi; start++) {
		Key *pivot = a[start];

		// Set left (and right) to the index where a[start] (pivot) belongs
		int left = lo;
		int right = start;
		while (left < right) {
			int mid = (left + right) >> 1;
			if (ascend) {
				if (map->comparator->compare(pivot, a[mid]) < 0)
					right = mid;
				else
					left = mid + 1;
			}
			else {
				if (map->comparator->compare(pivot, a[mid]) > 0)
					right = mid;
				else
					left = mid + 1;
			}
		}
		int n = start - left;  // The number of elements to move
							   // Switch is just an optimization for arraycopy in default case
		switch (n) {
		case 2:  a[left + 2] = a[left + 1];
		case 1:  a[left + 1] = a[left];
			break;

		default: {
			for (int i = 0; i < n; i++) {
				a[left + 1 + n] = a[left + n];
			}
		}
		}
		a[left] = pivot;
	}
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedSkipListMap_sortKeys
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray keysObj, jboolean ascend) {

	PartitionedConcurrentSkipListMap *map = getIndexSkipListMap(env, indexId);

	size_t keyCount = env->GetArrayLength(keysObj);
	Key ** nativeKeys = new Key*[keyCount];
	for (int i = 0; i < keyCount; i++) {
		jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(keysObj, i);
		nativeKeys[i] = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);
	}

	binarySortSkipListMap(map, nativeKeys, 0, keyCount, 0, ascend);

	for (int i = 0; i < keyCount; i++) {
		env->SetObjectArrayElement(keysObj, i, nativeKeyToJavaKey(env, map->dataTypes, nativeKeys[i]));
	}
}


