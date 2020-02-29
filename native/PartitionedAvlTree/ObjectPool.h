#pragma once

#include "AtomicJava.h"
#include <atomic>
//#include <bits/stdc++.h>
#include <thread>
#include <random>
#include <cstring>
#include <typeinfo>

#define OBJECT_POOL_SIZE 50000

#define CORE_COUNT std::thread::hardware_concurrency()

template<typename V>
class PoolableObject {
public:

	virtual V allocate(int count) = 0;

	virtual V allocate() = 0;

	virtual V getObjectAtOffset(V array, int offset) = 0;;

	virtual const char *className() = 0;
};

#define FREE_POOL_SIZE 1000000

template<typename V>
class FreePool {
public:
	V *freePool = new V[FREE_POOL_SIZE];
	int freeFreedOffset = 0;
	int freeAllocOffset = 0;

	FreePool() {
		memset(freePool, 0, FREE_POOL_SIZE * sizeof(V));
	 	freeFreedOffset = 0;
		freeAllocOffset = 0;
	}
};

template<typename V>
class NonSafeObjectPool {
public:
	V obj;
	long currInnerPool;
	long innerPoolOffset;
	V *innerPools = 0;
	std::vector<FreePool<V>*> freePools;
	FreePool<V> *currLastFreePool = 0;
	int freePoolsSize = 0;
public:

NonSafeObjectPool() {
	currLastFreePool = new FreePool<V>();
	freePools.push_back(currLastFreePool);
	freePoolsSize++;
}

//~NonSafeObjectPool() {
//	for (int i = 0; i < innerPoolOffset; i++) {
//		delete innerPools[i];
//	}
//}

void init() {
	innerPools = new V[10000000];
	innerPools[0] = obj->allocate(OBJECT_POOL_SIZE);
	currInnerPool = 0;
	innerPoolOffset = 0;
	//printf("created instance - class=%s\n", obj->className());
	//fflush(stdout);
}

V allocFromFree() {

	FreePool<V> *freePool = freePools[0];
	if (freePool->freeAllocOffset >= freePool->freeFreedOffset) {
		if (freePool->freeAllocOffset >= FREE_POOL_SIZE) 
		{
			//printf("freeAlloc > SIZE\n");
			//fflush(stdout);
			freePools.erase(freePools.begin());
			freePoolsSize--;
			if (freePools.size() == 0) {
				currLastFreePool = new FreePool<V>();
				freePools.push_back(currLastFreePool);
				freePoolsSize++;
						//printf("allocating free pool\n");
                		//fflush(stdout);

			}
			freePool = freePools[0];
			if (freePool->freeAllocOffset >= freePool->freeFreedOffset) {
					//printf("freeAlloc > freedOffset 2\n");
            		//fflush(stdout);

				return 0;
			}
		}
		else {
			return 0;
		}
	}

	V ret = freePool->freePool[freePool->freeAllocOffset++];
	if (ret == 0) {
		printf("returning null object\n");
		fflush(stdout);
	}
	return ret;
}

V allocate() {
	if (innerPools == 0) {
		init();
	}

	//printf("allocating node, currPool=%ld, innerOffset=%ld\n", currInnerPool, innerPoolOffset);
	//fflush(stdout);
	while (true) {
		long currPool = currInnerPool;
		long innerOffset = innerPoolOffset++;
		if (innerOffset < OBJECT_POOL_SIZE && currPool == currInnerPool) {
			V ret = obj->getObjectAtOffset((V)innerPools[currPool], innerOffset);
			//printf("returning pointer: %ld\n", (long)ret);
			//fflush(stdout);
			return ret;
		}
		{
			//printf("allocating new pool\n");
			//fflush(stdout);
			if (currPool == currInnerPool) {
				V inner = obj->allocate(OBJECT_POOL_SIZE);
				innerPools[currInnerPool + 1]  = inner;//innerPools.push_back(inner);
				currInnerPool++;
				innerPoolOffset = 0;
			}
		}
	}
}
	void free(V obj) {

		FreePool<V> *freePool = currLastFreePool;
		if (freePool->freeFreedOffset >= FREE_POOL_SIZE) {
			freePool = new FreePool<V>();
			currLastFreePool = freePool;
			//printf("past end\n");
			//fflush(stdout);
			freePools.push_back(currLastFreePool);
			freePoolsSize++;
		}
		int ofst = freePool->freeFreedOffset;
		//printf("free - ofst=%d\n", ofst);
		//fflush(stdout);
		freePool->freePool[ofst] = obj;
		freePool->freeFreedOffset++;
	}

};

extern void print_trace(void);

template<typename V>
class PooledObjectPool {
	V obj;
	int poolCount = CORE_COUNT;
	NonSafeObjectPool<V> *pools = new NonSafeObjectPool<V>[poolCount];
	std::atomic<int> *l = new std::atomic<int>[poolCount];
	std::atomic<int> *fl = new std::atomic<int>[poolCount];
	std::atomic<int> offset;
	std::atomic<int> freeFreeOffset;
	std::atomic<int> freeAllocOffset;
	std::mutex **freeMutexes = new std::mutex*[poolCount];

public:

	PooledObjectPool(V obj) {
		this->obj = obj;
		for (int i = 0; i < poolCount; i++) {
			pools[i].obj = obj;
			freeMutexes[i] = new std::mutex();
		}
		offset.store(0);
		for (int i = 0; i < poolCount; i++) {
			l[i].store(0);
			fl[i].store(0);
		}
	}

//	~PooledObjectPool() {
//		delete pools;
//	}

	V allocate() {
		return obj->allocate();

		while (true) {
			int o = abs(freeAllocOffset++) % poolCount;
			int lval = 0;
			if (fl[o].compare_exchange_strong(lval, 1)) {
				V ret = pools[o].allocFromFree();
				if (ret == 0) {
					fl[o].store(0);
					break;
				}
				//printf("alloc from free %lu\n", (unsigned long)ret);
				//fflush(stdout);

				fl[o].store(0);
				return ret;
			}
		}

		while (true) {
			int o = abs(offset++) % poolCount;
			int lval = 0;
			if (l[o].compare_exchange_strong(lval, 1)) {
				V ret = pools[o].allocate();
				l[o].store(0);
				return ret;
			}
		}
	}


	void free(V obj) {
		delete obj;
		return;
		if (obj == 0) {
			//printf("freeing null object\n");
			//fflush(stdout);
			//print_trace();
			return;
		}
		//if (0 != strcmp(obj->className(), this->obj->className())) {
		//	printf("freeing wrong class type, expected=%s, actual=%ld\n", this->obj->className(), obj);
		//	fflush(stdout);
		//	print_trace();
		//	return;
		//}
			//printf("freeing actual object: class=%s\n", obj->className());
			//fflush(stdout);

			//printf("free %lu\n", (unsigned long)obj);
			//fflush(stdout);
		while (true) {
			int o = abs(freeFreeOffset++) % poolCount;
			int lval = 0;
			
			if (fl[o].compare_exchange_strong(lval, 1)) {
				pools[o].free(obj);
				fl[o].store(0);
				return;
			}
			
		}

	}

};

