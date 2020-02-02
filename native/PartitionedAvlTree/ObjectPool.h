#pragma once

#include "AtomicJava.h"
#include <atomic>
//#include <bits/stdc++.h>
#include <thread>
#include <random>
#include <typeinfo>

#define OBJECT_POOL_SIZE 250000

#define CORE_COUNT std::thread::hardware_concurrency()

class PoolableObject {
public:

	virtual PoolableObject *allocate(int count) = 0;

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) = 0;;

	virtual char *className() = 0;

};

#define FREE_POOL_SIZE 100000

class FreePool {
public:
	PoolableObject **freePool = new PoolableObject*[FREE_POOL_SIZE];
	int freeFreedOffset = 0;
	int freeAllocOffset = 0;

	FreePool() {
		memset(freePool, 0, FREE_POOL_SIZE * sizeof(PoolableObject*));
	 	freeFreedOffset = 0;
		freeAllocOffset = 0;
	}
};

class NonSafeObjectPool {
public:
	PoolableObject *obj;
	long currInnerPool;
	long innerPoolOffset;
	PoolableObject **innerPools = 0;
	std::vector<FreePool*> freePools;
	FreePool *currLastFreePool = 0;
	int freePoolsSize = 0;
public:

NonSafeObjectPool() {
	currLastFreePool = new FreePool();
	freePools.push_back(currLastFreePool);
	freePoolsSize++;
}

void init() {
	innerPools = new PoolableObject*[100000];
	innerPools[0] = obj->allocate(OBJECT_POOL_SIZE);
	currInnerPool = 0;
	innerPoolOffset = 0;
	//printf("created instance - class=%s\n", obj->className());
	//fflush(stdout);
}

PoolableObject *allocFromFree() {
	return 0;

	FreePool *freePool = freePools[0];
	if (freePool->freeAllocOffset >= freePool->freeFreedOffset) {
		if (freePool->freeAllocOffset >= FREE_POOL_SIZE) {
			freePools.erase(freePools.begin());
			freePoolsSize--;
			if (freePools.size() == 0) {
				currLastFreePool = new FreePool();
				freePools.push_back(currLastFreePool);
				freePoolsSize++;
			}
			freePool = freePools[0];
			if (freePool->freeAllocOffset >= freePool->freeFreedOffset) {
				return 0;
			}
		}
		else {
			return 0;
		}
	}

	PoolableObject *ret = freePool->freePool[freePool->freeAllocOffset++];
	if (ret == 0) {
		printf("returning null object\n");
		fflush(stdout);
	}
	return ret;
}

PoolableObject *allocate() {

	if (innerPools == 0) {
		init();
	}

	//printf("allocating node, currPool=%ld, innerOffset=%ld\n", currInnerPool, innerPoolOffset);
	//fflush(stdout);
	while (true) {
		long currPool = currInnerPool;
		long innerOffset = innerPoolOffset++;
		if (innerOffset < OBJECT_POOL_SIZE && currPool == currInnerPool) {
			PoolableObject *ret = obj->getObjectAtOffset(innerPools[currPool], innerOffset);
			//printf("returning pointer: %ld\n", (long)ret);
			//fflush(stdout);
			return ret;
		}
		{
			//printf("allocating new pool\n");
			//fflush(stdout);
			if (currPool == currInnerPool) {
				PoolableObject *inner = obj->allocate(OBJECT_POOL_SIZE);
				innerPools[currInnerPool + 1]  = inner;//innerPools.push_back(inner);
				currInnerPool++;
				innerPoolOffset = 0;
			}
		}
	}
}
	void free(PoolableObject *obj) {
		FreePool *freePool = currLastFreePool;
		if (freePool->freeFreedOffset >= FREE_POOL_SIZE) {
			freePool = new FreePool();
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

class PooledObjectPool {
	PoolableObject *obj;
	int poolCount = CORE_COUNT / 4;
	NonSafeObjectPool *pools = new NonSafeObjectPool[poolCount];
	std::atomic<int> *l = new std::atomic<int>[poolCount];
	std::atomic<int> *fl = new std::atomic<int>[poolCount];
	std::atomic<int> offset;
	std::atomic<int> freeFreeOffset;
	std::atomic<int> freeAllocOffset;

public:

	PooledObjectPool(PoolableObject *obj) {
		this->obj = obj;
		for (int i = 0; i < poolCount; i++) {
			pools[i].obj = obj;
		}
		offset.store(0);
		for (int i = 0; i < poolCount; i++) {
			l[i].store(0);
		}
	}

	PoolableObject *allocate() {
		while (false) {
			int o = abs(freeAllocOffset++) % poolCount;
			int lval = 0;
			if (fl[o].compare_exchange_strong(lval, 1)) {
				PoolableObject *ret = pools[o].allocFromFree();
				if (ret == 0) {
					fl[o].store(0);
					break;
				}
				fl[o].store(0);
				return ret;
			}
		}

		while (true) {
			int o = abs(offset++) % poolCount;
			int lval = 0;
			if (l[o].compare_exchange_strong(lval, 1)) {
				PoolableObject *ret = pools[o].allocate();
				l[o].store(0);
				return ret;
			}
		}
	}


	void free(PoolableObject *obj) {

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

