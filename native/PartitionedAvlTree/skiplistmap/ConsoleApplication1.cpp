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
//#include "utf8.h"
//#include "BigDecimal.h"
//#include <lzo/lzoconf.h>
//#include <lzo/lzo1x.h>
#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <list>
//#include "ctpl_stl.h"

#include <jni.h>

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
#include "../sonicbase.h"

#include "com_sonicbase_index_NativeSkipListMap.h"

long recCount;


std::atomic<unsigned long> count;
long beginTime;

/*
KeyComparator *comparator = new KeyComparator(0, 0, 1, new int[1]{BIGINT});

ConcurrentSkipListMap<Key*, MyValue*> *map = new ConcurrentSkipListMap <Key*, MyValue *>();

long currMillis() {
	unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return now;
}

void readThread() {
	while (true) {
		ConcurrentNavigableMap<Key*, MyValue*> *headMap = map->headMap(map->lastKey())->descendingMap();
		Set<Map<Key*, MyValue*>::Entry<Key*, MyValue*>*> *set = ((AbstractMap<Key*, MyValue*>*)headMap)->entrySet();
		Iterator<Map<Key*, MyValue*>::Entry<Key*, MyValue*>*> *i = set->beginIterator();
                typename Map<Key*,MyValue*>::template Entry<Key*,MyValue*> *entry = map->allocateEntry();
		while (i->hasNext()) {
			i->nextEntry(entry);
			long value = entry->getValue()->value;
			//		printf("%u\n", entry->getValue()->value);
			//delete entry;
			if (count++ % 1000000 == 0) {
				printf("read progress count=%lu, rate=%f\n", count.load(), (float)((float)count.load() / (float)((currMillis() - beginTime)) * (float)1000));
			}
			
		}
		//delete set;
		//delete iterator;
		//delete headMap;
	}

}


std::atomic<unsigned long> pcount;

void insertForever(int offset) {
	for (long i = offset * 1000000000; ; i+=2000) {
		//Key *keys = new Key[2000];
		//MyValue *values = new MyValue[2000];
		for (long j = 0; j < 2000; j++) {
			//keys[j].value = i + j;
			//values[j].value = i + j;
		//	MyValue *v = map->put(&keys[j], &values[j]);
			Key *key = new LongKey(i + j);
			MyValue *v = map->put(key, new MyValue(i + j));
			if (v != NULL) {
				delete v;
			}
			if (pcount++ % 1000000 == 0) {
				printf("put progress count=%lu, rate=%f\n", pcount.load(), (float)((float)pcount.load() / (float)((currMillis() - beginTime)) * (float)1000));
			}
		}
		if (pcount.load() > recCount) {
			break;
		}
	}
}
*/

class Test : public std::enable_shared_from_this<Test> {
public:
	void run(int argc, char *argv[]) {
		//ConcurrentSkipListMap<long, long> *lmap = new ConcurrentSkipListMap <long, long>();
		if (argc < 3) {
			printf("missing parms: threadCount, recCount");
			return;
		}

/*
		int threadCount = atoi(argv[1]);
		recCount = atol(argv[2]);

		beginTime = currMillis();

		std::thread **pthrd = new std::thread*[threadCount];
		for (int i = 0; i < threadCount; i++) {
			pthrd[i] = new std::thread(insertForever, i);
		}

		for (int i = 0; i < threadCount; i++) {
			pthrd[i]->join();
		}
*/
		//delete map;
		//printf("done");
/*
		std::thread th1(foo1);
		std::thread th2(foo2);

		th1.join();
		th2.join();
	*/
		//for (int i = 0; i < 1000; i++) {
		//	printf("out: %u", map->get(new Key(i))->value);
		//}
		/*
		Iterator<MyValue*> *iterator = map->valueIterator();
		while (iterator->hasNext()) {
			printf("%u\n", iterator->nextEntry()->value);
		}

		Iterator<Key*> *kiterator = map->keyIterator();
		while (iterator->hasNext()) {
			Key *entry = kiterator->nextEntry();
			printf("%u\n", entry->value);
		}
		Iterator<Key*> *kkiterator = map->tailMap(map->firstKey())->keySet()->beginIterator();
		while (kkiterator->hasNext()) {
			printf("%u\n", entry->value);
		}
		*/

/*
		printf("finished load\n");
		printf("starting read\n");
		fflush(stdout);

		beginTime = currMillis();

		std::thread **thrd = new std::thread*[threadCount];
		for (int i = 0; i < threadCount; i++) {
			thrd[i] = new std::thread(readThread);
		}

		for (int i = 0; i < threadCount; i++) {
			thrd[i]->join();
		}
		
		delete map;
	*/
	}
};
int main2(int argc, char *argv[])
{

	Test t;
	t.run(argc, argv);

    return 0;
}

