#include "stdafx.h"
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
#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
#include <iostream>


class MyValue : public Comparable<MyValue*> {
public:
	long value;
	MyValue() {

	}
	MyValue(long v) {
		value = v;
	}

	virtual bool equals(void *o) {
		return ((MyValue*)o)->value == value;
	}

	int compareTo(MyValue o) {
		if (o.value < value) {
			return -1;
		}
		if (o.value > value) {
			return 1;
		}
		return 0;
	}
	int compareTo(MyValue* o) {
		if (o->value < value) {
			return -1;
		}
		if (o->value > value) {
			return 1;
		}
		return 0;
	}

	virtual int hashCode() {
		return value;
	}


};
class MyKey : public Comparable<MyKey*> {
public:
	long value;
	MyKey() {

	}
	MyKey(long v) {
		value = v;
	}

	virtual bool equals(void *o) {
		return ((MyKey*)o)->value == value;
	}

	int compareTo(MyKey o) {
		if (o.value < value) {
			return 1;
		}
		if (value < o.value) {
			return -1;
		}
		return 0;
	}
	int compareTo(MyKey* o) {
		if (o->value < value) {
			return 1;
		}
		if (value < o->value) {
			return -1;
		}
		return 0;
	}

	virtual int hashCode() {
		return value;
	}


};

ConcurrentSkipListMap<MyKey*, MyValue*> *map = new ConcurrentSkipListMap < MyKey*, MyValue *>();

void foo1() {
	for (int i = 0; i < 100000; i++) {
//		MyValue *v = map->put(new MyKey(i % 500), new MyValue(i % 500));
		MyValue *v = map->put(new MyKey(i), new MyValue(i));
		if (v != NULL) {
			delete v;
		}
	}
}

void foo2() {
	for (int i = 199999; i >= 100000; i--) {
//		MyValue *v = map->put(new MyKey(i % 500 + 1), new MyValue(i % 500 + 1));
		MyValue *v = map->put(new MyKey(i), new MyValue(i));
		if (v != NULL) {
			delete v;
		}
	}
}

std::atomic<unsigned long> count;
long begin;

long currMillis() {
	unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return now;
}

void readThread() {
	while (true) {
		ConcurrentNavigableMap<MyKey*, MyValue*> *headMap = map->headMap(map->lastKey())->descendingMap();
		Set<Map<MyKey*, MyValue*>::Entry<MyKey*, MyValue*>*> *set = ((AbstractMap<MyKey*, MyValue*>*)headMap)->entrySet();
		Iterator<Map<MyKey*, MyValue*>::Entry<MyKey*, MyValue*>*> *i = set->beginIterator();
                typename Map<MyKey*,MyValue*>::template Entry<MyKey*,MyValue*> *entry = map->allocateEntry();
		while (i->hasNext()) {
			i->nextEntry(entry);
			long value = entry->getValue()->value;
			//		printf("%u\n", entry->getValue()->value);
			//delete entry;
			if (count++ % 1000000 == 0) {
				printf("progress count=%lu, rate=%f\n", count.load(), (float)((float)count.load() / (float)((currMillis() - begin)) * (float)1000));
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
		//MyKey *keys = new MyKey[2000];
		//MyValue *values = new MyValue[2000];
		for (long j = 0; j < 2000; j++) {
			//keys[j].value = i + j;
			//values[j].value = i + j;
		//	MyValue *v = map->put(&keys[j], &values[j]);
			MyValue *v = map->put(new MyKey(i + j), new MyValue(i + j));
			if (v != NULL) {
				delete v;
			}
			if (pcount++ % 1000000 == 0) {
				printf("put progress count=%lu, rate=%f\n", pcount.load(), (float)((float)pcount.load() / (float)((currMillis() - begin)) * (float)1000));
			}
		}
		if (pcount.load() > 50000000) {
			break;
		}
	}
}


class Test : public std::enable_shared_from_this<Test> {
public:
	void run() {
		//ConcurrentSkipListMap<long, long> *lmap = new ConcurrentSkipListMap <long, long>();


		begin = currMillis();

		std::thread **pthrd = new std::thread*[10];
		for (int i = 0; i < 10; i++) {
			pthrd[i] = new std::thread(insertForever, i);
		}

		for (int i = 0; i < 10; i++) {
			pthrd[i]->join();
		}

		//delete map;
		//printf("done");
/*
		std::thread th1(foo1);
		std::thread th2(foo2);

		th1.join();
		th2.join();
	*/
		//for (int i = 0; i < 1000; i++) {
		//	printf("out: %u", map->get(new MyKey(i))->value);
		//}
		/*
		Iterator<MyValue*> *iterator = map->valueIterator();
		while (iterator->hasNext()) {
			printf("%u\n", iterator->nextEntry()->value);
		}

		Iterator<MyKey*> *kiterator = map->keyIterator();
		while (iterator->hasNext()) {
			MyKey *entry = kiterator->nextEntry();
			printf("%u\n", entry->value);
		}
		Iterator<MyKey*> *kkiterator = map->tailMap(map->firstKey())->keySet()->beginIterator();
		while (kkiterator->hasNext()) {
			MyKey *entry = kkiterator->nextEntry();
			printf("%u\n", entry->value);
		}
		*/

		begin = currMillis();

		std::thread **thrd = new std::thread*[10];
		for (int i = 0; i < 10; i++) {
			thrd[i] = new std::thread(readThread);
		}

		for (int i = 0; i < 10; i++) {
			thrd[i]->join();
		}
		
		delete map;
	}
};
int main()
{

	Test t;
	t.run();

    return 0;
}

