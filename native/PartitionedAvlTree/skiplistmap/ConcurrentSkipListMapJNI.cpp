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

#include "com_sonicbase_index_NativeSkipListMap.h"

#define SB_DEBUG 0
#define concurentThreadsSupported std::thread::hardware_concurrency()

//template <class K, class V>
//thread_local NonSafeObjectPool<typename ConcurrentSkipListMap<K,V>::template Node<K, V>>
//	ConcurrentSkipListMap<K,V>::nodePool;// = new NonSafeObjectPool<ConcurrentSkipListMap<K,V>::Node<K,V>();

MyValue::MyValue() {

}

MyValue::MyValue(long v) {
	value = v;
}

bool MyValue::equals(void *o) {
	return ((MyValue*)o)->value == value;
}

int MyValue::compareTo(MyValue o) {
	if (o.value < value) {
		return -1;
	}
	if (o.value > value) {
		return 1;
	}
	return 0;
}

int MyValue::compareTo(MyValue* o) {
	if (o->value < value) {
		return -1;
	}
	if (o->value > value) {
		return 1;
	}
	return 0;
}

int MyValue::hashCode() {
	return value;
}

std::atomic<unsigned long> nodeQCount;

void incrementNodeQCount() {
	nodeQCount++;
}

void decrementNodeQCount() {
	nodeQCount--;
}

std::atomic<unsigned long> nodeCount;

void incrementNodeCount() {
	nodeCount++;
}

void decrementNodeCount() {
	nodeCount--;
}

std::atomic<unsigned long> indexCount;

void incrementIndexCount() {
	indexCount++;
}

void decrementIndexCount() {
	indexCount--;
}

std::atomic<unsigned long> indexQCount;

void incrementIndexQCount() {
	indexQCount++;
}

void decrementIndexQCount() {
	indexQCount--;
}

PooledObjectPool<KeyImpl*> *createKeyPool(JNIEnv * env, int *dataTypes, int fieldCount) {

	if (fieldCount > 1) {
		return new PooledObjectPool<KeyImpl*>(new CompoundKey());
	}

	jint *types = (jint*)dataTypes;

	int type = types[0];

	switch (type) {
	case ROWID:
	case BIGINT:
		return new PooledObjectPool<KeyImpl*>(new LongKey());
	break;
	case TIME:
		return new PooledObjectPool<KeyImpl*>(new TimeKey());
	break;
	case DATE:
		return new PooledObjectPool<KeyImpl*>(new DateKey());
	break;
	case SMALLINT:
		return new PooledObjectPool<KeyImpl*>(new ShortKey());
	break;
	case INTEGER:
		return new PooledObjectPool<KeyImpl*>(new IntKey());
	break;
	case TINYINT:
		return new PooledObjectPool<KeyImpl*>(new ByteKey());
	break;
	case TIMESTAMP:
		return new PooledObjectPool<KeyImpl*>(new TimestampKey());
	break;
	case DOUBLE:
	case FLOAT:
		return new PooledObjectPool<KeyImpl*>(new DoubleKey());
	break;
	case REAL:
		return new PooledObjectPool<KeyImpl*>(new FloatKey());
	break;
	case NUMERIC:
	case DECIMAL:
		return new PooledObjectPool<KeyImpl*>(new BigDecimalKey());
	break;
	case VARCHAR:
	case CHAR:
	case LONGVARCHAR:
	case NCHAR:
	case NVARCHAR:
	case LONGNVARCHAR:
	case NCLOB:
		return new PooledObjectPool<KeyImpl*>(new StringKey());
	break;

	default:
	{
		//char *buffer = new char[75];
		printf("Unsupported datatype in serializeKeyValue: type=%d", type);
		//logError(env, buffer);
		//delete[] buffer;
	}

	}

}

class SkipListMap {
public:
    long id = 0;
    KeyComparator *comparator = 0;
	int *dataTypes = 0;
	int fieldCount = 0;
	ConcurrentSkipListMap<Key*, MyValue*> *map;
	PooledObjectPool<Key*> *keyPool;
	PooledObjectPool<KeyImpl*> *keyImplPool;
	PooledObjectPool<MyValue*> *valuePool;

	~SkipListMap() {
		delete comparator;
		delete[] dataTypes;
	}

    SkipListMap(JNIEnv * env, long indexId, jintArray dataTypes) {
    	this->id = indexId;
		fieldCount = env->GetArrayLength(dataTypes);
		jint *types = env->GetIntArrayElements(dataTypes, 0);
		this->dataTypes = new int[fieldCount];
		memcpy( (void*)this->dataTypes, (void*)types, fieldCount * sizeof(int));
		comparator = new KeyComparator(env, indexId, fieldCount, this->dataTypes);

		valuePool = new PooledObjectPool<MyValue*>(new MyValue());

		env->ReleaseIntArrayElements(dataTypes, types, 0);

		keyImplPool = createKeyPool(env, this->dataTypes, fieldCount);
		keyPool = new PooledObjectPool<Key*>(new Key());

/*
		for (int i = 0; i < 100; i++) {
			MyValue *key = (MyValue*)valuePool->allocate();
			printf("creating key: class=%s\n", key->className());
			fflush(stdout);
		}

		for (int i = 0; i < 100; i++) {
			LongKey *key = (LongKey*)keyImplPool->allocate();
			printf("creating key: class=%s\n", key->className());
			fflush(stdout);
		}
*/

		//printf("creating map\n");
		//fflush(stdout);
		map = new ConcurrentSkipListMap <Key*, MyValue *>(this, keyPool, keyImplPool, valuePool, this->dataTypes);
		//printf("created map\n");
		//fflush(stdout);
    }

    SkipListMap(JNIEnv * env, long indexId, int *dataTypes, int fieldCount) {
    	this->id = indexId;
		this->dataTypes = dataTypes;
		comparator = new KeyComparator(env, indexId, fieldCount, this->dataTypes);

		valuePool = new PooledObjectPool<MyValue*>(new MyValue());

		keyImplPool = createKeyPool(env, dataTypes, fieldCount);
		keyPool = new PooledObjectPool<Key*>(new Key());

/*
		for (int i = 0; i < 100; i++) {
			MyValue *key = (MyValue*)valuePool->allocate();
			printf("creating key: class=%s\n", key->className());
			fflush(stdout);
		}

		for (int i = 0; i < 100; i++) {
			LongKey *key = (LongKey*)keyImplPool->allocate();
			printf("creating key: class=%s\n", key->className());
			fflush(stdout);
		}
*/

		//printf("creating map\n");
		//fflush(stdout);
		map = new ConcurrentSkipListMap <Key*, MyValue *>(this, keyPool, keyImplPool, valuePool, this->dataTypes);
		//printf("created map\n");
		//fflush(stdout);
    }
};

//thread_local NonSafeObjectPool<Key> SkipListMap::keyPool;// = new NonSafeObjectPool<ConcurrentSkipListMap<K,V>::Node<K,V>();
//thread_local NonSafeObjectPool<MyValue> SkipListMap::valuePool;// = new NonSafeObjectPool<ConcurrentSkipListMap<K,V>::Node<K,V>();


std::mutex partitionIdLockSkipListMap;

SkipListMap **allMapsSkipListMap = new SkipListMap*[10000];
uint64_t highestMapIdSkipListMap = 0;

SkipListMap *getSkipListMap(JNIEnv *env, jlong indexId) {
	SkipListMap* map = allMapsSkipListMap[(int)indexId];
	if (map == 0) {
		throwException(env, "Invalid index id");
	}
	return map;
}



DeleteQueue *nodeDeleteQueue = new DeleteQueue();

std::thread **deleteThreads;


void nodeDeleteRunner() {
	printf("started nodeDelete runner");
	fflush(stdout);
	while (true) {
		try {
			DeleteQueueEntry *entry = nodeDeleteQueue->pop();
			if (entry == 0) {
				//			printf("popped nothing");
				//			fflush(stdout);
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
				//printf("############ continue\n");
				//fflush(stdout);
				continue;
			}
			//		printf("############# deleting");
			//		fflush(stdout);
				 //printf("deleting node\n	");
				 //fflush(stdout);
			if (entry->type == TYPE_INDEX) {
				((SkipListMap*)entry->map)->map->indexPool->free((ConcurrentSkipListMap<Key*, MyValue*>::Index<Key*, MyValue*>*)entry->value);
			}
			else if (entry->type == TYPE_REF) {
				ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* node = (ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>*)entry->value;
				ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* next = node->atomicNext.get();
				
				if (node->atomicNext.compareAndSet(next, 0) && next != 0) {
					next->deleteRef(entry->map, ((SkipListMap*)entry->map)->map->nodePool);
				}
				node->deleteRef(entry->map, ((SkipListMap*)entry->map)->map->nodePool);
			}
			else {
				ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* node = (ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>*)entry->value;
				if (node->key != 0) {
					//((SkipListMap*)entry->map)->keyImplPool->free(node->key->key);
					//((SkipListMap*)entry->map)->keyPool->free(node->key);
				}
				ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* next = node->atomicNext.get();
				if (next != 0) {
					//delete marker
					//if (next->atomicValue.get() == next) {
						//printf("deleting marker\n	");
						//fflush(stdout);
						   //
						//if (next->key == 0) {
			//				pushNodeDelete(entry->map, next);
						//}
						//((SkipListMap*)entry->map)->map->nodePool->free(next);
					//}
				}
				if (node->refCount.load() == 0)
				{
					ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* next = node->atomicNext.get();
					if (node->atomicNext.compareAndSet(next, 0) &&
							next != 0) {
						//printf("delete ref 2\n");
//						if (next->refCount.load() > 1) {
//							printf("greater\n");
//						}
					//printf("delref %lu, value=%lu, next=%lu\n", next->refCount.load(), (unsigned long)next->atomicValue.get(), (unsigned long)next->atomicNext.get());
						next->deleteRef(entry->map, ((SkipListMap*)entry->map)->map->nodePool);
					}
					if (node->key != 0) {
						((SkipListMap*)entry->map)->keyImplPool->free(node->key->key);
						((SkipListMap*)entry->map)->keyPool->free(node->key);
					}
					node->refCount = 99999999;
					((SkipListMap*)entry->map)->map->nodePool->free(node);
				}
			}
			if (entry->type == TYPE_NODE) {
				delete (DeleteQueueEntry*)entry;
			}
			else {
				delete (DeleteQueueEntry*)entry;
			}
		}
		catch (const std::exception& e) {
			printf("exp");
			fflush(stdout);
		}
	}
}

std::mutex nodeDeleteThreadMutex;

#define DELETE_THREAD_COUNT concurentThreadsSupported * 4

void initDeleteThreads() {
	if (deleteThreads == 0) {
		std::lock_guard<std::mutex> lock(nodeDeleteThreadMutex);
		if (deleteThreads == 0) {
			deleteThreads = (std::thread**)malloc(DELETE_THREAD_COUNT * sizeof(std::thread*));
			for (int i = 0; i < DELETE_THREAD_COUNT; i++) {
				deleteThreads[i] = new std::thread(nodeDeleteRunner);
			}
		}
	}
}

void pushNodeDelete(void *map, void *value) {
	initDeleteThreads();

	DeleteQueueEntry *entry = new DeleteQueueEntry();
	entry->map = map;
	entry->type = TYPE_NODE;
	entry->value = value;
	nodeDeleteQueue->push(entry);
}

void pushRefDelete(void *map, void *value) {
	initDeleteThreads();

	DeleteQueueEntry *entry = new DeleteQueueEntry();
	entry->map = map;
	entry->type = TYPE_REF;
	entry->value = value;
	nodeDeleteQueue->push(entry);
}


DeleteQueue *indexDeleteQueue = new DeleteQueue();

std::thread *indexDeleteThread;


void indexDeleteRunner() {
	printf("started indexDelete runner");
	fflush(stdout);
	while (true) {
		DeleteQueueEntry *entry = indexDeleteQueue->pop();
		if (entry == 0) {
			//			printf("popped nothing");
			//			fflush(stdout);
			std::this_thread::sleep_for(std::chrono::milliseconds(10000));
			//printf("############ continue\n");
			//fflush(stdout);
			continue;
		}
		//		printf("############# deleting");
		//		fflush(stdout);
		//printf("deleting node\n	");
		//fflush(stdout);
		
		//delete entry->value;
		//((SkipListMap*)entry->map)->map->indexPool->free(entry->value);

		/*
		if (entry->value->key != 0) {
			((SkipListMap*)entry->map)->keyImplPool->free(entry->value->key->key);
			((SkipListMap*)entry->map)->keyPool->free(entry->value->key);
		}
		ConcurrentSkipListMap<Key*, MyValue*>::Node<Key*, MyValue*>* next = entry->value->atomicNext.get();
		if (next != 0) {
			//delete marker
			if (next->key == 0) {
				//printf("deleting marker\n	");
				//fflush(stdout);
				//
				pushNodeDelete(entry->map, next);

				//((SkipListMap*)entry->map)->map->nodePool->free(next);
			}
		}
		((SkipListMap*)entry->map)->map->nodePool->free(entry->value);
		*/
		delete entry;
	}
}

std::mutex indexDeleteThreadMutex;

void pushIndexDelete(void *map, void *value) {
	initDeleteThreads();

	DeleteQueueEntry *entry = new DeleteQueueEntry();
	entry->map = map;
	entry->type = TYPE_INDEX;
	entry->value = value;
	nodeDeleteQueue->push((DeleteQueueEntry*)entry);
}


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3Ljava_lang_Object_2J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jlong value) {

	if (SB_DEBUG) {
		printf("put begin\n");
		fflush(stdout);
	}
		SkipListMap* map = getSkipListMap(env, indexId);
		if (map == 0) {
			return 0;
		}

		//printf("before key\n");
		//fflush(stdout);
		Key * key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);
		//printf("after key\n");
		//fflush(stdout);
		//printf("before value\n");
		//fflush(stdout);
		MyValue *vvalue = (MyValue*)map->valuePool->allocate();
		//printf("after value1, class=%s\n", ((PoolableObject*)vvalue)->className());
		//fflush(stdout);
		vvalue->value = value;
		//printf("after value2\n");
		//fflush(stdout);

		MyValue *v =
		map->map->put(key, vvalue);
		//printf("after put\n");
		//fflush(stdout);

		if (v != NULL) {
			long value = v->value;
			map->valuePool->free(v);
			return value;
		}

	if (SB_DEBUG) {
		printf("put end\n");
		fflush(stdout);
  }
		return -1;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3_3Ljava_lang_Object_2_3J_3J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues, jlongArray jRetValues) {

	if (SB_DEBUG) {
		printf("batch put begin\n");
		fflush(stdout);
	}

		SkipListMap* map = getSkipListMap(env, indexId);
		if (map == 0) {
			return;
		}

		int len = env->GetArrayLength(jKeys);
		jlong *values = env->GetLongArrayElements(jValues, 0);
		jlong *retValues = env->GetLongArrayElements(jRetValues, 0);

		for (int i = 0; i < len; i++) {
			jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(jKeys, i);

			Key * key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);
			MyValue *v = map->map->put(key, new MyValue(values[i]));
			if (v != NULL) {
				map->valuePool->free(v);
			}
		}

		env->ReleaseLongArrayElements(jValues, values, 0);
		env->ReleaseLongArrayElements(jRetValues, retValues, 0);

  	if (SB_DEBUG) {
  		printf("batch put end\n");
  		fflush(stdout);
  	}

  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    get
 * Signature: (J[Ljava/lang/Object;)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_get
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {

	if (SB_DEBUG) {
		printf("get begin\n");
		fflush(stdout);
	}

  	SkipListMap* map = getSkipListMap(env, indexId);
  	if (map == 0) {
  		return 0;
  	}

		//printf("javakey begin\n");
		//fflush(stdout);
	Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);
		//printf("get\n");
		//fflush(stdout);
	MyValue *ret = map->map->get(startKey);
		//printf("deletetkey begin\n");
		//fflush(stdout);

	deleteKey(env, map->dataTypes, startKey, map->keyPool, map->keyImplPool);
		//printf("finish\n");
		//fflush(stdout);


	if (SB_DEBUG) {
		printf("get end\n");
		fflush(stdout);
	}

	if (ret == 0) {
		return -1;
	}
	return ret->value;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    remove
 * Signature: (J[Ljava/lang/Object;)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_remove
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {

	if (SB_DEBUG) {
		printf("remove begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
	//printf("delete %ld\n", ((LongKey*)key->key)->longKey);
	//fflush(stdout);
	MyValue *ret = map->map->remove(key);
	//printf("deleted\n");
	//fflush(stdout);
	long retValue = -1;
	if (ret != 0) {
		retValue = ret->value;
		map->valuePool->free(ret);
	}
	//printf("deleting key\n");
	//fflush(stdout);
	deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("remove end\n");
		fflush(stdout);
	}
	//printf("deleted key\n");
	//fflush(stdout);

    return retValue;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_clear
  (JNIEnv *env, jobject obj, jlong indexId) {

	if (SB_DEBUG) {
		printf("clear begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return;
	}

	map->map->clear(); //todo: free keys

	//SkipListMap *newMap = new SkipListMap(env, indexId, map->dataTypes, map->fieldCount);
	//allMapsSkipListMap[indexId] = newMap;

	if (SB_DEBUG) {
		printf("clear end\n");
		fflush(stdout);
	}
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    getResultsBytes
 * Signature: (J[Ljava/lang/Object;IZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativeSkipListMap_getResultsBytes
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean) {
  return 0;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    getResultsObjects
 * Signature: (J[Ljava/lang/Object;IZ[[Ljava/lang/Object;[J)I
 */
JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativeSkipListMap_getResultsObjects
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean, jobjectArray, jlongArray) {
  return 0;
  }


bool processResults(JNIEnv *env, SkipListMap *map, int retCount, Key **keys, uint64_t* values, jbyteArray bytes, jint len) {
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

	env->ReleaseByteArrayElements(bytes, rawjBytes, 0);

	return fit;
}
/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    tailBlockArray
 * Signature: (J[Ljava/lang/Object;IZ[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_tailBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jint count, jboolean first, jbyteArray bytes,
  jint len) {

  	if (SB_DEBUG) {
  		printf("tailBlock begin\n");
  		fflush(stdout);
  	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	if (key == 0) {
		printf("tailBlockArray null key\n");
		fflush(stdout);
	}
	//else {
	//	printf("tailBlockArray key=%llu\n", *(uint64_t*)key->key[0]);
	//	fflush(stdout);
	//}

  	Key **keys = new Key*[count];
  	for (int i = 0; i < count; i++) {
  		keys[i] = 0;
  	}
  	uint64_t* values = new uint64_t[count];
  	int retCount = 0;
  	bool firstEntry = true;

	//printf("before get tail map\n");
	//fflush(stdout);

	ConcurrentNavigableMap<Key*, MyValue*> *tailMap = map->map->tailMap(key);

	if (tailMap != 0) {
		//printf("got tail map\n");
		//fflush(stdout);

		Iterator<Map<Key*, MyValue*>::Entry<Key*, MyValue*>*> *i = ((ConcurrentSkipListMap<Key*, MyValue*>::SubMap<Key*, MyValue*>*)tailMap)->entryIterator();
		if (i != 0) {
			//printf("got iterator\n");
			//fflush(stdout);

			typename Map<Key*,MyValue*>::template Entry<Key*,MyValue*> *entry = map->map->allocateEntry();

			//printf("after get tail map\n");
			//fflush(stdout);

			while (retCount < count && i->hasNext()) {
				i->nextEntry(entry);
				long value = entry->getValue()->value;
				Key *currKey = entry->getKey();

				keys[retCount] = currKey;
				values[retCount] = value;

				//printf("got entry\n");
				//fflush(stdout);


				retCount++;
				if (firstEntry && (!first /*|| !tail*/)) {
					if (keys[0]->compareTo(key) == 0) {
						retCount = 0;
					}
					firstEntry = false;
				}
			}

			delete entry;
		}
	}

	//printf("before process results\n");
	//fflush(stdout);

	jboolean fit = processResults(env, map, retCount, keys, values, bytes, len);

	//printf("after process results\n");
	//fflush(stdout);

	delete[] keys;
	delete[] values;

	deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	//printf("after delete key\n");
	//fflush(stdout);

	if (SB_DEBUG) {
		printf("tail block end\n");
		fflush(stdout);
	}

	return fit;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    headBlockArray
 * Signature: (J[Ljava/lang/Object;IZ[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_headBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jint count, jboolean first, jbyteArray bytes, jint len) {

	if (SB_DEBUG) {
		printf("head block begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	if (key == 0) {
		printf("tailBlockArray null key");
		fflush(stdout);
	}

  	Key **keys = new Key*[count];
  	for (int i = 0; i < count; i++) {
  		keys[i] = 0;
  	}
  	uint64_t* values = new uint64_t[count];
  	int retCount = 0;
  	bool firstEntry = true;

	ConcurrentNavigableMap<Key*, MyValue*> *headMap = map->map->headMap(key)->descendingMap();

	if (headMap != 0) {
		//printf("got tail map\n");
		//fflush(stdout);

		Iterator<Map<Key*, MyValue*>::Entry<Key*, MyValue*>*> *i = ((ConcurrentSkipListMap<Key*, MyValue*>::SubMap<Key*, MyValue*>*)headMap)->entryIterator();
		if (i != 0) {
			typename Map<Key*,MyValue*>::template Entry<Key*,MyValue*> *entry = map->map->allocateEntry();

			while (retCount < count && i->hasNext()) {
				i->nextEntry(entry);
				long value = entry->getValue()->value;
				Key *currKey = entry->getKey();

				keys[retCount] = currKey;
				values[retCount] = value;

				retCount++;
				if (firstEntry && (!first /*|| !tail*/)) {
					if (keys[0]->compareTo(key) == 0) {
						retCount = 0;
					}
					firstEntry = false;
				}
			}
			delete entry;
		}
	}

	jboolean fit = processResults(env, map, retCount, keys, values, bytes, len);

	delete[] keys;
	delete[] values;
	deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("head block end\n");
		fflush(stdout);
	}

	return fit;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    higherEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("higheEntry begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->higherEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		//delete entry;

	   	return true;
	}

    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("higherEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    lowerEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("lowerEntry begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->lowerEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;

	   	return true;
	}

    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("lowerEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    floorEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_floorEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("floorEntry begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->floorEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;
	    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

		if (SB_DEBUG) {
			printf("floorEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("floorEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    ceilingEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_ceilingEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("ceilingEntry begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator, map->comparator->fieldCount);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->ceilingEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;
	    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

		if (SB_DEBUG) {
			printf("ceilingEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("ceilingEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    lastEntry2
 * Signature: (J[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_lastEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("lastEntry begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->lastEntry();
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;

		if (SB_DEBUG) {
			printf("lastEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

	if (SB_DEBUG) {
		printf("lastEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    firstEntry2
 * Signature: (J[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_firstEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("firstEntry end\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return 0;
	}

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->firstEntry();
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		//delete entry;

		if (SB_DEBUG) {
			printf("firstEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

	if (SB_DEBUG) {
		printf("firstEntry end\n");
		fflush(stdout);
	}

	return false;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    delete
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_delete
  (JNIEnv *env, jobject obj, jlong indexId) {

	if (SB_DEBUG) {
		printf("delete begin\n");
		fflush(stdout);
	}

	SkipListMap* map = getSkipListMap(env, indexId);
	if (map == 0) {
		return;
	}

	Java_com_sonicbase_index_NativeSkipListMap_clear(env, obj, indexId);
	allMapsSkipListMap[(int)indexId] = 0;

//    delete map->map;
  //  delete map->keyPool;
    //delete map->keyImplPool;
	//delete map->valuePool;

	if (SB_DEBUG) {
		printf("delete end\n");
		fflush(stdout);
	}

	delete map;
  }

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    sortKeys
 * Signature: (J[[Ljava/lang/Object;Z)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_sortKeys
  (JNIEnv *, jobject, jlong, jobjectArray, jboolean) {
  return;
  }


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_initIndexSkipListMap
  (JNIEnv * env, jobject obj, jintArray dataTypes) {

  	if (SB_DEBUG) {
  		printf("initIndexSkipListMap end\n");
  		fflush(stdout);
  	}

	  {
		  std::lock_guard<std::mutex> lock(partitionIdLockSkipListMap);

		  if (highestMapIdSkipListMap == 0) {
			 for (int i = 0; i < 10000; i++) {
				 allMapsSkipListMap[i] = 0;
			}
		  }

		  uint64_t currMapId = highestMapIdSkipListMap;
		  SkipListMap *newMap = new SkipListMap(env, currMapId, dataTypes);
		  allMapsSkipListMap[highestMapIdSkipListMap] = newMap;
		  highestMapIdSkipListMap++;
		  return currMapId;
	  }

	if (SB_DEBUG) {
		printf("initIndexSkipListMap end\n");
		fflush(stdout);
	}

}




SkipListMap *map;

long currMillis() {
	unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return now;
}

void readThread() {
	/*
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
	*/
}


std::atomic<unsigned long> pcount;
long recCount = 0;
long beginTime = 0;

void insertForever(int offset) {

	int partCount = 0;
	for (long i = offset * 100000000; ; i += 2001) {
		//Key *keys = new Key[2000];
		//MyValue *values = new MyValue[2000];
		for (long j = 0; j < 2001; j++) {
			//keys[j].value = i + j;
			//values[j].value = i + j;
			//	MyValue *v = map->put(&keys[j], &values[j]);

			partCount++;
			int count = pcount++;

			Key *key = (Key*)map->keyPool->allocate();//new Key();
			key->key = (KeyImpl*)map->keyImplPool->allocate();
			((LongKey*)key->key)->longKey = i + j;
			MyValue *value = (MyValue*)map->valuePool->allocate();
			value->value = i + j;
			//printf("%ld\n", i);
			//fflush(stdout);
			MyValue *v = map->map->put(key, value);
			if (v != NULL) {
				//delete v;
			}
			if (count % 100000 == 0) {
				printf("put progress count=%lu, rate=%f, nodeCount=%lu, indexCount=%lu, queue=%lu\n", pcount.load(),
				(float)((float)pcount.load() / (float)((currMillis() - beginTime)) * (float)1000),
				nodeCount.load(), indexCount.load(), nodeQCount.load());
			}
		}
		if (pcount.load() > recCount) {
			break;
		}
	}
}

void deleteForever(int offset) {

	int partCount = 0;
	for (long i = offset * 100000000; ; i += 2001) {
		//Key *keys = new Key[2000];
		//MyValue *values = new MyValue[2000];
		for (long j = 0; j < 2001; j++) {
			//keys[j].value = i + j;
			//values[j].value = i + j;
			//	MyValue *v = map->put(&keys[j], &values[j]);

			partCount++;
			int count = pcount++;

			Key *key = (Key*)map->keyPool->allocate();//new Key();
			key->key = (KeyImpl*)map->keyImplPool->allocate();
			((LongKey*)key->key)->longKey = i + j;
			//printf("%ld\n", i);
			//fflush(stdout);
			MyValue *v = map->map->remove(key);
			if (v != NULL) {
				map->valuePool->free(v);
			}
			map->keyImplPool->free(key->key);
			map->keyPool->free(key);
			if (count % 100000 == 0) {
				printf("delete progress count=%lu, rate=%f, nodeCount=%lu, indexCount==%lu, queue=%lu\n", pcount.load(),
				(float)((float)pcount.load() / (float)((currMillis() - beginTime)) * (float)1000),
				nodeCount.load(), indexCount.load(), nodeQCount.load());
			}
		}
		if (pcount.load() > recCount) {
			break;
		}
	}
}

void doInsert(int threadCount) {
	beginTime = currMillis();
	pcount = 0;

	std::thread **pthrd = new std::thread*[threadCount];
	for (int i = 0; i < threadCount; i++) {
		pthrd[i] = new std::thread(insertForever, i);
	}

	for (int i = 0; i < threadCount; i++) {
		pthrd[i]->join();
	}
}

void doDelete(int threadCount) {
	beginTime = currMillis();
	pcount = 0;

	std::thread **pthrd = new std::thread*[threadCount];
	for (int i = 0; i < threadCount; i++) {
		pthrd[i] = new std::thread(deleteForever, i);
	}

	for (int i = 0; i < threadCount; i++) {
		pthrd[i]->join();
	}
}

class Test : public std::enable_shared_from_this<Test> {
public:
	void run(int argc, char *argv[]) {
		//ConcurrentSkipListMap<long, long> *lmap = new ConcurrentSkipListMap <long, long>();
		if (argc < 2) {
			printf("missing parms: threadCount, recCount");
			return;
		}

		int threadCount = atoi(argv[1]);
		recCount = atol(argv[2]);

		int *dataTypes = new int[1]{ BIGINT };

		map = new SkipListMap(0, 0, new int[1]{ BIGINT }, 1);


		doInsert(threadCount);

		for (int i = 0; i < 100; i++) {
			//recCount = 20000000;;
			doDelete(threadCount);


			//recCount = 20000000;;
			doInsert(threadCount);
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
int main(int argc, char *argv[])
{

	Test t;
	t.run(argc, argv);

	return 0;
}

