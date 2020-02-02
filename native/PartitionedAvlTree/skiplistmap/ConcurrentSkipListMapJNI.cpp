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
#include "ObjectPool.h"

#include "com_sonicbase_index_NativeSkipListMap.h"

#define SB_DEBUG 0

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

PooledObjectPool *createKeyPool(JNIEnv * env, int *dataTypes, int fieldCount) {

	if (fieldCount > 1) {
		return new PooledObjectPool(new CompoundKey());
	}

	jint *types = dataTypes;

	int type = types[0];

	switch (type) {
	case ROWID:
	case BIGINT:
		return new PooledObjectPool(new LongKey());
	break;
	case TIME:
		return new PooledObjectPool(new TimeKey());
	break;
	case DATE:
		return new PooledObjectPool(new DateKey());
	break;
	case SMALLINT:
		return new PooledObjectPool(new ShortKey());
	break;
	case INTEGER:
		return new PooledObjectPool(new IntKey());
	break;
	case TINYINT:
		return new PooledObjectPool(new ByteKey());
	break;
	case TIMESTAMP:
		return new PooledObjectPool(new TimestampKey());
	break;
	case DOUBLE:
	case FLOAT:
		return new PooledObjectPool(new DoubleKey());
	break;
	case REAL:
		return new PooledObjectPool(new FloatKey());
	break;
	case NUMERIC:
	case DECIMAL:
		return new PooledObjectPool(new BigDecimalKey());
	break;
	case VARCHAR:
	case CHAR:
	case LONGVARCHAR:
	case NCHAR:
	case NVARCHAR:
	case LONGNVARCHAR:
	case NCLOB:
		return new PooledObjectPool(new StringKey());
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
	PooledObjectPool *keyPool;
	PooledObjectPool *keyImplPool;
	PooledObjectPool *valuePool;

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

		valuePool = new PooledObjectPool(new MyValue());

		env->ReleaseIntArrayElements(dataTypes, types, 0);

		keyImplPool = createKeyPool(env, types, fieldCount);
		keyPool = new PooledObjectPool(new Key());

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
		map = new ConcurrentSkipListMap <Key*, MyValue *>(keyPool, keyImplPool, valuePool);
		//printf("created map\n");
		//fflush(stdout);
    }

    SkipListMap(JNIEnv * env, long indexId, int *dataTypes, int fieldCount) {
    	this->id = indexId;
		this->dataTypes = dataTypes;
		comparator = new KeyComparator(env, indexId, fieldCount, this->dataTypes);

		valuePool = new PooledObjectPool(new MyValue());

		keyImplPool = createKeyPool(env, dataTypes, fieldCount);
		keyPool = new PooledObjectPool(new Key());

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
		map = new ConcurrentSkipListMap <Key*, MyValue *>(keyPool, keyImplPool, valuePool);
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


#include <execinfo.h>
void print_trace(void) {
    char **strings;
    size_t i, size;
    enum Constexpr { MAX_SIZE = 1024 };
    void *array[MAX_SIZE];
    size = backtrace(array, MAX_SIZE);
    strings = backtrace_symbols(array, size);
    for (i = 0; i < size; i++)
        printf("%s\n", strings[i]);
    puts("");
    free(strings);
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
		Key * key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);
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

			Key * key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);
			MyValue *v = map->map->put(key, new MyValue(values[i]));
			if (v != NULL) {
				delete v;
			}
			deleteKey(env, map->dataTypes, key);
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

	Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);
	MyValue *ret = map->map->get(startKey);

	deleteKey(env, map->dataTypes, startKey);

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

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
	MyValue *ret = map->map->remove(key);
	long retValue = -1;
	if (ret != 0) {
		retValue = ret->value;
		map->valuePool->free(ret);
	}
	deleteKey(env, map->dataTypes, key);

	if (SB_DEBUG) {
		printf("remove end\n");
		fflush(stdout);
	}

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

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

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

	deleteKey(env, map->dataTypes, key);

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

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

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
	deleteKey(env, map->dataTypes, key);

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

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->higherEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		//delete entry;

	   	return true;
	}

    deleteKey(env, map->dataTypes, key);

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

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->lowerEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;

	   	return true;
	}

    deleteKey(env, map->dataTypes, key);

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

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->floorEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;
	    deleteKey(env, map->dataTypes, key);

		if (SB_DEBUG) {
			printf("floorEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

    deleteKey(env, map->dataTypes, key);

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

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, map->comparator);

	Map<Key*,MyValue*>::Entry<Key*,MyValue*> *entry = map->map->ceilingEntry(key);
	if (entry != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, entry->getKey()));
    	valuesArray[0] = entry->getValue()->value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

		delete entry;
	    deleteKey(env, map->dataTypes, key);

		if (SB_DEBUG) {
			printf("ceilingEntry end\n");
			fflush(stdout);
		}

	   	return true;
	}

    deleteKey(env, map->dataTypes, key);

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
