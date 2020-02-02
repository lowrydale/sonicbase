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
#include "BigDecimal.h"
//#include <lzo/lzoconf.h>
//#include <lzo/lzo1x.h>
#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <list>
#include "ctpl_stl.h"

#include <jni.h>
#

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

long recCount;

static jmethodID gNativePartitionedTree_logError_mid;
static jclass gNativePartitioned_class;
static jclass gObject_class;

static jclass gLong_class;
static jmethodID gLong_longValue_mid;
static jmethodID gLong_valueOf_mid;

static jclass gShort_class;
static jmethodID gShort_shortValue_mid;
static jmethodID gShort_valueOf_mid;

static jclass gInt_class;
static jmethodID gInt_intValue_mid;
static jmethodID gInt_valueOf_mid;

static jclass gByte_class;
static jmethodID gByte_byteValue_mid;
static jmethodID gByte_valueOf_mid;

static jclass gBool_class;
static jmethodID gBool_boolValue_mid;
static jmethodID gBool_valueOf_mid;

static jclass gDouble_class;
static jmethodID gDouble_doubleValue_mid;
static jmethodID gDouble_valueOf_mid;

static jclass gFloat_class;
static jmethodID gFloat_floatValue_mid;
static jmethodID gFloat_valueOf_mid;

static jclass gTimestamp_class;
static jmethodID gTimestamp_ctor;
static jmethodID gTimestamp_setNanos_mid;
static jmethodID gTimestamp_getTime_mid;
static jmethodID gTimestamp_getNanos_mid;
static jmethodID gTimestamp_toString_mid;
static jmethodID gTimestamp_valueOf_mid;

static jclass gDate_class;
static jmethodID gDate_ctor;
static jmethodID gDate_getTime_mid;

static jclass gTime_class;
static jmethodID gTime_ctor;
static jmethodID gTime_getTime_mid;

static jclass gBigDecimal_class;
static jmethodID gBigDecimal_ctor;
static jmethodID gBigDecimal_toPlainString_mid;



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


class Key {
public:
	uint8_t len;
	void **key;
};

Key *javaKeyToNativeKey(JNIEnv *env, jobjectArray jKey) {
	int len = env->GetArrayLength(jKey);
	void **ret = new void*[len];
	for (int i = 0; i < len; i++) {
		jobject obj = env->GetObjectArrayElement(jKey, i);
		if (obj == 0) {
			ret[i] = 0;
			continue;
		}
		uint64_t l = (uint64_t)env->CallLongMethod(obj, gLong_longValue_mid);
		uint64_t *pl = new uint64_t();
		*pl = l;
		//printf("toNative: keyOffset=%d, long=%ld", i, l);
		//fflush(stdout);
		ret[i] = pl;
	}
	Key *key = new Key();
	key->len = len;
	key->key = ret;
    return key;
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3Ljava_lang_Object_2J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jlong value) {

		Key * key = javaKeyToNativeKey(env, jKey);
		MyValue *v = map->put(new MyKey(*(uint64_t*)key->key[0]), new MyValue(*(uint64_t*)key->key[0]));
		if (v != NULL) {
			delete v;
		}
		delete key;
		return -1; //todo: fix this
  }

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3_3Ljava_lang_Object_2_3J_3J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues, jlongArray jRetValues) {

		int len = env->GetArrayLength(jKeys);
		jlong *values = env->GetLongArrayElements(jValues, 0);
		jlong *retValues = env->GetLongArrayElements(jRetValues, 0);

		for (int i = 0; i < len; i++) {
			jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(jKeys, i);

			Key * key = javaKeyToNativeKey(env, jKey);
			MyValue *v = map->put(new MyKey(*(uint64_t*)key->key[0]), new MyValue(*(uint64_t*)key->key[0]));
			if (v != NULL) {
				delete v;
			}
			delete key;
		}

		env->ReleaseLongArrayElements(jValues, values, 0);
		env->ReleaseLongArrayElements(jRetValues, retValues, 0);
  }

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
				printf("read progress count=%lu, rate=%f\n", count.load(), (float)((float)count.load() / (float)((currMillis() - begin)) * (float)1000));
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
		if (pcount.load() > recCount) {
			break;
		}
	}
}


class Test : public std::enable_shared_from_this<Test> {
public:
	void run(int argc, char *argv[]) {
		//ConcurrentSkipListMap<long, long> *lmap = new ConcurrentSkipListMap <long, long>();
		if (argc < 3) {
			printf("missing parms: threadCount, recCount");
			return;
		}

		int threadCount = atoi(argv[1]);
		recCount = atol(argv[2]);

		begin = currMillis();

		std::thread **pthrd = new std::thread*[threadCount];
		for (int i = 0; i < threadCount; i++) {
			pthrd[i] = new std::thread(insertForever, i);
		}

		for (int i = 0; i < threadCount; i++) {
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
			printf("%u\n", entry->value);
		}
		*/

		printf("finished load\n");
		printf("starting read\n");
		fflush(stdout);

		begin = currMillis();

		std::thread **thrd = new std::thread*[threadCount];
		for (int i = 0; i < threadCount; i++) {
			thrd[i] = new std::thread(readThread);
		}

		for (int i = 0; i < threadCount; i++) {
			thrd[i]->join();
		}
		
		delete map;
	}
};
int main(int argc, char *argv[])
{

	Test t;
	t.run(argc, argv);

    return 0;
}

/**************************************************************
 * Declare JNI_VERSION for use in JNI_Onload/JNI_OnUnLoad
 * Change value if a Java upgrade requires it (prior: JNI_VERSION_1_6)
 **************************************************************/
static jint JNI_VERSION = JNI_VERSION_1_8;

/**************************************************************
 * Initialize the static Class and Method Id variables
 **************************************************************/
jint JNI_OnLoad(JavaVM* vm, void* reserved) {

	printf("onLoad");
	fflush(stdout);
    // Obtain the JNIEnv from the VM and confirm JNI_VERSION
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {

        return JNI_ERR;
    }

	/*
	* Step 1: initialize the LZO library
	*/
	/*int ret = lzo_init();
	if (ret != LZO_E_OK)
	{
		printf("internal error - lzo_init() failed !!!\n");
		printf("(this usually indicates a compiler bug - try recompiling\nwithout optimizations, and enable '-DLZO_DEBUG' for diagnostics)\n");
		return 4;
	}
	*/
	jclass cls = env->FindClass("com/sonicbase/index/NativePartitionedTree");
    gNativePartitioned_class = (jclass) env->NewGlobalRef(cls);
	gNativePartitionedTree_logError_mid = env->GetMethodID(cls, "logError", "(Ljava/lang/String;)V");
    env->DeleteLocalRef(cls);

	cls = env->FindClass("java/lang/Object");
    gObject_class = (jclass) env->NewGlobalRef(cls);
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Long");
    gLong_class = (jclass) env->NewGlobalRef(cls);
    gLong_longValue_mid = env->GetMethodID(cls, "longValue", "()J");
    gLong_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(J)Ljava/lang/Long;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Short");
    gShort_class = (jclass) env->NewGlobalRef(cls);
    gShort_shortValue_mid = env->GetMethodID(cls, "shortValue", "()S");
    gShort_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(S)Ljava/lang/Short;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Integer");
    gInt_class = (jclass) env->NewGlobalRef(cls);
    gInt_intValue_mid = env->GetMethodID(cls, "intValue", "()I");
    gInt_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(I)Ljava/lang/Integer;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Byte");
    gByte_class = (jclass) env->NewGlobalRef(cls);
    gByte_byteValue_mid = env->GetMethodID(cls, "byteValue", "()B");
    gByte_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(B)Ljava/lang/Byte;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Boolean");
    gBool_class = (jclass) env->NewGlobalRef(cls);
    gBool_boolValue_mid = env->GetMethodID(cls, "booleanValue", "()Z");
    gBool_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(Z)Ljava/lang/Boolean;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Double");
    gDouble_class = (jclass) env->NewGlobalRef(cls);
    gDouble_doubleValue_mid = env->GetMethodID(cls, "doubleValue", "()D");
    gDouble_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(D)Ljava/lang/Double;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/lang/Float");
    gFloat_class = (jclass) env->NewGlobalRef(cls);
    gFloat_floatValue_mid = env->GetMethodID(cls, "floatValue", "()F");
    gFloat_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(F)Ljava/lang/Float;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/sql/Timestamp");
    gTimestamp_class = (jclass) env->NewGlobalRef(cls);
	gTimestamp_ctor = env->GetMethodID(cls, "<init>", "(J)V");
    gTimestamp_setNanos_mid = env->GetMethodID(cls, "setNanos", "(I)V");
    gTimestamp_getTime_mid = env->GetMethodID(cls, "getTime", "()J");
    gTimestamp_getNanos_mid = env->GetMethodID(cls, "getNanos", "()I");
    gTimestamp_toString_mid = env->GetMethodID(cls, "toString", "()Ljava/lang/String;");
    gTimestamp_valueOf_mid = env->GetStaticMethodID(cls, "valueOf", "(Ljava/lang/String;)Ljava/sql/Timestamp;");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/sql/Date");
    gDate_class = (jclass) env->NewGlobalRef(cls);
	gDate_ctor = env->GetMethodID(cls, "<init>", "(J)V");
	gDate_getTime_mid = env->GetMethodID(cls, "getTime", "()J");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/sql/Time");
    gTime_class = (jclass) env->NewGlobalRef(cls);
	gTime_ctor = env->GetMethodID(cls, "<init>", "(J)V");
	gTime_getTime_mid = env->GetMethodID(cls, "getTime", "()J");
    env->DeleteLocalRef(cls);

    cls = env->FindClass("java/math/BigDecimal");
    gBigDecimal_class = (jclass) env->NewGlobalRef(cls);
	gBigDecimal_ctor = env->GetMethodID(cls, "<init>", "(Ljava/lang/String;)V");
	gBigDecimal_toPlainString_mid = env->GetMethodID(cls, "toPlainString", "()Ljava/lang/String;");
    env->DeleteLocalRef(cls);

    // Return the JNI Version as required by method
    return JNI_VERSION;
}

