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
#include "com_sonicbase_index_NativePartitionedTree.h"
#include "sonicbase.h"
#include "ObjectPool.h"


//#include <pthread.h>

#ifdef _WIN32
//#include <WinSock2.h>
//#include <Windows.h>
#else
#include <sys/time.h>
#include<pthread.h>
#include<semaphore.h>
#endif
#include "kavl.h"

#define SB_DEBUG 0


//extern "C" {void *__dso_handle = NULL; } extern "C" {void *__cxa_atexit = NULL; }

#define concurentThreadsSupported std::thread::hardware_concurrency()
#define PARTITION_COUNT 64
//736
//(concurentThreadsSupported * 8)
 // 64 for 17mil tailBlock reads/sec

//const int NUM_RECORDS_PER_PARTITION =  32 + 2;
//const int BLOCK_SIZE = (PARTITION_COUNT * NUM_RECORDS_PER_PARTITION);


jmethodID gNativePartitionedTree_logError_mid;
jclass gNativePartitioned_class;
jclass gObject_class;

jclass gLong_class;
jmethodID gLong_longValue_mid;
jmethodID gLong_valueOf_mid;

jclass gShort_class;
jmethodID gShort_shortValue_mid;
jmethodID gShort_valueOf_mid;

jclass gInt_class;
jmethodID gInt_intValue_mid;
jmethodID gInt_valueOf_mid;

jclass gByte_class;
jmethodID gByte_byteValue_mid;
jmethodID gByte_valueOf_mid;

jclass gBool_class;
jmethodID gBool_boolValue_mid;
jmethodID gBool_valueOf_mid;

jclass gDouble_class;
jmethodID gDouble_doubleValue_mid;
jmethodID gDouble_valueOf_mid;

jclass gFloat_class;
jmethodID gFloat_floatValue_mid;
jmethodID gFloat_valueOf_mid;

jclass gTimestamp_class;
jmethodID gTimestamp_ctor;
jmethodID gTimestamp_setNanos_mid;
jmethodID gTimestamp_getTime_mid;
jmethodID gTimestamp_getNanos_mid;
jmethodID gTimestamp_toString_mid;
jmethodID gTimestamp_valueOf_mid;

jclass gDate_class;
jmethodID gDate_ctor;
jmethodID gDate_getTime_mid;

jclass gTime_class;
jmethodID gTime_ctor;
jmethodID gTime_getTime_mid;

jclass gBigDecimal_class;
jmethodID gBigDecimal_ctor;
jmethodID gBigDecimal_toPlainString_mid;

bool shutdownFlag = false;

//ctpl::thread_pool threadPool(concurentThreadsSupported);

void logError(JNIEnv *env, char msg[], std::exception e) {
	const char *what = e.what();
    std::string str = std::string(msg) + std::string(what);
//	const char *buffer = new char[strlen(msg) + strlen(what) + 1];
//	sprintf(buffer, "%s exception=%s", msg, what);
	jstring jMsg = env->NewStringUTF(str.c_str());
	env->CallVoidMethod(gNativePartitioned_class, gNativePartitionedTree_logError_mid, jMsg);
}

void logError(JNIEnv *env, char msg[]) {
	jstring jMsg = env->NewStringUTF(msg);
	env->CallVoidMethod(gNativePartitioned_class, gNativePartitionedTree_logError_mid, jMsg);
}

#define synchronized(m) \
    for(std::unique_lock<std::recursive_mutex> lk(m); lk; lk.unlock())



long readLong(std::vector<jbyte> &vector, int offset) {
	long result = 0;
	for (int i = 0; i < 8; i++) {
		result <<= 8;
		result |= (vector[i + offset] & 0xFF);
	}
	return result;
}

jboolean writeLong(long long int value, jbyte *bytes, int *offset, int len) {
	if (offset[0] + 8 > len) {
		return false;
	}
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 56));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 48));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 40));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 32));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 24));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 16));
	bytes[offset[0]++] = (jbyte)(0xff & (value >> 8));
	bytes[offset[0]++] = (jbyte)(0xff & value);
	return true;
}

jboolean writeInt(int value, jbyte *p, int *offset, int len) {
	if (offset[0] + 4 > len) {
		return false;
	}
	p[offset[0]++] = (0xff & (value >> 24));
	p[offset[0]++] = (0xff & (value >> 16));
	p[offset[0]++] = (0xff & (value >> 8));
	p[offset[0]++] = (0xff & value);
	return true;
}

jboolean writeShort(int value, jbyte *p, int *offset, int len) {
	if (offset[0] + 2 > len) {
		return false;
	}
	p[offset[0]++] = (0xff & (value >> 8));
	p[offset[0]++] = (0xff & value);
	return true;
}

long long int readUnsignedVarLong(std::vector<jbyte> &vector, int *offset) {
	long long int tmp;
	// CHECKSTYLE: stop InnerAssignment
	if ((tmp = vector[offset[0]++]) >= 0) {
		return tmp;
	}
	long long int result = tmp & 0x7f;
	if ((tmp = vector[offset[0]++]) >= 0) {
		result |= tmp << 7;
	}
	else {
		result |= (tmp & 0x7f) << 7;
		if ((tmp = vector[offset[0]++]) >= 0) {
			result |= tmp << 14;
		}
		else {
			result |= (tmp & 0x7f) << 14;
			if ((tmp = vector[offset[0]++]) >= 0) {
				result |= tmp << 21;
			}
			else {
				result |= (tmp & 0x7f) << 21;
				if ((tmp = vector[offset[0]++]) >= 0) {
					result |= tmp << 28;
				}
				else {
					result |= (tmp & 0x7f) << 28;
					if ((tmp = vector[offset[0]++]) >= 0) {
						result |= tmp << 35;
					}
					else {
						result |= (tmp & 0x7f) << 35;
						if ((tmp = vector[offset[0]++]) >= 0) {
							result |= tmp << 42;
						}
						else {
							result |= (tmp & 0x7f) << 42;
							if ((tmp = vector[offset[0]++]) >= 0) {
								result |= tmp << 49;
							}
							else {
								result |= (tmp & 0x7f) << 49;
								if ((tmp = vector[offset[0]++]) >= 0) {
									result |= tmp << 56;
								}
								else {
									result |= (tmp & 0x7f) << 56;
									result |= ((long long int)vector[offset[0]++]) << 63;
								}
							}
						}
					}
				}
			}
		}
	}
	return result;
}

long long int readVarLong(std::vector<jbyte> &vector, int *offset) {
	long long int raw = readUnsignedVarLong(vector, offset);
	long long int temp = (((raw << 63) >> 63) ^ raw) >> 1;
	return temp ^ (raw & (1L << 63));
}


void writeVarLong(
	long long int value,
	jbyte *bytes,
	int *offset
)
{
	value = (value << 1);
	long long int tmp = (value >> 63);
	value = value ^ tmp;
	while (true) {
		int bits = ((int)value) & 0x7f;
		value = static_cast<unsigned long long int>(value) >> 7;
		if (value == 0) {
			bytes[offset[0]++] = (jbyte)bits;
			return;
		}
		bytes[offset[0]++] = (jbyte)(bits | 0x80);
	}
}


/* These are parameters to deflateInit2. See
http://zlib.net/manual.html for the exact meanings. */

/* portability layer */
static const char *progname = NULL;
#define WANT_LZO_MALLOC 1
#define WANT_XMALLOC 1
//#include "examples/portab.h"


/* We want to compress the data block at 'in' with length 'IN_LEN' to
* the block at 'out'. Because the input block may be incompressible,
* we must provide a little more output space in case that compression
* is not possible.
*/

#ifndef IN_LEN
#define IN_LEN      (128*1024L)
#endif
#define OUT_LEN     (IN_LEN + IN_LEN / 16 + 64 + 3)

//#include <lzo/lzoconf.h>
//#include <lzo/lzo1x.h>
//#define LZO_OS_WIN64 true
//#define __WIN64__
//#define LZO_DEBUG

/*
#include "minilzo.h"
*/

/*
lzo_bytep allocOut(lzo_uint inLen, lzo_uint outLen) {
	outLen = inLen + inLen / 16 + 64 + 3;
	return new lzo_byte[outLen];
}

lzo_bytep lzoDecompress(lzo_bytep in, lzo_uint in_len, lzo_bytep out, lzo_uint &new_len) {
	
	new_len = in_len;
	int r = lzo1x_decompress(in, in_len, out, &new_len, NULL);
	if (r != LZO_E_OK)
	{ 
		printf("internal error - decompression failed: %d\n", r);
		return 0;
	}

	return out;
}

lzo_bytep lzoCompress(lzo_bytep in, lzo_uint in_len, lzo_bytep out, lzo_uint out_len, lzo_uint &new_len) {

	int r;
	lzo_voidp wrkmem;

	wrkmem = (lzo_voidp)malloc(LZO1X_999_MEM_COMPRESS);
	if (in == NULL || out == NULL || wrkmem == NULL)
	{
		printf("out of memory\n");
		return 0;
	}

	r = lzo1x_999_compress(in, in_len, out, &new_len, wrkmem);
	if (r != LZO_E_OK)
	{
		printf("internal error - compression failed: %d\n", r);
		return 0;
	}

	//writeInt(in_len, out, 0);

	free(wrkmem);
	
	return out;
}
*/

KeyComparator::KeyComparator(JNIEnv *env, long indexId, int fieldCount, int *dataTypes) {
	this->fieldCount = fieldCount;
	comparators = new KAVLComparator*[fieldCount];
	//printf("init comparator: indexId=%ld, fieldCount=%d\n", indexId, fieldCount);
	//fflush(stdout);
	for (int i = 0; i < fieldCount; i++) {
		switch (dataTypes[i]) {
			case BIGINT:
			case ROWID:
				comparators[i] = new LongComparator();
				//printf("init comparator: field=BIGINT\n");
				//fflush(stdout);
				break;
			case SMALLINT:
				comparators[i] = new ShortComparator();
				break;
			case TINYINT:
				comparators[i] = new ByteComparator();
				break;
			case BOOLEAN:
			case BIT:
				comparators[i] = new BooleanComparator();
				break;
			case INTEGER:
				comparators[i] = new IntComparator();
				break;
			case NUMERIC:
			case DECIMAL:
				comparators[i] = new BigDecimalComparator();
				break;
			case DATE:
			case TIME:
				comparators[i] = new LongComparator();
				break;
			case TIMESTAMP:
				comparators[i] = new TimestampComparator();
				break;
			case DOUBLE:
			case FLOAT:
				comparators[i] = new DoubleComparator();
				break;
			case REAL:
				comparators[i] = new FloatComparator();
				break;
			case VARCHAR:
			case CHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
				comparators[i] = new Utf8Comparator();
				break;
			default:
				//char *buffer = new char[75];
				printf("Unsupported datatype in KeyComparator: type=%d", dataTypes[i]);
				//logError(env, buffer);
				//delete[] buffer;
				break;
		}
	}
}

int KeyComparator::compare(void *o1, void *o2) {
	return ((Key*)o1)->compareTo((Key*)o2);
}


jint throwException(JNIEnv *env, const char *message)
{
	jclass exClass;
	const char *className = "com/sonicbase/query/DatabaseException";

	exClass = env->FindClass(className);
	if (exClass == NULL) {
		printf("class not found: com/sonicbase/query/DatabaseException");
		fflush(stdout);
	}

	printf("got exception\n");
	fflush(stdout);

	jboolean flag = env->ExceptionCheck();
	if (flag) {
		env->ExceptionClear();
		printf("cleared exception\n");
		fflush(stdout);
	}

	jint ret = env->ThrowNew(exClass, message);
	printf("throw exception: ret=%d\n", ret);
	fflush(stdout);
	return ret;
}

class spinlock_t {
    std::atomic_flag lock_flag;
public:
    spinlock_t() { lock_flag.clear(); }

    bool try_lock() { return !lock_flag.test_and_set(std::memory_order_acquire); }
    void lock() { for (size_t i = 0; !try_lock(); ++i) if (i % 100 == 0) std::this_thread::yield(); }
    void unlock() { lock_flag.clear(std::memory_order_release); }
};

class spinlock_guard {
	spinlock_t *lock;
public:

	spinlock_guard(spinlock_t &lock) {
		this->lock = &lock;
	}
	~spinlock_guard() {
		this->lock->unlock();
	}
};

class PartitionedMap {
public:
    long id = 0;
    my_node * maps[PARTITION_COUNT];
    std::mutex *locks = new std::mutex[PARTITION_COUNT];
    spinlock_t *slocks = new spinlock_t[PARTITION_COUNT];
    KAVLComparator *comparator = 0;
    PooledObjectPool<Key*> *keyPool;
    PooledObjectPool<KeyImpl*> *keyImplPool;
	int *dataTypes = 0;
	int fieldCount = 0;

	~PartitionedMap() {
		delete[] locks;
		delete[] slocks;
		delete comparator;
		delete[] dataTypes;
	}

    PartitionedMap(JNIEnv * env, long indexId, jintArray dataTypes) {
    	id = indexId;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            maps[i] = 0;
        }

		fieldCount = env->GetArrayLength(dataTypes);
		jint *types = env->GetIntArrayElements(dataTypes, 0);
		this->dataTypes = new int[fieldCount];
		memcpy( (void*)this->dataTypes, (void*)types, fieldCount * sizeof(int));
		comparator = new KeyComparator(env, indexId, fieldCount, this->dataTypes);

        keyImplPool = createKeyPool(env, (int*)types, fieldCount);
		keyPool = new PooledObjectPool<Key*>(new Key());

		env->ReleaseIntArrayElements(dataTypes, types, 0);
    }

	PartitionedMap(long indexId, int* dataTypes, int fieldCount) {
		id = indexId;
		for (int i = 0; i < PARTITION_COUNT; i++) {
			maps[i] = 0;
		}

		this->dataTypes = new int[fieldCount];
		memcpy((void*)this->dataTypes, (void*)dataTypes, fieldCount * sizeof(int));
		comparator = new KeyComparator(NULL, indexId, fieldCount, this->dataTypes);
	}
};




void deleteKey(JNIEnv *env, int *dataTypes, Key *key, PooledObjectPool<Key*> *keyPool, PooledObjectPool<KeyImpl*> *keyImplPool) {

	key->key->free(dataTypes);
	keyImplPool->free(key->key);
	keyPool->free(key);
}




uint64_t getCurrMillis() {
#ifdef _WIN32
	SYSTEMTIME time;
	GetSystemTime(&time);
	return (time.wSecond * 1000) + time.wMilliseconds;
#else
	struct timeval tp;
	gettimeofday(&tp, NULL);
	return (uint64_t)tp.tv_sec * 1000L + tp.tv_usec / 1000;
#endif
}


DeleteQueue *deleteQueue = new DeleteQueue();

std::thread *deleteThread;


void deleteRunner() {
	printf("started runner");
	fflush(stdout);
	while (!shutdownFlag) {
		DeleteQueueEntry *entry = deleteQueue->pop();
		if (entry == 0) {
			//			printf("popped nothing");
			//			fflush(stdout);
			std::this_thread::sleep_for(std::chrono::milliseconds(10000));
			continue;
		}
		//		printf("############# deleting");
		//		fflush(stdout);
		//deleteKey(0, ((PartitionedMap*)entry->map)->dataTypes, entry->value,
		//	((PartitionedMap*)entry->map)->keyPool, ((PartitionedMap*)entry->map)->keyImplPool);
		delete entry;
	}
}

void pushDelete(PartitionedMap *map, Key *key) {
	if (deleteThread == 0) {
		printf("creating thread");
		fflush(stdout);
		deleteThread = new std::thread(deleteRunner);
		deleteThread->detach();
		printf("created thread");
		fflush(stdout);
	}
	DeleteQueueEntry *entry = new DeleteQueueEntry();
	entry->map = map;
	entry->value = key;
	deleteQueue->push(entry);
}


class MyFreeNode : public FreeNode {
public:
	void free(void *map, my_node *p) {
		pushDelete((PartitionedMap *)map, p->key);
		delete p;
	}
};

FreeNode *freeNode = new MyFreeNode();

//float timedifference_msec(struct timeval t0, struct timeval t1)
//{
//    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
//}


static uint64_t EXP_BIT_MASK = 9218868437227405312L;
static uint64_t SIGNIF_BIT_MASK = 4503599627370495L;

static inline uint64_t doubleToRawLongBits(double x) {
	uint64_t bits;
	memcpy(&bits, &x, sizeof bits);
	return bits;
}

uint64_t doubleToLongBits(double value) {
	uint64_t result = doubleToRawLongBits(value);
	// Check for NaN based on values of bit fields, maximum
	// exponent and nonzero significand.
	if (((result & EXP_BIT_MASK) ==
		EXP_BIT_MASK) &&
		(result & SIGNIF_BIT_MASK) != 0L)
		result = 0x7ff8000000000000L;
	return result;
}

unsigned float_to_bits(float x)
{
	unsigned y;
	memcpy(&y, &x, 4);
	return y;
}

uint32_t hashKey(JNIEnv* env, int* dataTypes, Key *key) {
	return key->hashCode(dataTypes);
}

/*
std::string byte_seq_to_string( uint8_t bytes[], int n)
{
    std::ostringstream stm ;

    for( std::size_t i = 0 ; i < n ; i++ )
        stm << std::setw(2) << std::setfill( '0' ) << bytes[i];

    return stm.str() ;
}
*/
/*
std::string byte_seq_to_string_orig( const unsigned char bytes[], std::size_t n )
{
    std::ostringstream stm ;
    stm << std::hex << std::uppercase ;

    for( std::size_t i = 0 ; i < n ; ++i )
        stm << std::setw(2) << std::setfill( '0' ) << unsigned( bytes[i] ) ;

    return stm.str() ;
}
*/


Key *javaKeyToNativeKey(JNIEnv *env, PooledObjectPool<Key*> *keyPool, PooledObjectPool<KeyImpl*> *keyImplPool,
	int *dataTypes, jobjectArray jKey, KeyComparator *comparator, int fieldCount) {

	int len = env->GetArrayLength(jKey);

	if (fieldCount == 1) {
		int type = dataTypes[0];
		jobject obj = env->GetObjectArrayElement(jKey, 0);
		switch (type) {
			case ROWID:
			case BIGINT:
			{
				jlong l = env->CallLongMethod(obj, gLong_longValue_mid);
				LongKey *key = (LongKey*)keyImplPool->allocate();
				key->longKey = l;
				void *obj = keyPool->allocate();
				Key* ret = static_cast<Key*>(obj);
				ret->key = key;
				return ret;
			}
			break;
			case SMALLINT:
			{
				jshort l = env->CallShortMethod(obj, gShort_shortValue_mid);
				ShortKey *key = (ShortKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->shortKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case INTEGER:
			{
				jint l = env->CallIntMethod(obj, gInt_intValue_mid);
				IntKey *key = (IntKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->intKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case TINYINT:
			{
				jbyte l = env->CallByteMethod(obj, gByte_byteValue_mid);
				ByteKey *key = (ByteKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->byteKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case BOOLEAN:
			case BIT:
			{
				jboolean l = env->CallBooleanMethod(obj, gBool_boolValue_mid);
				ByteKey *key = (ByteKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->byteKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case DECIMAL:
			case NUMERIC:
			{
				jstring str = (jstring)env->CallObjectMethod(obj, gBigDecimal_toPlainString_mid);
				const char *nativeString = env->GetStringUTFChars(str, 0);
				std::string *std = new std::string(nativeString);
                env->ReleaseStringUTFChars(str, nativeString);

				BigDecimalKey *key = (BigDecimalKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->bigDecimalKey = std;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
				break;
			case DATE:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gDate_getTime_mid);
				DateKey *key = (DateKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->longKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case TIME:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gTime_getTime_mid);
				TimeKey *key = (TimeKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->longKey = l;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case TIMESTAMP:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gTimestamp_getTime_mid);
				int iVal = (int)env->CallIntMethod(obj, gTimestamp_getNanos_mid);
				TimestampKey *key = (TimestampKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->timestampKey = new jlong[2];
				key->timestampKey[0] = l;
				key->timestampKey[1] = iVal;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case FLOAT:
			case DOUBLE:
			{
				double d = env->CallDoubleMethod(obj, gDouble_doubleValue_mid);
				DoubleKey *key = (DoubleKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->doubleKey = d;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case REAL:
			{
				float f = env->CallFloatMethod(obj, gFloat_floatValue_mid);
				FloatKey *key = (FloatKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;
				key->floatKey = f;
				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;
			}
			break;
			case VARCHAR:
			case CHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
			{
				jsize size = env->GetArrayLength((jcharArray)obj);
				uint16_t *bytes = (uint16_t*)env->GetCharArrayElements((jcharArray)obj, NULL);
				uint16_t *copy = new uint16_t[size + 1];
				for (int j = 0; j < size; j++) {
					copy[j] = (uint16_t)bytes[j];
				}
				copy[size] = 0;
			    env->ReleaseCharArrayElements((jcharArray)obj, (jchar*)bytes, 0);
				StringKey *key = (StringKey*)keyImplPool->allocate();//new Key(0, 1, comparator);;

			    key->stringKey.bytes = copy;
			    key->stringKey.len = size;

				Key* ret = (Key*)keyPool->allocate();
				ret->key = key;
				return ret;

			}
			break;
			default:
			{
				//char *buffer = new char[75];
				printf("Unsupported datatype in javaKeyToNativeKey: type=%d", type);
				//logError(env, buffer);
				//delete[] buffer;
			}
		}
	}

	void **ret = new void*[len];
	for (int i = 0; i < len; i++) {
		int type = dataTypes[i];
		jobject obj = env->GetObjectArrayElement(jKey, i);
		if (obj == 0) {
			ret[i] = 0;
			continue;
		}
		switch (type) {
			case ROWID:
			case BIGINT:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gLong_longValue_mid);
				uint64_t *pl = new uint64_t();
				*pl = l;
				//printf("toNative: keyOffset=%d, long=%ld\n", i, l);
				//fflush(stdout);
				ret[i] = pl;
			}
			break;
			case SMALLINT:
			{
				short l = env->CallShortMethod(obj, gShort_shortValue_mid);
				short *p = new short();
				*p = l;
				ret[i] = p;
			}
			break;
			case INTEGER:
			{
				int l = env->CallIntMethod(obj, gInt_intValue_mid);
				int *p = new int();
				*p = l;
				ret[i] = p;
			}
			break;
			case TINYINT:
			{
				uint8_t l = env->CallByteMethod(obj, gByte_byteValue_mid);
				uint8_t *p = new uint8_t();
				*p = l;
				ret[i] = p;
			}
			break;
			case BOOLEAN:
			case BIT:
			{
				bool l = env->CallBooleanMethod(obj, gBool_boolValue_mid);
				bool *p = new bool();
				*p = l;
				ret[i] = p;
			}
			break;
			case DECIMAL:
			case NUMERIC:
			{
				jstring str = (jstring)env->CallObjectMethod(obj, gBigDecimal_toPlainString_mid);

				const char *nativeString = env->GetStringUTFChars(str, 0);
				std::string *std = new std::string(nativeString);
                env->ReleaseStringUTFChars(str, nativeString);

				//printf("key=%s, len=%d\n", std->c_str(), (int)std->length());
				//fflush(stdout);
				ret[i] = std;
			}
				break;
			case DATE:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gDate_getTime_mid);
				uint64_t *pl = new uint64_t();
				*pl = l;
				ret[i] = pl;
			}
			break;
			case TIME:
			{
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gTime_getTime_mid);
				uint64_t *pl = new uint64_t();
				*pl = l;
				ret[i] = pl;
			}
			break;
			case TIMESTAMP:
			{/*
				jclass cls = env->GetObjectClass(obj);

				// First get the class object
				jmethodID mid = env->GetMethodID(cls, "getClass", "()Ljava/lang/Class;");
				jobject clsObj = env->CallObjectMethod(obj, mid);

				// Now get the class object's class descriptor
				cls = env->GetObjectClass(clsObj);

				// Find the getName() method on the class object
				mid = env->GetMethodID(cls, "getName", "()Ljava/lang/String;");

				// Call the getName() to get a jstring object back
				jstring strObj = (jstring)env->CallObjectMethod(clsObj, mid);

				// Now get the c string from the java jstring object
				const char* str = env->GetStringUTFChars(strObj, NULL);

				// Print the class name
				printf("\nCalling class is: %s\n", str);
				fflush(stdout);
				// Release the memory pinned char array
				env->ReleaseStringUTFChars(strObj, str);
*/
				uint64_t l = (uint64_t)env->CallLongMethod(obj, gTimestamp_getTime_mid);
				int iVal = (int)env->CallIntMethod(obj, gTimestamp_getNanos_mid);
				uint64_t *entry = new uint64_t[2];
				entry[0] = l;
				entry[1] = (uint64_t)iVal;
				ret[i] = (void*)entry;
			}
			break;
			case FLOAT:
			case DOUBLE:
			{
				double d = env->CallDoubleMethod(obj, gDouble_doubleValue_mid);
				double *p = new double(d);
				ret[i] = (void*)p;
			}
			break;
			case REAL:
			{
				float f = env->CallFloatMethod(obj, gFloat_floatValue_mid);
				float *p = new float(f);
				ret[i] = p;
			}
			break;
			case VARCHAR:
			case CHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
			{

				//printf("toNative - start string\n");
				//fflush(stdout);
				jsize size = env->GetArrayLength((jcharArray)obj);
				uint16_t *bytes = (uint16_t*)env->GetCharArrayElements((jcharArray)obj, NULL);
				uint16_t *copy = new uint16_t[size + 1];
				for (int j = 0; j < size; j++) {
					copy[j] = (uint16_t)bytes[j];
				}
				copy[size] = 0;
			    env->ReleaseCharArrayElements((jcharArray)obj, (jchar*)bytes, 0);
			    sb_utf8str *str = new sb_utf8str();
			    str->bytes = copy;
			    str->len = size;
			    ret[i] = (void*)str;

				//printf("toNative: keyOffset=%d, str=%s\n", i, str->bytes);
				//fflush(stdout);

			}
			break;
			default:
			{
				//char *buffer = new char[75];
				printf("Unsupported datatype in javaKeyToNativeKey: type=%d", type);
				//logError(env, buffer);
				//delete[] buffer;
			}
		}
	}
	CompoundKey *key = new CompoundKey(ret, len, comparator);
	Key* r = (Key*)keyPool->allocate();
	r->key = key;
	return r;
}

jobjectArray nativeKeyToJavaKey(JNIEnv *env, int *dataTypes, Key *key) {

	return key->toJavaKey(env, dataTypes);

}

jboolean serializeKeyValue(JNIEnv *env, int* dataTypes, jbyte *bytes, int len, int *offset, Key *key, uint64_t value) {

	if (!key->serializeKey(dataTypes, bytes, offset, len)) {
		return false;
	}

	if (!writeLong(value, bytes, offset, len)) {
		return false;
	}

	return true;
}

std::mutex partitionIdLock;

PartitionedMap **allMaps = new PartitionedMap*[10000];
uint64_t highestMapId = 0;

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_initIndex
  (JNIEnv * env, jobject obj, jintArray dataTypes) {
	  {
		  std::lock_guard<std::mutex> lock(partitionIdLock);

		  if (highestMapId == 0) {
			 for (int i = 0; i < 10000; i++) {
				 allMaps[i] = 0;
			}
		  }

		  uint64_t currMapId = highestMapId;
		  PartitionedMap *newMap = new PartitionedMap(env, (long)currMapId, dataTypes);
		  allMaps[highestMapId] = newMap;
		  highestMapId++;
		  return currMapId;
	  }
}

PartitionedMap *getIndex(JNIEnv *env, jlong indexId) {
	PartitionedMap* map = allMaps[(int)indexId];
	if (map == 0) {
		throwException(env, "Invalid index id");
	}
	return map;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_put__J_3_3Ljava_lang_Object_2_3J_3J
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues, jlongArray jRetValues) {

		if (SB_DEBUG) {
			printf("put begin\n");
			fflush(stdout);
		}
	try {
		uint64_t retValue = -1;
		PartitionedMap* map = getIndex(env, indexId);
		if (map == 0) {
			return;
		}

		int len = env->GetArrayLength(jKeys);
		jlong *values = env->GetLongArrayElements(jValues, 0);
		jlong *retValues = env->GetLongArrayElements(jRetValues, 0);

		my_node *** bins = new my_node**[PARTITION_COUNT];
		int *offsets = new int[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++) {
			offsets[i] = 0;
			bins[i] = new my_node*[len];
//			for (int j = 0; j < len; j++) {
//				bins[i][j] = 0;
//			}
		}

		for (int i = 0; i < len; i++) {
			jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(jKeys, i);

			Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

        	my_node *q, *p = new my_node;

			p->key = startKey;
			p->value = 0;//(uint64_t)values[i];

			bool replaced = false;
			int partition = hashKey(env, map->dataTypes, startKey) % PARTITION_COUNT;

			bins[partition][offsets[partition]++] = p;

		}
		for (int i = 0; i < PARTITION_COUNT; i++) {
			int partition = i;
			for (int j = 0; j < offsets[i]; j++) {
				bool replaced = false;

				//spinlock_guard lock(map->slocks[partition]);
				std::lock_guard<std::mutex> lock(map->locks[partition]);
        		my_node *q = kavl_insert(&map->maps[partition], bins[i][j], map->comparator, 0);
				if (bins[i][j] != q && q != 0) {
		//            	printf("replaced\n");
		//            	fflush(stdout);
					//retValues[i] = q->value;
					q->value = (uint64_t)bins[i][j]->value;
					replaced = true;
				}
				else {
					//retValues[i] = -1;
				}
				if (replaced) {
					//printf("replaced\n");
					//fflush(stdout);
					pushDelete(map, bins[i][j]->key);
					delete bins[i][j];
				}

				//throw std::runtime_error("Failed: ");

				if (SB_DEBUG) {
					printf("put end\n");
					fflush(stdout);
				}
			}
		}
		env->ReleaseLongArrayElements(jValues, values, 0);
		env->ReleaseLongArrayElements(jRetValues, retValues, 0);
		for (int i = 0; i < PARTITION_COUNT; i++) {
			delete[] bins[i];
		}
		delete[] offsets;
	}
	catch (const std::runtime_error&) {
		//logError(env, "Error in jni 'put' call: ", e);
		return;
	}
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put__J_3Ljava_lang_Object_2J
  (JNIEnv * env, jobject obj, jlong indexId, jobjectArray jstartKey, jlong value) {
        my_node *q, *p = new my_node;

		if (SB_DEBUG) {
			printf("put begin\n");
			fflush(stdout);
		}
	try {
		uint64_t retValue = -1;
		PartitionedMap* map = getIndex(env, indexId);
		if (map == 0) {
			return 0;
		}


		Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jstartKey, 0, map->fieldCount);

		if (startKey == 0) {
			printf("put, null key");
			fflush(stdout);
			return 0;
		}

		p->key = startKey;
		p->value = (uint64_t)value;

		bool replaced = false;
		int partition = hashKey(env, map->dataTypes, startKey) % PARTITION_COUNT;
		{
			//spinlock_guard lock(map->slocks[partition]);
			std::lock_guard<std::mutex> lock(map->locks[partition]);
			q = kavl_insert(&map->maps[partition], p, map->comparator, 0);
			if (p != q && q != 0) {
	//            	printf("replaced\n");
	//            	fflush(stdout);
				retValue = q->value;
				q->value = (uint64_t)value;
				replaced = true;
			}
		}
		if (replaced) {
			//printf("replaced\n");
			//fflush(stdout);
			pushDelete(map, p->key);
			delete p;
		}

		//throw std::runtime_error("Failed: ");

		if (SB_DEBUG) {
			printf("put end\n");
			fflush(stdout);
		}
		return retValue;
	}
	catch (const std::runtime_error&) {
		//logError(env, "Error in jni 'put' call: ", e);
		return 0;
	}
}


class SortedListNode {
public:
	SortedListNode * next = 0;
	SortedListNode *prev = 0;
	head_t *head = 0;
	Key *key = 0;
	uint64_t value = 0;
	int partition;
};


class PartitionResults {
  public:
    Key **keys;
	uint64_t* values;
    int count = -1;
    int posWithinPartition = 0;
    Key *key = 0;
    uint64_t value = 0;
    int partition = 0;
	SortedListNode sortedListNode;

	PartitionResults() {
	}

	~PartitionResults() {
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

class SortedList {
 	SortedListNode *head = 0;
 	SortedListNode *tail = 0;
	bool ascending = true;
	PartitionedMap *map = 0;

	SortedListNode **pools = 0;
	int currPoolOffset = 0;
	int poolCount = 0;
	int poolSize = 0;;
	std::mutex l;

 	public:

 	SortedList(PartitionedMap *map, bool ascending, int size) {
 		this->map = map;
 		this->ascending = ascending;
		//poolSize = size;
		//pools = new SortedListNode*[1];
		//pools[0] = new SortedListNode[size];
		//poolCount = 1;
 	}

	~SortedList() {/*
		SortedListNode *curr = head;
		while (curr != 0) {
			SortedListNode *next = curr->next;
			delete curr;
			curr = next;
		}
		*/

		
		//for (int i = 0; i < poolCount; i++) {
			//delete[] pools[i];
		//}
		//delete[] pools;
		
	}

	SortedListNode *allocateNode() {
		//return new SortedListNode();
		std::lock_guard<std::mutex> lock(l);

		currPoolOffset++;
		if (currPoolOffset > poolSize) {
			//printf("called resize");
			//fflush(stdout);

			SortedListNode **newPools = new SortedListNode*[++poolCount];
			memcpy(newPools, pools, (poolCount - 1) * sizeof(SortedListNode*));
			newPools[poolCount - 1] = new SortedListNode[poolSize];
			delete[] pools;
			pools = newPools;
			currPoolOffset = 1;
		}
		return &pools[poolCount - 1][currPoolOffset - 1];
	}

	int size() {
		int ret = 0;
		SortedListNode *curr = head;
		while (curr != 0) {
			curr = curr->next;
			ret++;
		}
		return ret;
	}
 	void push(SortedListNode *node) {
 		if (ascending) {
 			pushAscending(node);
 		}
 		else {
 			pushDescending(node);
 		}
	}

	void pushAscending(SortedListNode *node) {
		if (tail == 0 || head == 0) {
			head = tail = node;
			return;
		}
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
		SortedListNode *curr = tail;
		SortedListNode *last = 0;
		int compareCount = 0;
		while (curr != 0 /*|| (!ascending && curr->next != 0)*/) {
			compareCount++;
			int cmp = map->comparator->compare(node->key, curr->key);
			if (cmp > 0) {
				SortedListNode *next = curr->next;
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

	void pushDescending(SortedListNode *node) {
		if (tail == 0 || head == 0) {
			head = tail = node;
			return;
		}
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
		SortedListNode *curr = head;
 		SortedListNode *last = 0;

		int compareCount = 0;
		while (curr != 0/*|| (!ascending && curr->next != 0)*/) {
			compareCount++;
			int cmp = map->comparator->compare(node->key, curr->key);
			if (cmp < 0) {
				SortedListNode *prev = curr->prev;
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

 	SortedListNode *pop() {
 		if (ascending) {
 			return popAscending();
 		}
 		return popDescending();
	}

	SortedListNode *popAscending() {
 		if (head == 0) {
 			return 0;
 		}

 		SortedListNode *ret = 0;

		ret = head;

		SortedListNode *next = head->next;

		if (next != 0) {
			next->prev = 0;
		}

		head = next;
		if (next == 0) {
			head = tail = 0;
		}

		return ret;
	}

	SortedListNode *popDescending() {
// 		printf("pop\n");
// 		fflush(stdout);
 		if (tail == 0) {
 			return 0;
 		}
		SortedListNode *ret = tail;
		SortedListNode *prev = tail->prev;

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


int nextEntries(PartitionedMap* map, int count, int numRecordsPerPartition, int partition, Key** keys, uint64_t* values, Key *currKey, bool tail) {
    my_node keyNode;
    keyNode.key = currKey;
    int currOffset = 0;
    kavl_itr itr;
    int countReturned = 0;

    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
		//spinlock_guard lock(map->slocks[partition]);

        if (tail) {
            kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator);
        }
        else {
            kavl_itr_find_prev(map->maps[partition], &keyNode, &itr, map->comparator);
		}

        bool found = 0;
        do {

            const my_node *p = kavl_at(&itr);
            if (p == NULL) {
//            	printf("at == 0\n");
//            	fflush(stdout);
                return countReturned;
            }
//            printf("found=%ld", (uint64_t)p->key[0]);
//            fflush(stdout);

			//printf("key: %llu | %llu\n", *(uint64_t*)p->key->key[0], *(uint64_t*)p->key->key[1]);
			//fflush(stdout);

            keys[currOffset] = p->key;
            values[currOffset] = p->value;
            //delete p;
            currOffset++;
            countReturned++;

            if (currOffset >= numRecordsPerPartition) {
                break;
            }
            if (tail) {
				found = kavl_itr_next(&itr);
			}
			else {
            	found = kavl_itr_prev(&itr);
		  	}
//		  	if (!found) {
//		  		printf("next not found");
//            	fflush(stdout);
//		  	}

        } while (found);
//        printf("count returned=%d\n", countReturned);
    }
    return countReturned;
}

void getNextEntryFromPartition(
        PartitionedMap* map, SortedList *sortedList, SortedListNode *node, PartitionResults* currResults,
        int count, int numRecordsPerPartition,
        PartitionResults* entry, int partition, Key *currKey, jboolean first, bool tail) {

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

        entry->count = nextEntries(map, count, numRecordsPerPartition, partition, entry->keys, entry->values, currKey, tail);
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
            if (map->comparator->compare((void**)entry->keys[0], currKey) == 0) {
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

bool next(PartitionedMap* map, SortedList *sortedList, PartitionResults* currResults,
	int count, int numRecordsPerPartition, Key **keys, uint64_t* values,
    int* posInTopLevelArray, int* retCount, bool tail) {

    Key *currKey = 0;
	uint64_t currValue = 0;
    int lowestPartition = 0;

    SortedListNode *node = sortedList->pop();
	//printf("popped: %llu | %llu\n", *(uint64_t*)node->key->key[0], *(uint64_t*)node->key->key[1]);
	//fflush(stdout);

    if (node == 0) {
    	return false;
    }
    lowestPartition = node->partition;

    *posInTopLevelArray = lowestPartition;

    currKey = node->key;
    currValue = node->value;
    getNextEntryFromPartition(map, sortedList, node, currResults, count, numRecordsPerPartition, &currResults[*posInTopLevelArray],
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

jboolean JNICALL tailHeadlockArray
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray jstartKey, jint count, jboolean first, bool tail, jbyteArray bytes, jint len) {
	if (SB_DEBUG) {
		printf("tailHeadBlockArray begin\n");
		fflush(stdout);
	}

	int numRecordsPerPartition = (int)ceil((double)count / (double)PARTITION_COUNT) + 2;

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return true;
	}

	int posInTopLevelArray = 0;

	Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jstartKey, 0, map->fieldCount);

	SortedList sortedList(map, tail, count);

	PartitionResults* currResults = new PartitionResults[PARTITION_COUNT];
	//std::vector<PartitionResults*> currResults(PARTITION_COUNT);

	/*
	std::vector<std::future<int>> results(PARTITION_COUNT);
	for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		results[partition] = threadPool.push([map, partition, currResults, &sortedList, count, startKey, first, tail, numRecordsPerPartition](int) {
			currResults[partition].init(numRecordsPerPartition);
			getNextEntryFromPartition(map, &sortedList, &currResults[partition].sortedListNode, currResults, count, numRecordsPerPartition,
				&currResults[partition], partition, startKey, first, tail);
			return partition;
		});
	}

	for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		results[partition].get();
	}
	*/

	for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		currResults[partition].init(numRecordsPerPartition);
		getNextEntryFromPartition(map, &sortedList, &currResults[partition].sortedListNode, currResults, count, numRecordsPerPartition,
			&currResults[partition], partition, startKey, first, tail);
	}

	//printf("###################### next\n");


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


	/*
	jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

	for (int i = 0; i < retCount; i++) {
		env->SetObjectArrayElement(jKeys, i, nativeKeyToJavaKey(env, map, keys[i]));
		valuesArray[i] = values[i];
	}

	env->ReleaseLongArrayElements(jValues, valuesArray, 0);
	*/

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
JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativePartitionedTree_getResultsObjects
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

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_getResultsBytes
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

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_headBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jbyteArray bytes, jint len) {
  return tailHeadlockArray(env, obj, indexId, startKey, count, first, false, bytes, len);
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jbyteArray bytes, jint len) {
  return tailHeadlockArray(env, obj, indexId, startKey, count, first, true, bytes, len);
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_remove
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
	
	if (SB_DEBUG) {
		printf("remove begin\n");
		fflush(stdout);
	}

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
	uint64_t retValue = -1;
    int partition = hashKey(env, map->dataTypes, key) % PARTITION_COUNT;
	my_node keyNode;
    keyNode.key = key;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
        //spinlock_guard lock(map->slocks[partition]);

	    kavl_itr itr;
        if (0 != kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator)) {

			my_node* nodeRemoved = kavl_erase(&map->maps[partition], &keyNode, 0, map->comparator);
			if (nodeRemoved != 0) {
				if (map->comparator->compare(nodeRemoved->key, key) == 0) {
					retValue = nodeRemoved->value;
					pushDelete(map, nodeRemoved->key);
					delete nodeRemoved;
				}
			}
			else {
				printf("###### - delete not found");
				fflush(stdout);
			}
		}
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);
	
	if (SB_DEBUG) {
		printf("remove end\n");
		fflush(stdout);
	}

    return retValue;

}


JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_clear
  (JNIEnv *env, jobject obj, jlong indexId) {
 
	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return;
	}

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        while (true) {
            if (map->maps[partition] == 0) {
                break;
            }
            //todo: free keys
            kavl_free(map, map->maps[partition], freeNode);

            map->maps[partition] = 0;
        }
    }
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_delete
(JNIEnv *env, jobject obj, jlong indexId) {

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return;
	}

	Java_com_sonicbase_index_NativePartitionedTree_clear(env, obj, indexId);
	allMaps[(int)indexId] = 0;
	delete map;
}


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_get
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
	uint64_t offset = 0;
    kavl_itr itr;
	if (SB_DEBUG) {
		printf("get begin\n");
		fflush(stdout);
	}

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *startKey = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

    int partition = hashKey(env, map->dataTypes, startKey) % PARTITION_COUNT;

	uint64_t retValue = -1;
    my_node keyNode;
    keyNode.key = startKey;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
        //spinlock_guard lock(map->slocks[partition]);

        if (0 != kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator)) {

            const my_node *p = kavl_at(&itr);
            if (p != 0) {
                retValue = p->value;
            }
        }
    }
    deleteKey(env, map->dataTypes, startKey, map->keyPool, map->keyImplPool);

	if (SB_DEBUG) {
		printf("get end\n");
		fflush(stdout);
	}
	return retValue;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("higherEntry begin\n");
		fflush(stdout);
	}
	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

    my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
          std::lock_guard<std::mutex> lock(map->locks[partition]);
          //spinlock_guard lock(map->slocks[partition]);

			kavl_itr itr;
			bool found = kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator);

			p = kavl_at(&itr);
			if (p != NULL) {
			//todo: should this return higher entry even if key doesn' exist?
				if (found) {
					if (kavl_itr_next(&itr)) {
						p = kavl_at(&itr);
					}
					else {
						p = 0;
					}
				}
			}
			if (lowest.key == 0 && p != 0) {
				lowest.key = p->key;
				lowest.value = p->value;
			}
			else if (p != 0 && map->comparator->compare(p->key, lowest.key) < 0) {
				lowest.key = p->key;
				lowest.value = p->value;
			}
		}
	}

    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

	if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest.key));
    	valuesArray[0] = lowest.value;

    	env->ReleaseLongArrayElements(jValues, valuesArray, 0);

	   	return true;
	}
	if (SB_DEBUG) {
		printf("higherEntry end\n");
		fflush(stdout);
	}
	return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {
    
	if (SB_DEBUG) {
		printf("lowerEntry begin\n");
		fflush(stdout);
	}
	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

    my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
    		std::lock_guard<std::mutex> lock(map->locks[partition]);
    		//spinlock_guard lock(map->slocks[partition]);

            kavl_itr_find_prev(map->maps[partition], &keyNode, &itr, map->comparator);
            p = kavl_at(&itr);
            if (p != NULL) {
                if (map->comparator->compare(p->key, key) == 0) {
                    if (kavl_itr_prev(&itr)) {
                        p = kavl_at(&itr);
                    }
                    else {
                        p = 0;
                    }
                }
            }
            if (highest.key == 0 && p != 0) {
                highest.key = p->key;
                highest.value = p->value;
            }
            else if (p != 0 && map->comparator->compare(p->key, highest.key) > 0) {
                highest.key = p->key;
                highest.value = p->value;
            }
        }
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (highest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, highest.key));
		valuesArray[0] = highest.value;

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

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_floorEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("floorEntry end\n");
		fflush(stdout);
	}
	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

    Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

	my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);
			//spinlock_guard lock(map->slocks[partition]);

            kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator);

            p = kavl_at(&itr);

            if (lowest.key == 0 && p != 0) {
                lowest.key = p->key;
                lowest.value = p->value;
			}
            else if (p != 0) {
				//printf("checking: %llu | %llu\n", *(uint64_t*)p->key->key[0], *(uint64_t*)p->key->key[1]);
				//fflush(stdout);
				if (map->comparator->compare(p->key, lowest.key) < 0) {
					lowest.key = p->key;
					lowest.value = p->value;
					//printf("now highest: %llu | %llu\n", *(uint64_t*)lowest.key->key[0], *(uint64_t*)lowest.key->key[1]);
					//fflush(stdout);
				}
            }
        }
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest.key));
		valuesArray[0] = lowest.value;

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

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_ceilingEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("ceilingEntry begin\n");
		fflush(stdout);
	}

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

	Key *key = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);

	my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);
	        //spinlock_guard lock(map->slocks[partition]);

            if (kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator)) {
            	p = kavl_at(&itr);
                lowest.key = p->key;
                lowest.value = p->value;
                break;
            }

            p = kavl_at(&itr);
            if (lowest.key == 0 && p != 0) {
                lowest.key = p->key;
                lowest.value = p->value;
            }
            else if (p != 0 && map->comparator->compare(p->key, lowest.key) < 0) {
                lowest.key = p->key;
			 	lowest.value = p->value;
            }
        }
    }
    deleteKey(env, map->dataTypes, key, map->keyPool, map->keyImplPool);

    if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest.key));
		valuesArray[0] = lowest.value;

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


JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_lastEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {
    
	if (SB_DEBUG) {
		printf("lastEntry2 begin\n");
		fflush(stdout);
	}

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

	const my_node *p = 0;
    my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        std::lock_guard<std::mutex> lock(map->locks[partition]);
	        //spinlock_guard lock(map->slocks[partition]);

			p = kavl_itr_last(map->maps[partition]);

			if (highest.key == 0 && p != 0) {
				highest.key = p->key;
				highest.value = p->value;
			}
			else if (p != 0 && map->comparator->compare(p->key, highest.key) > 0) {
				highest.key = p->key;
				highest.value = p->value;
			}
        }
    }

    if (highest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, highest.key));
		valuesArray[0] = highest.value;

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

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_firstEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {

	if (SB_DEBUG) {
		printf("firstEntry2 begin\n");
		fflush(stdout);
	}

	PartitionedMap* map = getIndex(env, indexId);
	if (map == 0) {
		return 0;
	}

	const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        std::lock_guard<std::mutex> lock(map->locks[partition]);
	        //spinlock_guard lock(map->slocks[partition]);

            p = kavl_itr_first(map->maps[partition]);

			if (lowest.key == 0 && p != 0) {
				lowest.key = p->key;
				lowest.value = p->value;
			}
			else if (p != 0 && map->comparator->compare(p->key, lowest.key) < 0) {
				lowest.key = p->key;
				lowest.value = p->value;
			}
        }
    }

    if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map->dataTypes, lowest.key));
		valuesArray[0] = lowest.value;

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

void binarySort(PartitionedMap *map, Key **a, size_t lo, size_t hi, size_t start, bool ascend) {

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

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_sortKeys
(JNIEnv *env, jobject obj, jlong indexId, jobjectArray keysObj, jboolean ascend) {

	PartitionedMap *map = getIndex(env, indexId);

	size_t keyCount = env->GetArrayLength(keysObj);
	Key ** nativeKeys = new Key*[keyCount];
	for (int i = 0; i < keyCount; i++) {
		jobjectArray jKey = (jobjectArray)env->GetObjectArrayElement(keysObj, i);
		nativeKeys[i] = javaKeyToNativeKey(env, map->keyPool, map->keyImplPool, map->dataTypes, jKey, 0, map->fieldCount);
	}

	binarySort(map, nativeKeys, 0, keyCount, 0, ascend);

	for (int i = 0; i < keyCount; i++) {
		env->SetObjectArrayElement(keysObj, i, nativeKeyToJavaKey(env, map->dataTypes, nativeKeys[i]));
	}
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

/**************************************************************
 * Destroy the global static Class Id variables
 **************************************************************/
void JNI_OnUnload(JavaVM *vm, void *reserved) {

    // Obtain the JNIEnv from the VM
    // NOTE: some re-do the JNI Version check here, but I find that redundant
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    // Destroy the global references
    env->DeleteGlobalRef(gNativePartitioned_class);
    env->DeleteGlobalRef(gObject_class);
    env->DeleteGlobalRef(gLong_class);
    env->DeleteGlobalRef(gShort_class);
    env->DeleteGlobalRef(gInt_class);
    env->DeleteGlobalRef(gByte_class);
    env->DeleteGlobalRef(gBool_class);
    env->DeleteGlobalRef(gDouble_class);
    env->DeleteGlobalRef(gFloat_class);
    env->DeleteGlobalRef(gTimestamp_class);
    env->DeleteGlobalRef(gTime_class);
    env->DeleteGlobalRef(gDate_class);
    env->DeleteGlobalRef(gBigDecimal_class);
}

spinlock_t slock;
std::mutex mlock;
std::atomic<long> countExecuted;
unsigned long begin;

using namespace std::chrono;

bool spin = true;

void runner() {
	while (true) {
		if (spin) {
			int count = 0;
			slock.lock();
			for (int i = 0; i < 100000; i++) {
				count++;
			}

			slock.unlock();
		}
		else {
			int count = 0;
			std::lock_guard<std::mutex> lock(mlock);
			for (int i = 0; i < 100000; i++) {
				count++;
			}
		}
		if (countExecuted++ % 10000000 == 0) {
    		unsigned long ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

			printf("progress: %ld, rate=%f\n", countExecuted.load(), ((float)countExecuted.load() / ((float)ms - (float)begin)*(float)1000));
		}
	}
}

class Node {
 	void **key;
 	int len;
};
class MyValue2 {
	long value;
};


void allocator() {
/*
	thread_local PooledObjectPool<Node> pool;
	while (true) {
		Node *node = pool.allocate();
		node = pool.allocate();
		node = pool.allocate();

		//Node *node = new Node();
		//node = new Node();
		//node = new Node();

		//void ** key = new void*[1];
		//long * kkey = new long();
		//MyValue2 *value = new MyValue2();
		//MyValue2 *value2 = new MyValue2();
		long c = countExecuted += 1;
		if (c % 10000000 == 0) {
    		unsigned long ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

			printf("progress: %ld, rate=%.2f\n", countExecuted.load(), ((double)countExecuted.load() / ((double)ms - (double)begin)*(double)1000));
		}
	}
	*/
}
int main2() {

	begin = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();


	//countExcecuted =
	std::thread **threads = new std::thread*[96];
	for (int i = 0; i < 96; i++) {
		threads[i] = new std::thread(allocator);
    	threads[i]->detach();
    	}

	std::this_thread::sleep_for(std::chrono::milliseconds(1000000));



	return 0;
}
/*
class spinlock_t {
    std::atomic_flag lock_flag;
public:
    spinlock_t() { lock_flag.clear(); }

    bool try_lock() { return !lock_flag.test_and_set(std::memory_order_acquire); }
    void lock() { for (size_t i = 0; !try_lock(); ++i) if (i % 100 == 0) std::this_thread::yield(); }
    void unlock() { lock_flag.clear(std::memory_order_release); }
};

class PartitionedMap {
public:
    int id = 0;
    my_node * maps[PARTITION_COUNT];
    std::mutex *locks = new std::mutex[PARTITION_COUNT];
*/




