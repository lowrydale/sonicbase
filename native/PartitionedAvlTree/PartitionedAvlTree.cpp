/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//#include "stdafx.h"
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


//#include <pthread.h>

#ifdef _WIN32
#include <WinSock2.h>
#include <Windows.h>
#else
#include <sys/time.h>
#include<pthread.h>
#include<semaphore.h>
#endif
#include "kavl.h"

#define SB_DEBUG 0


//extern "C" {void *__dso_handle = NULL; } extern "C" {void *__cxa_atexit = NULL; }

const int PARTITION_COUNT = 64; // 64 for 17mil tailBlock reads/sec
//const int NUM_RECORDS_PER_PARTITION =  32 + 2;
//const int BLOCK_SIZE = (PARTITION_COUNT * NUM_RECORDS_PER_PARTITION);



#define BIT -7
#define TINYINT -6
#define SMALLINT 5
#define INTEGER 4
#define BIGINT -5
#define FLOAT 6
#define REAL 7
#define DOUBLE 8
#define NUMERIC 2
#define DECIMAL 3
#define CHAR 1
#define VARCHAR 12
#define LONGVARCHAR -1
#define DATE 91
#define TIME 92
#define TIMESTAMP 93
#define BINARY -2
#define VARBINARY -3
#define LONGVARBINARY -4
#define OTHER 1111
#define JAVA_OBJECT 2000
#define DISTINCT 2001
#define STRUCT 2002
#define ARRAY 2003
#define BLOB 2004
#define CLOB 2005
#define REF 2006
#define DATALINK 70
#define BOOLEAN 16
#define ROWID -8
#define NCHAR -15
#define NVARCHAR -9
#define LONGNVARCHAR -16
#define NCLOB 2011
#define SQLXML 2009
#define REF_CURSOR 2012
#define TIME_WITH_TIMEZONE 2013
#define TIMESTAMP_WITH_TIMEZONE 2014


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

bool shutdownFlag = false;

//unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
//ctpl::thread_pool threadPool(concurentThreadsSupported);

#ifdef _WIN32
#define smin(x, y) min(x, y)
#else
#define smin(x, y) std::min(x, y)
#endif

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

struct ByteArray {
	uint8_t* bytes;
	int len;
};

struct sb_utf8str {
    uint16_t *bytes;
    int len;
};



class LongComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			if (o1 == 0 || o2 == 0) {
				return 0;
			}
			if (*((uint64_t*) o1) < *((uint64_t*) o2)) {
			  return -1;
			}
			else {
			  return *((uint64_t*) o1) > *((uint64_t*) o2) ? 1 : 0;
			}
		}
};

class Utf8Comparator : public Comparator {

	public:
	  int compareBytes(uint8_t* b1, int s1, int l1, uint8_t* b2, int s2, int l2) {
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		int i = s1;

		for(int j = s2; i < end1 && j < end2; ++j) {
		  int a = b1[i] & 255;
		  int b = b2[j] & 255;
		  if (a != b) {
			return a - b;
		  }
		  ++i;
		}

		return l1 - l2;
	  }

	int compare(void *o1, void *o2) {
		if (o1 == 0 || o2 == 0) {
		  return 0;
		}

		sb_utf8str *o1Bytes = (sb_utf8str*)o1;
		sb_utf8str *o2Bytes = (sb_utf8str*)o2;

        int len1 = o1Bytes->len;
        int len2 = o2Bytes->len;
        int lim = smin(len1, len2);
        uint16_t *v1 = o1Bytes->bytes;
        uint16_t *v2 = o2Bytes->bytes;

        int k = 0;
        while (k < lim) {
            uint16_t c1 = v1[k];
            uint16_t c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;


		//return compareBytes(o1Bytes->bytes, 0, o1Bytes->len, o2Bytes->bytes, 0, o2Bytes->len);
		//return utf8casecmp(o1Bytes->bytes, o2Bytes->bytes);

		//return compareBytes(o1Bytes->bytes, 0, o1Bytes->len, o2Bytes->bytes, 0, o2Bytes->len);
	}
};


class ShortComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			short *l1 = (short*)o1;
			short *l2 = (short*)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};

class ByteComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			uint8_t *l1 = (uint8_t*)o1;
			uint8_t *l2 = (uint8_t*)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};

class BooleanComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			bool *l1 = (bool*)o1;
			bool *l2 = (bool*)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};


class IntComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			int *l1 = (int*)o1;
			int *l2 = (int*)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};

class DoubleComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			double *l1 = (double *)o1;
			double *l2 = (double *)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};

class FloatComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			float *l1 = (float *)o1;
			float *l2 = (float *)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (*l1 < *l2) {
			  return -1;
			}
			else {
			  return *l1 > *l2 ? 1 : 0;
			}
		}
};

class TimestampComparator : public Comparator {
	public:
		int compare(void *o1, void *o2) {
			uint64_t *l1 = (uint64_t*)o1;
			uint64_t *l2 = (uint64_t*)o2;
			if (l1 == 0 || l2 == 0) {
				return 0;
			}
			if (l1[0] < l2[0]) {
			  return -1;
			}
			else {
			  int ret = l1[0] > l2[0] ? 1 : 0;
			  if (ret == 0) {
			  	if (l1[1] < l2[1]) {
			  		return -1;
			  	}
			  	ret = l1[1] > l2[1] ? 1 : 0;
			  }
			  return ret;
			}
		}
};

class BigDecimalComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			if (o1 == 0 || o2 == 0) {
				return 0;
			}
			int ret = BigDecimal::compareTo(*(std::string*)o1, *(std::string*)o2);
			//printf("compare: lhs=%s, rhs=%s, ret=%d]\n", ((std::string*)o1)->c_str(), ((std::string*)o2)->c_str(), ret);
			//fflush(stdout);
			return ret;
		}
};

class KeyComparator : public Comparator {
	private:
		int fieldCount = 0;
		Comparator **comparators = 0;

	public:

	KeyComparator(JNIEnv *env, int fieldCount, int *dataTypes) {
		this->fieldCount = fieldCount;
		comparators = new Comparator*[fieldCount];
		for (int i = 0; i < fieldCount; i++) {
			switch (dataTypes[i]) {
				case BIGINT:
				case ROWID:
					comparators[i] = new LongComparator();
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

	int compare(void *o1, void *o2) {
		int keyLen = ((Key*)o1)->len <= ((Key*)o2)->len ? ((Key*)o1)->len : ((Key*)o2)->len;
		for (int i = 0; i < keyLen; i++) {
          int value = comparators[i]->compare(((Key*)o1)->key[i], ((Key*)o2)->key[i]);
          if (value != 0) {
            return value;
          }
        }
        return 0;
	}
};



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
		printf("cleard exception\n");
		fflush(stdout);
	}

	jint ret = env->ThrowNew(exClass, message);
	printf("throw exception: ret=%d\n", ret);
	fflush(stdout);
	return ret;
}

class PartitionedMap {
public:
    int id = 0;
    my_node * maps[PARTITION_COUNT];
    std::mutex *locks = new std::mutex[PARTITION_COUNT];
    Comparator *comparator = 0;
	int *dataTypes = 0;
	int fieldCount = 0;

	~PartitionedMap() {
		delete[] locks;
		delete comparator;
		delete[] dataTypes;
	}

    PartitionedMap(JNIEnv * env, jintArray dataTypes) {
        for (int i = 0; i < PARTITION_COUNT; i++) {
            maps[i] = 0;
        }

		fieldCount = env->GetArrayLength(dataTypes);
		jint *types = env->GetIntArrayElements(dataTypes, 0);
		this->dataTypes = new int[fieldCount];
		memcpy( (void*)this->dataTypes, (void*)types, fieldCount * sizeof(int));
		comparator = new KeyComparator(env, fieldCount, this->dataTypes);

		env->ReleaseIntArrayElements(dataTypes, types, 0);
    }

	PartitionedMap(int* dataTypes, int fieldCount) {
		for (int i = 0; i < PARTITION_COUNT; i++) {
			maps[i] = 0;
		}

		this->dataTypes = new int[fieldCount];
		memcpy((void*)this->dataTypes, (void*)dataTypes, fieldCount * sizeof(int));
		comparator = new KeyComparator(NULL, fieldCount, this->dataTypes);
	}
};




void deleteKey(JNIEnv *env, PartitionedMap *map, Key *key) {
	for (int i = 0; i < key->len; i++) {
		if (key->key[i] == 0) {
			continue;
		}
		int type = map->dataTypes[i];

		switch (type) {
		case ROWID:
		case BIGINT:
		case DATE:
		case TIME:
			delete (uint64_t*)key->key[i];
			break;
		case SMALLINT:
			delete (short*)key->key[i];
			break;
		case INTEGER:
			delete (int*)key->key[i];
			break;
		case TINYINT:
			delete (uint8_t*)key->key[i];
			break;
		case NUMERIC:
		case DECIMAL:
			delete (std::string *)key->key[i];
			break;
		case VARCHAR:
		case CHAR:
		case LONGVARCHAR:
		case NCHAR:
		case NVARCHAR:
		case LONGNVARCHAR:
		case NCLOB:
		{
			sb_utf8str *str = (sb_utf8str*)key->key[i];
			delete[] str->bytes;
			delete str;
		}
		break;
		case TIMESTAMP:
		{
			//printf("deleting");
			//fflush(stdout);
			uint64_t *entry = (uint64_t*)key->key[i];
			delete[] entry;
		}
		break;
		case DOUBLE:
		case FLOAT:
		{
			delete (double*)key->key[i];
		}
		break;
		case REAL:
		{
			delete (float*)key->key[i];
		}
		break;
		default:
		{
			if (env != 0) {
				//char *buffer = new char[75];
				printf("Unsupported datatype in deleteKey: type=%d", type);
				//logError(env, buffer);
				//delete[] buffer;
			}
		}

		}

	}
	delete[] key->key;
	delete key;
}


class DeleteQueueEntry {
public:
	PartitionedMap * map = 0;
	Key *key = 0;
	uint64_t time = 0;
	DeleteQueueEntry *next = 0;
	DeleteQueueEntry *prev = 0;
};

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
class DeleteQueue {
	std::mutex queueLock;
	DeleteQueueEntry *head = 0;
	DeleteQueueEntry *tail = 0;

public:
	void push(DeleteQueueEntry *entry) {
		{

			entry->time = getCurrMillis();
			std::lock_guard<std::mutex> lock(queueLock);
			if (tail == 0) {
				head = tail = entry;
			}
			else {
				tail->next = entry;
				entry->prev = tail;
				tail = entry;
			}
		}
	}

	DeleteQueueEntry *pop() {
		{
			std::lock_guard<std::mutex> lock(queueLock);
			if (head == 0) {
				return 0;
			}

			if (getCurrMillis() - head->time > 10000) {
				return 0;
			}

			DeleteQueueEntry *ret = 0;
			ret = head;
			head = ret->next;
			if (head == 0) {
				tail = 0;
			}
			return ret;
		}
	}
};

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
		deleteKey(0, entry->map, entry->key);
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
	entry->key = key;
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

float timedifference_msec(struct timeval t0, struct timeval t1)
{
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}


static uint64_t EXP_BIT_MASK = 9218868437227405312L;
static uint64_t SIGNIF_BIT_MASK = 4503599627370495L;

static inline uint64_t doubleToRawLongBits(double x) {
	uint64_t bits;
	memcpy(&bits, &x, sizeof bits);
	return bits;
}

static uint64_t doubleToLongBits(double value) {
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

uint32_t hashKey(JNIEnv* env, PartitionedMap *map, Key *key) {
	uint32_t hashCode = 1;
	for (int i = 0; i < key->len; i++) {
		int type = map->dataTypes[i];
		uint32_t currHashCode = 0;
		if (key->key[i] == 0) {
			continue;
		}
		switch (type) {
			case ROWID:
			case BIGINT:
			case DATE:
			case TIME:
			{
				currHashCode = (int)(*((uint64_t*)key->key[i]) ^ (*((uint64_t*)key->key[i]) >> 32));
			}
			break;
			case INTEGER:
			{
				currHashCode = *(int*)key->key[i];
			}
			break;
			case SMALLINT:
			{
				currHashCode = *(short*)key->key[i];
			}
			break;
			case TINYINT:
			{
				currHashCode = *(uint8_t*)key->key[i];
			}
			break;
			case BOOLEAN:
			case BIT:
			{
				currHashCode = *(bool*)key->key[i] ? 1231 : 1237;
			}
			break;
			case TIMESTAMP:
			{
				//printf("hash: entering");
				//fflush(stdout);
				//long l = ( ((long*)(key[i]))[0]);
				//printf("hash: key=%ld", (long)l);
				//fflush(stdout);
				currHashCode = (int)( ((uint64_t*)key->key[i])[0] ^ (((uint64_t*)key->key[i])[0] >> 32));
				//currHashCode = 31 * ((long*)key[i])[1] ^ (((long*)key[i])[1] >> 32) + currHashCode;
			}
			break;
			case DOUBLE:
			case FLOAT:
			{
				uint64_t bits = doubleToLongBits(*(double *)key->key[i]);
				currHashCode = (uint32_t)(bits ^ (bits >> 32));
			}
			break;
			case REAL:
			{
				uint32_t bits = float_to_bits(*(float *)key->key[i]);
				currHashCode = bits;
			}
			break;
			case NUMERIC:
			case DECIMAL:
			{
				std::vector<uint8_t> myVector(((std::string*)key->key[i])->begin(), ((std::string*)key->key[i])->end());
				uint8_t *p = &myVector[0];
				currHashCode = 1;
				size_t len = ((std::string*)key->key[i])->length();
				for (int j = 0; j < len; j++) {
					currHashCode = 31 * currHashCode + p[j];
				}
			}
			break;
			case CHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
			case VARCHAR:
			{
				sb_utf8str *str = (sb_utf8str*)key->key[i];
				uint16_t *bytes = str->bytes;
				currHashCode = 1;
				int len = str->len;
				for (int j = 0; j < len; j++) {
					currHashCode = 31 * currHashCode + bytes[j];
				}
			}
			break;
			default:
				//char *buffer = new char[75];
				printf("Unsupported datatype in hashKey: type=%d", type);
				//logError(env, buffer);
				//delete[] buffer;
			break;

		}

		hashCode = 31 * hashCode + currHashCode;
	}
	return abs((int)hashCode);
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


Key *javaKeyToNativeKey(JNIEnv *env, PartitionedMap *map, jobjectArray jKey) {
	int len = env->GetArrayLength(jKey);
	void **ret = new void*[len];
	for (int i = 0; i < len; i++) {
		int type = map->dataTypes[i];
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
				//printf("toNative: keyOffset=%d, long=%ld", i, l);
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
	Key *key = new Key();
	key->len = len;
	key->key = ret;
    return key;
}

jobjectArray nativeKeyToJavaKey(JNIEnv *env, PartitionedMap *map, Key *key) {

	jobjectArray ret = env->NewObjectArray(key->len, gObject_class, NULL);
    if (ret == 0) {
    	return 0;
    }

	for (int i = 0; i < key->len; i++) {
		if (key->key[i] == 0) {
			continue;
		}
		int type = map->dataTypes[i];
		switch (type) {
			case ROWID:
			case BIGINT:
			{
				jobject obj = env->CallStaticObjectMethod(gLong_class, gLong_valueOf_mid, *((uint64_t*)key->key[i]));
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case SMALLINT:
			{
				jobject obj = env->CallStaticObjectMethod(gShort_class, gShort_valueOf_mid, *((short*)key->key[i]));
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case INTEGER:
			{
				jobject obj = env->CallStaticObjectMethod(gInt_class, gInt_valueOf_mid, *(int*)key->key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TINYINT:
			{
				jobject obj = env->CallStaticObjectMethod(gInt_class, gByte_valueOf_mid, *(uint8_t*)key->key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case DATE:
			{
				jobject obj = env->NewObject(gDate_class, gDate_ctor, *(uint64_t*)key->key[i]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TIME:
			{
				jobject obj = env->NewObject(gTime_class, gTime_ctor, *(uint64_t*)key->key[i]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TIMESTAMP:
			{
				jobject obj = env->NewObject(gTimestamp_class, gTimestamp_ctor, ((uint64_t*)key->key[i])[0]);
                env->CallVoidMethod(obj, gTimestamp_setNanos_mid, (int)((uint64_t*)key->key[i])[1]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case DOUBLE:
			case FLOAT:
			{
				jobject obj = env->CallStaticObjectMethod(gDouble_class, gDouble_valueOf_mid, *(double*)key->key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case REAL:
			{
				jobject obj = env->CallStaticObjectMethod(gFloat_class, gFloat_valueOf_mid, *(float*)key->key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case NUMERIC:
			case DECIMAL:
			{
				jstring jstr = env->NewStringUTF(((std::string*)key->key[i])->c_str());
				jobject obj = env->NewObject(gBigDecimal_class, gBigDecimal_ctor, jstr);
				env->SetObjectArrayElement(ret, i, obj);
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
				if (((sb_utf8str*)key->key[i])->len != 0) {
					jcharArray arr = env->NewCharArray(((sb_utf8str*)key->key[i])->len);
					env->SetCharArrayRegion(arr, 0, ((sb_utf8str*)key->key[i])->len, (jchar*)((sb_utf8str*)key->key[i])->bytes);
					env->SetObjectArrayElement(ret, i, (jobject)arr);
					//printf("key=" + std::wstring(((sb_utf8str*)key[i])->bytes));
				}
			}
			break;
			default:
			{
				//char *buffer = new char[75];
				printf("Unsupported datatype in nativeKeyToJavaKey: type=%d", type);
				//logError(env, buffer);
				//delete[] buffer;
			}

		}
	}
    return ret;
}

jboolean serializeKeyValue(JNIEnv *env, PartitionedMap *map, jbyte *bytes, int len, int *offset, Key *key, uint64_t value) {

	/*
	for (int i = 0; i < key->len; i++) {
		if (key->key[i] == 0) {
			printf("null key field: offset=%i", i);
			fflush(stdout);
			return;
		}
	}
	*/

	for (int i = 0; i < key->len; i++) {
		if (offset[0] > len) {
			return false;
		}
		if (key->key[i] == 0) {
			bytes[offset[0]++] = 0;
			continue;
		}
		else {
			bytes[offset[0]++] = 1;
		}
		int type = map->dataTypes[i];
		switch (type) {
		case ROWID:
		case BIGINT:
		{
			if (!writeLong(*((uint64_t*)key->key[i]), bytes, offset, len)) {
				return false;
			}
		}
		break;
		case SMALLINT:
		{
			if (!writeShort(*((short*)key->key[i]), bytes, offset, len)) {
				return false;
			}
		}
		break;
		case INTEGER:
		{
			if (!writeInt(*((int*)key->key[i]), bytes, offset, len)) {
				return false;
			}
		}
		break;
		case TINYINT:
		{
			if (offset[0] > len) {
				return false;
			}
			bytes[offset[0]++] = (jbyte)*(uint8_t*)key->key[i];
		}
		break;
		case DATE:
		{
			if (!writeLong(*(uint64_t*)key->key[i], bytes, offset, len)) {
				return false;
			}
		}
		break;
		case TIME:
		{
			if (!writeLong(*(uint64_t*)key->key[i], bytes, offset, len)) {
				return false;
			}
		}
		break;
		case TIMESTAMP:
		{
			if (!writeLong(((uint64_t*)key->key[i])[0], bytes, offset, len)) {
				return false;
			}
			if (!writeInt((int)((uint64_t*)key->key[i])[1], bytes, offset, len)) {
				return false;
			}
		}
		break;
		case DOUBLE:
		case FLOAT:
		{
			uint64_t bits;
			memcpy(&bits, (double*)key->key[i], sizeof bits);
			if (!writeLong(bits, bytes, offset, len)) {
				return false;
			}
		}
		break;
		case REAL:
		{
			uint32_t bits;
			memcpy(&bits, (float*)key->key[i], sizeof bits);
			if (!writeInt(bits, bytes, offset, len)) {
				return false;
			}
		}
		break;
		case NUMERIC:
		case DECIMAL:
		{
			std::string *s = (std::string*)key->key[i];
			const char *str = ((std::string*)key->key[i])->c_str();
			size_t slen = s->length();
			if (!writeLong(slen, bytes, offset, len)) {
				return false;
			}
			if (offset[0] + slen * 2 > len) {
				return false;
			}
			for (int i = 0; i < slen; i++) {
				uint16_t v = str[i];
				bytes[offset[0]++] = (v >> 8) & 0xFF;
				bytes[offset[0]++] = (v >> 0) & 0xFF;
			}
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
			const uint16_t *str = ((sb_utf8str*)key->key[i])->bytes;
			int slen = ((sb_utf8str*)key->key[i])->len;
			if (!writeLong(slen, bytes, offset, len)) {
				return false;
			}
			if (offset[0] + slen * 2 > len) {
				return false;
			}
			for (int i = 0; i < slen; i++) {
				uint16_t v = str[i];
				bytes[offset[0]++] = (v >> 8) & 0xFF;
				bytes[offset[0]++] = (v >> 0) & 0xFF;
			}
		}
		break;
		default:
		{
			//char *buffer = new char[75];
			printf("Unsupported datatype in serializeKeyValue: type=%d", type);
			//logError(env, buffer);
			//delete[] buffer;
			continue;
		}

	    }
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

		  PartitionedMap *newMap = new PartitionedMap(env, dataTypes);
		  uint64_t currMapId = highestMapId;
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

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put
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


		Key *startKey = javaKeyToNativeKey(env, map, jstartKey);

		if (startKey == 0) {
			printf("put, null key");
			fflush(stdout);
			return 0;
		}

		p->key = startKey;
		p->value = (uint64_t)value;

		bool replaced = false;
		int partition = hashKey(env, map, startKey) % PARTITION_COUNT;
		{
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

	Key *startKey = javaKeyToNativeKey(env, map, jstartKey);

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
				fit = serializeKeyValue(env, map, rawjBytes, len, offset, keys[i], values[i]);
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
		deleteKey(env, map, startKey);
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

	Key *key = javaKeyToNativeKey(env, map, jKey);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
	uint64_t retValue = -1;
    int partition = hashKey(env, map, key) % PARTITION_COUNT;
	my_node keyNode;
    keyNode.key = key;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);

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
    deleteKey(env, map, key);
	
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

    Key *startKey = javaKeyToNativeKey(env, map, jKey);

    int partition = hashKey(env, map, startKey) % PARTITION_COUNT;

	uint64_t retValue = -1;
    my_node keyNode;
    keyNode.key = startKey;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);

        if (0 != kavl_itr_find(map->maps[partition], &keyNode, &itr, map->comparator)) {

            const my_node *p = kavl_at(&itr);
            if (p != 0) {
                retValue = p->value;
            }
        }
    }
    deleteKey(env, map, startKey);

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

    Key *key = javaKeyToNativeKey(env, map, jKey);

    my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
          std::lock_guard<std::mutex> lock(map->locks[partition]);

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

    deleteKey(env, map, key);

	if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

    	env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, lowest.key));
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

	Key *key = javaKeyToNativeKey(env, map, jKey);

    my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
    		std::lock_guard<std::mutex> lock(map->locks[partition]);

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
    deleteKey(env, map, key);

    if (highest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, highest.key));
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

    Key *key = javaKeyToNativeKey(env, map, jKey);

	my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

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
    deleteKey(env, map, key);

    if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, lowest.key));
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

	Key *key = javaKeyToNativeKey(env, map, jKey);

	my_node keyNode;
    keyNode.key = key;
    const my_node *p = 0;
    my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

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
    deleteKey(env, map, key);

    if (lowest.key != 0) {
		jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, lowest.key));
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

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, highest.key));
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

		env->SetObjectArrayElement(jKeys, 0, nativeKeyToJavaKey(env, map, lowest.key));
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
		nativeKeys[i] = javaKeyToNativeKey(env, map, jKey);
	}

	binarySort(map, nativeKeys, 0, keyCount, 0, ascend);

	for (int i = 0; i < keyCount; i++) {
		env->SetObjectArrayElement(keysObj, i, nativeKeyToJavaKey(env, map, nativeKeys[i]));
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




