/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <stdio.h>
#include <string>
#include <stdlib.h>
//#include <sys/time.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <iomanip>
#ifdef _WIN32
#include "stdafx.h"
#else
#include <unistd.h>
#endif
#include <map>
#include <iterator>
#include <vector>
#include <algorithm>
#include <cstring>
#include <locale>
#include <future>
#include <sys/time.h>
#include <math.h>
#include "utf8.h"
#include "kavl.h"
#include "BigDecimal.h"

#include <jni.h>        // JNI header provided by JDK#include <stdio.h>      // C Standard IO Header
#include "com_sonicbase_index_NativePartitionedTree.h"


#include <pthread.h>

#ifdef WIN32
#include <WinSock2.h>
#include <Windows.h>
#else
#include<pthread.h>
#include<semaphore.h>
#endif


const int PARTITION_COUNT = 16; // 64 for 17mil tailBlock reads/sec
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


struct rwlock {
    pthread_mutex_t lock;
    pthread_cond_t read, write;
    unsigned readers, writers, read_waiters, write_waiters;
};

void reader_lock(struct rwlock *self) {
    pthread_mutex_lock(&self->lock);
    if (self->writers || self->write_waiters) {
        self->read_waiters++;
        do pthread_cond_wait(&self->read, &self->lock);
        while (self->writers || self->write_waiters);
        self->read_waiters--;
    }
    self->readers++;
    pthread_mutex_unlock(&self->lock);
}

void reader_unlock(struct rwlock *self) {
    pthread_mutex_lock(&self->lock);
    self->readers--;
    if (self->write_waiters)
        pthread_cond_signal(&self->write);
    pthread_mutex_unlock(&self->lock);
}

void writer_lock(struct rwlock *self) {
    pthread_mutex_lock(&self->lock);
    if (self->readers || self->writers) {
        self->write_waiters++;
        do pthread_cond_wait(&self->write, &self->lock);
        while (self->readers || self->writers);
        self->write_waiters--;
    }
    self->writers = 1;
    pthread_mutex_unlock(&self->lock);
}

void writer_unlock(struct rwlock *self) {
    pthread_mutex_lock(&self->lock);
    self->writers = 0;
    if (self->write_waiters)
        pthread_cond_signal(&self->write);
    else if (self->read_waiters)
        pthread_cond_broadcast(&self->read);
    pthread_mutex_unlock(&self->lock);
}

void rwlock_init(struct rwlock *self) {
    self->readers = self->writers = self->read_waiters = self->write_waiters = 0;
    pthread_mutex_init(&self->lock, NULL);
    pthread_cond_init(&self->read, NULL);
    pthread_cond_init(&self->write, NULL);
}

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


//class rlock
//{
//    rwlock &m_;
//
//public:
//    rlock(rwlock &m)
//      : m_(m)
//    {
//        reader_lock(&m_);
//    }
//    ~rlock()
//    {
//        reader_unlock(&m_);
//    }
//};
//
//class wlock
//{
//    rwlock &m_;
//
//public:
//    wlock(rwlock &m)
//      : m_(m)
//    {
//        writer_lock(&m_);
//    }
//    ~wlock()
//    {
//        writer_unlock(&m_);
//    }
//};


#define synchronized(m) \
    for(std::unique_lock<std::recursive_mutex> lk(m); lk; lk.unlock())




struct ByteArray {
	uint8_t* bytes;
	int len;
};

struct sb_utf8str {
    uint8_t *bytes;
    int len;
};



class LongComparator : public Comparator {

	public:
		int compare(void *o1, void *o2) {
			if (o1 == 0 || o2 == 0) {
				return 0;
			}
			if (*((long*) o1) < *((long*) o2)) {
			  return -1;
			}
			else {
			  return *((long*) o1) > *((long*) o2) ? 1 : 0;
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

		return compareBytes(o1Bytes->bytes, 0, o1Bytes->len, o2Bytes->bytes, 0, o2Bytes->len);
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
			long *l1 = (long*)o1;
			long *l2 = (long*)o2;
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
					char *buffer = new char[75];
					sprintf(buffer, "Unsupported datatype in KeyComparator: type=%d", dataTypes[i]);
					logError(env, buffer);
					delete[] buffer;
					break;
			}
		}
	}

	int compare(void *o1, void *o2) {
        for (int i = 0; i < fieldCount; i++) {
          int value = comparators[i]->compare(((void**)o1)[i], ((void**)o2)[i]);
          if (value != 0) {
            return value;
          }
        }
        return 0;
	}
};



struct PartitionedMap {
    int id = 0;
    struct my_node * maps[PARTITION_COUNT];
    std::mutex *locks = new std::mutex[PARTITION_COUNT];
    Comparator *comparator = 0;
	int *dataTypes = 0;
	int fieldCount = 0;

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
};

class PartitionResults {
  public:
    void*** keys;
    long* values;
    int count = -1;
    int posWithinPartition = 0;
    void** key = 0;
    long value = 0;
    int partition = 0;

    PartitionResults(int numRecordsPerPartition) {
    	values = new long[numRecordsPerPartition];
    	keys = new void**[numRecordsPerPartition];

  //      key = new void*[2];
        for (int i = 0; i < numRecordsPerPartition; i++) {
            keys[i] = 0;
            values[i] = 0;
        }
    }
    void** getKey() {
      return  key;
    }

    long getValue() {
      return value;
    }

    int getPartition() {
      return partition;
    }
  };

long readLong(uint8_t* b) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + 0] & 0xFF);
    }
    return result;
  }

void** deserializeKey(uint8_t* bytes, int len) {
    void** ret = new void*[1];
    ret[0] = (void*) readLong(bytes);
    return ret;
}

void writeLong(uint8_t* bytes, long v, int* offset) {
    for (int i = 7; i >= 0; i--) {
      bytes[i] = (uint8_t)(v & 0xFF);
      v >>= 8;
    }
        *offset += 8;
  }

void writeInt(uint8_t* bytes, int v, int* offset) {
    bytes[*offset] = (unsigned(v) >> 24) & 0xFF;
    bytes[*offset + 1] = (unsigned(v) >> 16) & 0xFF;
    bytes[*offset + 2] = (unsigned(v) >>  8) & 0xFF;
    bytes[*offset + 3] = (unsigned(v) >>  0) & 0xFF;
    *offset += 4;
}

uint8_t* serializeKey(void** key, int* size) {
    uint8_t* ret = new uint8_t[8];
    int offset = 0;
    writeLong(ret, (long)key[0], &offset);
    *size = 8;
    return ret;
}

jbyteArray serializeKeyValue(JNIEnv* env, const struct my_node* p) {
    int size = 0;
    uint8_t* keyBytes = serializeKey(p->key, &size);
    int totalBytes = 4 + size + 8;

    jbyte* bytes = new jbyte[totalBytes];
    jbyteArray ret = env->NewByteArray(totalBytes);

    int offset = 0;
    writeInt((uint8_t*)&bytes[0], 1, &offset);
    memcpy(&bytes[4], keyBytes, size);
    writeLong((uint8_t*)&bytes[size + 4], p->value, &offset);

    env->SetByteArrayRegion(ret, 0, totalBytes, bytes);
    delete[] bytes;
    return ret;
}

void** getSerializedKey(JNIEnv *env, jbyteArray jkeyBytes) {
    jsize keyLen = env->GetArrayLength(jkeyBytes);

   jboolean isCopy = false;
   jbyte *keyBytes = env->GetByteArrayElements(jkeyBytes, &isCopy);

   void ** ret = deserializeKey((uint8_t*)keyBytes, (int)keyLen);

   env->ReleaseByteArrayElements(jkeyBytes, keyBytes, 0);
//   if (isCopy) {
//   		delete[] keyBytes;
//   }
   return ret;
}

#define my_cmp(p, q) \
    doCompare(p, q)

float timedifference_msec(struct timeval t0, struct timeval t1)
{
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}

bool shutdown = false;




int hashKey(JNIEnv* env, PartitionedMap *map, void **key) {
	int hashCode = 1;
	for (int i = 0; i < map->fieldCount; i++) {
		int type = map->dataTypes[i];
		int currHashCode = 0;
		if (key[i] == 0) {
			continue;
		}
		switch (type) {
			case ROWID:
			case BIGINT:
			case DATE:
			case TIME:
			{
				currHashCode = *((long*)key[i]) ^ (*((long*)key[i]) >> 32);
			}
			break;
			case INTEGER:
			{
				currHashCode = *(int*)key[i];
			}
			break;
			case SMALLINT:
			{
				currHashCode = *(short*)key[i];
			}
			break;
			case TINYINT:
			{
				currHashCode = *(uint8_t*)key[i];
			}
			break;
			case BOOLEAN:
			case BIT:
			{
				currHashCode = *(bool*)key[i] ? 1231 : 1237;
			}
			break;
			case TIMESTAMP:
			{
				//printf("hash: entering");
				//fflush(stdout);
				//long l = ( ((long*)(key[i]))[0]);
				//printf("hash: key=%ld", (long)l);
				//fflush(stdout);
				currHashCode = ((long*)key[i])[0] ^ (((long*)key[i])[0] >> 32);
				//currHashCode = 31 * ((long*)key[i])[1] ^ (((long*)key[i])[1] >> 32) + currHashCode;
			}
			break;
			case DOUBLE:
			case FLOAT:
			{
				std::hash<double> hashVal;
				currHashCode = hashVal(*(double *)key[i]);
			}
			break;
			case REAL:
			{
				std::hash<float> hashVal;
				currHashCode = hashVal(*(float *)key[i]);
			}
			break;
			case NUMERIC:
			case DECIMAL:
			{
				std::vector<uint8_t> myVector(((std::string*)key[i])->begin(), ((std::string*)key[i])->end());
				uint8_t *p = &myVector[0];
				currHashCode = 1;
				int len = ((std::string*)key[i])->length();
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
				sb_utf8str *str = (sb_utf8str*)key[i];
				uint8_t *bytes = str->bytes;
				currHashCode = 1;
				int len = str->len;
				for (int j = 0; j < len; j++) {
					currHashCode = 31 * currHashCode + bytes[j];
				}
			}
			break;
			default:
				char *buffer = new char[75];
				sprintf(buffer, "Unsupported datatype in hashKey: type=%d", type);
				logError(env, buffer);
				delete[] buffer;
			break;

		}

		hashCode = 31 * hashCode + currHashCode;
	}
	return abs(hashCode);
}

std::string byte_seq_to_string( uint8_t bytes[], int n)
{
    std::ostringstream stm ;

    for( std::size_t i = 0 ; i < n ; i++ )
        stm << std::setw(2) << std::setfill( '0' ) << bytes[i];

    return stm.str() ;
}

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

void **javaKeyToNativeKey(JNIEnv *env, PartitionedMap *map, jobjectArray jKey) {
	int len = env->GetArrayLength(jKey);
	void **ret = new void*[map->fieldCount];
	for (int i = 0; i < map->fieldCount; i++) {
		int type = map->dataTypes[i];
		jobject obj = env->GetObjectArrayElement(jKey, i);
		if (obj == 0) {
			continue;
		}
		switch (type) {
			case ROWID:
			case BIGINT:
			{
				long l = env->CallLongMethod(obj, gLong_longValue_mid);
				long *pl = new long();
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
				long l = env->CallLongMethod(obj, gDate_getTime_mid);
				long *pl = new long();
				*pl = l;
				ret[i] = pl;
			}
			break;
			case TIME:
			{
				long l = env->CallLongMethod(obj, gTime_getTime_mid);
				long *pl = new long();
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
				long l = env->CallLongMethod(obj, gTimestamp_getTime_mid);
				int iVal = env->CallIntMethod(obj, gTimestamp_getNanos_mid);
				long *entry = new long[2];
				entry[0] = l;
				entry[1] = (long)iVal;
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
				jbyteArray objStr = (jbyteArray)obj;
				jsize size = env->GetArrayLength(objStr);
				jbyte *bytes = env->GetByteArrayElements(objStr, NULL);
				uint8_t *copy = new uint8_t[size + 1];
				memcpy( (void*)copy, (void*)bytes, size * sizeof(uint8_t));
				copy[size] = 0;
			    env->ReleaseByteArrayElements(objStr, bytes, 0);
			    sb_utf8str *str = new sb_utf8str();
			    str->bytes = copy;
			    str->len = size;
			    ret[i] = (void*)str;
			}
			break;
			default:
			{
				char *buffer = new char[75];
				sprintf(buffer, "Unsupported datatype in javaKeyToNativeKey: type=%d", type);
				logError(env, buffer);
				delete[] buffer;
			}
		}
	}
    return ret;
}

jobjectArray nativeKeyToJavaKey(JNIEnv *env, PartitionedMap *map, void **key) {

	jobjectArray ret = env->NewObjectArray(map->fieldCount, gObject_class, NULL);
    if (ret == 0) {
    	return 0;
    }

	for (int i = 0; i < map->fieldCount; i++) {
		if (key[i] == 0) {
			continue;
		}
		int type = map->dataTypes[i];
		switch (type) {
			case ROWID:
			case BIGINT:
			{
				jobject obj = env->CallStaticObjectMethod(gLong_class, gLong_valueOf_mid, *((long*)key[i]));
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case SMALLINT:
			{
				jobject obj = env->CallStaticObjectMethod(gShort_class, gShort_valueOf_mid, *((short*)key[i]));
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case INTEGER:
			{
				jobject obj = env->CallStaticObjectMethod(gInt_class, gInt_valueOf_mid, *(int*)key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TINYINT:
			{
				jobject obj = env->CallStaticObjectMethod(gInt_class, gByte_valueOf_mid, *(uint8_t*)key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case DATE:
			{
				jobject obj = env->NewObject(gDate_class, gDate_ctor, *(long*)key[i]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TIME:
			{
				jobject obj = env->NewObject(gTime_class, gTime_ctor, *(long*)key[i]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case TIMESTAMP:
			{
				jobject obj = env->NewObject(gTimestamp_class, gTimestamp_ctor, ((long*)key[i])[0]);
                env->CallVoidMethod(obj, gTimestamp_setNanos_mid, (int)((long*)key[i])[1]);
				env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case DOUBLE:
			case FLOAT:
			{
				jobject obj = env->CallStaticObjectMethod(gDouble_class, gDouble_valueOf_mid, *(double*)key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case REAL:
			{
				jobject obj = env->CallStaticObjectMethod(gFloat_class, gFloat_valueOf_mid, *(float*)key[i]);
			    env->SetObjectArrayElement(ret, i, obj);
			}
			break;
			case NUMERIC:
			case DECIMAL:
			{
				jstring jstr = env->NewStringUTF(((std::string*)key[i])->c_str());
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
				if (((sb_utf8str*)key[i])->len != 0) {
					jbyteArray arr = env->NewByteArray(((sb_utf8str*)key[i])->len);
					env->SetByteArrayRegion(arr, 0, ((sb_utf8str*)key[i])->len, (jbyte*)((sb_utf8str*)key[i])->bytes);
					env->SetObjectArrayElement(ret, i, (jobject)arr);
				}
			}
			break;
			default:
			{
				char *buffer = new char[75];
				sprintf(buffer, "Unsupported datatype in nativeKeyToJavaKey: type=%d", type);
				logError(env, buffer);
				delete[] buffer;
			}

		}
	}
    return ret;
}

void deleteKey(JNIEnv *env, PartitionedMap *map, void **key) {
	for (int i = 0; i < map->fieldCount; i++) {
		if (key[i] == 0) {
			continue;
		}
		int type = map->dataTypes[i];

		switch (type) {
			case ROWID:
			case BIGINT:
			case DATE:
			case TIME:
				delete (long*)key[i];
			break;
			case SMALLINT:
				delete (short*)key[i];
				break;
			case INTEGER:
				delete (int*)key[i];
				break;
			case TINYINT:
				delete (uint8_t*)key[i];
				break;
			case NUMERIC:
			case DECIMAL:
				delete (std::string *)key[i];
			break;
			case VARCHAR:
			case CHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
			{
				sb_utf8str *str = (sb_utf8str*)key[i];
				delete[] str->bytes;
				delete str;
			}
			break;
			case TIMESTAMP:
			{
				//printf("deleting");
				//fflush(stdout);
				long *entry = (long*)key[i];
				delete[] entry;
			}
			break;
			case DOUBLE:
			case FLOAT:
			{
				delete (double*)key[i];
			}
			break;
			case REAL:
			{
				delete (float*)key[i];
			}
			break;
			default:
			{
				if (env != 0) {
					char *buffer = new char[75];
					sprintf(buffer, "Unsupported datatype in deleteKey: type=%d", type);
					logError(env, buffer);
					delete[] buffer;
				}
			}

		}

	}
}


class DeleteQueueEntry {
	public:
		PartitionedMap *map = 0;
		void **key = 0;
		long time = 0;
		DeleteQueueEntry *next = 0;
		DeleteQueueEntry *prev = 0;
};

class DeleteQueue {
    std::mutex queueLock;
	DeleteQueueEntry *head = 0;
	DeleteQueueEntry *tail = 0;

public:
	void push(DeleteQueueEntry *entry) {
		{
			struct timeval tp;
			gettimeofday(&tp, NULL);
			long mslong = (long long) tp.tv_sec * 1000L + tp.tv_usec / 1000; //get current timestamp in milliseconds

			entry->time = mslong;
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

			struct timeval tp;
			gettimeofday(&tp, NULL);
			long mslong = (long long) tp.tv_sec * 1000L + tp.tv_usec / 1000; //get current timestamp in milliseconds
			if (mslong - head->time > 10000) {
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
	while (!shutdown) {
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

void pushDelete(PartitionedMap *map, void **key) {
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

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_initIndex
  (JNIEnv * env, jobject obj, jintArray dataTypes) {

    PartitionedMap *newMap = new PartitionedMap(env, dataTypes);
    return (long)newMap;
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put
  (JNIEnv * env, jobject obj, jlong indexId, jobjectArray jstartKey, jlong value) {
        struct my_node *q, *p = new my_node;

	try {
		long retValue = -1;
		PartitionedMap* map = (PartitionedMap*)indexId;

		void** startKey = javaKeyToNativeKey(env, map, jstartKey);

		if (startKey == 0) {
			printf("put, null key");
			fflush(stdout);
			return 0;
		}

		p->key = startKey;
		p->value = value;

		bool replaced = false;
		int partition = hashKey(env, map, startKey) % PARTITION_COUNT;
		{
			std::lock_guard<std::mutex> lock(map->locks[partition]);
			q = kavl_insert(my, &map->maps[partition], p, map->comparator, 0);
			if (p != q && q != 0) {
	//            	printf("replaced\n");
	//            	fflush(stdout);
				retValue = q->value;
				q->value = value;
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

		return retValue;
	}
	catch (const std::runtime_error& e) {
		//logError(env, "Error in jni 'put' call: ", e);
		return 0;
	}
}

 class SortedListNode {
 	public:
 	 SortedListNode *next = 0;
 	SortedListNode *prev = 0;
 	void **key = 0;
 	long value;
 	int partition;
 };


class SortedList {
 	SortedListNode *head = 0;
 	SortedListNode *tail = 0;
	bool ascending = true;
	PartitionedMap *map = 0;

 	public:

 	SortedList(PartitionedMap *map, bool ascending) {
 		this->map = map;
 		this->ascending = ascending;
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

 	~SortedList() {
 		SortedListNode *curr = head;
 		while (curr != 0) {
 			SortedListNode *next = curr->next;
 			delete curr;
 			curr = next;
 		}
 	}
 };


int nextEntries(PartitionedMap* map, int count, int numRecordsPerPartition, int partition, void*** keys, long* values, void** currKey, bool tail) {
    struct my_node keyNode;
    keyNode.key = currKey;
    int currOffset = 0;
    kavl_itr_t(my) itr;
    int countReturned = 0;

    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);

        if (tail) {
            kavl_itr_find(my, map->maps[partition], &keyNode, &itr, map->comparator);
        }
        else {
            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr, map->comparator);
        }

        bool found = 0;
        do {

            const struct my_node *p = kavl_at(&itr);
            if (p == NULL) {
//            	printf("at == 0\n");
//            	fflush(stdout);
                return countReturned;
            }
//            printf("found=%ld", (long)p->key[0]);
//            fflush(stdout);

            keys[currOffset] = p->key;
            values[currOffset] = p->value;
            //delete p;
            currOffset++;
            countReturned++;

            if (currOffset >= numRecordsPerPartition) {
                break;
            }
            if (tail) {
				found = kavl_itr_next(my, &itr);
			}
			else {
            	found = kavl_itr_prev(my, &itr);
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
        PartitionedMap* map, SortedList *sortedList, SortedListNode *node, std::vector<PartitionResults*> currResults,
        int count, int numRecordsPerPartition,
        PartitionResults* entry, int partition, void** currKey, jboolean first, bool tail) {

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

        if (!first || !tail) {
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
//    printf("key=%ld, partition=%d, pos=%d", (long)entry->key[0], partition, entry->posWithinPartition);
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

bool next(PartitionedMap* map, SortedList *sortedList, std::vector<PartitionResults*> currResults,
	int count, int numRecordsPerPartition, void*** keys, long* values,
    int* posInTopLevelArray, int* retCount, bool tail) {

    void** currKey = 0;
    long currValue = 0;
    int lowestPartition = 0;

    SortedListNode *node = sortedList->pop();
    if (node == 0) {
    	return false;
    }
    lowestPartition = node->partition;

    *posInTopLevelArray = lowestPartition;

    currKey = node->key;
    currValue = node->value;
    getNextEntryFromPartition(map, sortedList, node, currResults, count, numRecordsPerPartition, currResults[*posInTopLevelArray],
        *posInTopLevelArray, currKey, false, tail);

    if (currKey == 0) {
        return false;
    }
//    printf("curr_key=%ld\n", (long)currKey[0]);
//    fflush(stdout);
    keys[(*retCount)] = currKey;
    values[(*retCount)] = currValue;
    (*retCount)++;
    return true;
}

jint JNICALL tailHeadlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jstartKey, jint count, jboolean first, bool tail, jobjectArray jKeys, jlongArray jValues) {

	int numRecordsPerPartition = ceil((double)count / (double)PARTITION_COUNT) + 2;

    PartitionedMap* map = (PartitionedMap*)indexId;

   int posInTopLevelArray = 0;

   void** startKey = javaKeyToNativeKey(env, map, jstartKey);

	SortedList sortedList(map, tail);

   std::vector<PartitionResults*> currResults(PARTITION_COUNT);

   for (int partition = 0; partition < PARTITION_COUNT; partition++) {
       currResults[partition] = new PartitionResults(numRecordsPerPartition);
       getNextEntryFromPartition(map, &sortedList, new SortedListNode(), currResults, count, numRecordsPerPartition,
       currResults[partition], partition, startKey, first, tail);
   }


    void*** keys = new void**[count];
    for (int i = 0; i < count; i++) {
    	keys[i] = 0;
    }
    long* values = new long[count];
    int retCount = 0;
    bool firstEntry = true;
    while (retCount < count) {
        if (!next(map, &sortedList, currResults, count, numRecordsPerPartition, keys, values, &posInTopLevelArray, &retCount, tail)) {
            break;
        }
		if (firstEntry && (!first || !tail)) {
			if (map->comparator->compare((void**)keys[0], startKey) == 0) {
				retCount = 0;
			}
			firstEntry = false;
		}
    }


	jlong *valuesArray = env->GetLongArrayElements(jValues, 0);

	for (int i = 0; i < retCount; i++) {
		env->SetObjectArrayElement(jKeys, i, nativeKeyToJavaKey(env, map, keys[i]));
		valuesArray[i] = values[i];
	}

	env->ReleaseLongArrayElements(jValues, valuesArray, 0);


    delete[] keys;
    delete[] values;
    deleteKey(env, map, startKey);

    for (int i = 0; i < PARTITION_COUNT; i++) {
        delete[] currResults[i]->keys;
        delete[] currResults[i]->values;
        delete currResults[i];
    }

    return retCount;
  }

JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativePartitionedTree_headBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jobjectArray keys, jlongArray values) {
  return tailHeadlockArray(env, obj, indexId, startKey, count, first, false, keys, values);
}

JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlockArray
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray startKey, jint count, jboolean first, jobjectArray keys, jlongArray values) {
  return tailHeadlockArray(env, obj, indexId, startKey, count, first, true, keys, values);
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_remove
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = javaKeyToNativeKey(env, map, jKey);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
    long retValue = -1;
    int partition = hashKey(env, map, key) % PARTITION_COUNT;
	struct my_node keyNode;
    keyNode.key = key;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);

	    kavl_itr_t(my) itr;
        if (0 != kavl_itr_find(my, map->maps[partition], &keyNode, &itr, map->comparator)) {

			struct my_node* nodeRemoved = kavl_erase_my(&map->maps[partition], &keyNode, 0, map->comparator);
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
    return retValue;

}

void freeNode(PartitionedMap *map, struct my_node* p) {
    pushDelete(map, p->key);
    delete p;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_clear
  (JNIEnv *env, jobject obj, jlong indexId) {
  PartitionedMap *map = (PartitionedMap*)indexId;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        while (true) {
            if (map->maps[partition] == 0) {
                break;
            }
            //todo: free keys
            kavl_free(my_node, head, map, map->maps[partition], freeNode);

            map->maps[partition] = 0;
        }
    }
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_get
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey) {
    long offset = 0;
    kavl_itr_t(my) itr;

    PartitionedMap* map = (PartitionedMap*)indexId;

    void** startKey = javaKeyToNativeKey(env, map, jKey);

    int partition = hashKey(env, map, startKey) % PARTITION_COUNT;

    long retValue = -1;
    struct my_node keyNode;
    keyNode.key = startKey;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);

        if (0 != kavl_itr_find(my, map->maps[partition], &keyNode, &itr, map->comparator)) {
            const struct my_node *p = kavl_at(&itr);
            if (p != 0) {
                retValue = p->value;
            }
        }

    }
    deleteKey(env, map, startKey);
    return retValue;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

    PartitionedMap* map = (PartitionedMap*)indexId;

    void** key = javaKeyToNativeKey(env, map, jKey);

    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    struct my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
          std::lock_guard<std::mutex> lock(map->locks[partition]);

			kavl_itr_t(my) itr;
			bool found = kavl_itr_find(my, map->maps[partition], &keyNode, &itr, map->comparator);

			p = kavl_at(&itr);
			if (p != NULL) {
			//todo: should this return higher entry even if key doesn' exist?
				if (found) {
					if (kavl_itr_next(my, &itr)) {
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
   return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {
    PartitionedMap* map = (PartitionedMap*)indexId;

    void** key = javaKeyToNativeKey(env, map, jKey);

    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    struct my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr_t(my) itr;
    		std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr, map->comparator);
            p = kavl_at(&itr);
            if (p != NULL) {
                if (map->comparator->compare(p->key, key) == 0) {
                    if (kavl_itr_prev(my, &itr)) {
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

		return true;
    }

   	return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_floorEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {
    PartitionedMap* map = (PartitionedMap*)indexId;

    void** key = javaKeyToNativeKey(env, map, jKey);

	struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    struct my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr_t(my) itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr, map->comparator);

            p = kavl_at(&itr);

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

		return true;
    }

   return false;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_ceilingEntry
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKey, jobjectArray jKeys, jlongArray jValues) {

    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = javaKeyToNativeKey(env, map, jKey);

	struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    struct my_node lowest;
    lowest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        kavl_itr_t(my) itr;
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

            if (kavl_itr_find(my, map->maps[partition], &keyNode, &itr, map->comparator)) {
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

		return true;
    }

   	return 0;
}


JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_lastEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    const struct my_node *p = 0;
    struct my_node highest;
    highest.key = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
		kavl_itr_t(my) itr;
        {
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

			p = kavl_itr_last_my(map->maps[partition]);

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

		return true;
    }

   return 0;
}

JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativePartitionedTree_firstEntry2
  (JNIEnv *env, jobject obj, jlong indexId, jobjectArray jKeys, jlongArray jValues) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    const struct my_node *p = 0;
    struct my_node lowest;
    lowest.key = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        {
	        std::lock_guard<std::mutex> lock(map->locks[partition]);

            p = kavl_itr_first(my, map->maps[partition]);

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

		return true;
    }


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




