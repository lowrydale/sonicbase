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
#include <math.h>
#include "utf8.h"
#include "kavl.h"

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


//obtained from https://github.com/zsummer/thread4z
class CLock
{
public:
	CLock();
	virtual ~CLock();
public:
	void Lock();
	void UnLock();
private:
#ifdef WIN32
	CRITICAL_SECTION m_crit;
#else
	pthread_mutex_t  m_crit;
#endif
};


CLock::CLock()
{
#ifdef WIN32
	InitializeCriticalSection(&m_crit);
#else
	//m_crit = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&m_crit, &attr);
	pthread_mutexattr_destroy(&attr);
#endif
}


CLock::~CLock()
{
#ifdef WIN32
	DeleteCriticalSection(&m_crit);
#else
	pthread_mutex_destroy(&m_crit);
#endif
}


void CLock::Lock()
{
#ifdef WIN32
	EnterCriticalSection(&m_crit);
#else
	pthread_mutex_lock(&m_crit);
#endif
}


void CLock::UnLock()
{
#ifdef WIN32
	LeaveCriticalSection(&m_crit);
#else
	pthread_mutex_unlock(&m_crit);
#endif
}


//class rlock
//{
//    CLock* m_;
//
//public:
//    rlock(CLock *m)
//      : m_(m)
//    {
//    	m_->Lock();
//    }
//    ~rlock()
//    {
//    	m_->UnLock();
//    }
//};
//
//class wlock
//{
//    CLock* m_;
//
//public:
//    wlock(CLock* m)
//      : m_(m)
//    {
//    	m_->Lock();
//    }
//    ~wlock()
//    {
//    	m_->UnLock();
//    }
//};
//

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


 struct my_node {
    void** key;
    KAVL_HEAD(struct my_node) head;
    long value;
};

struct PartitionedMap {
    int id = 0;
    struct my_node * maps[PARTITION_COUNT];
    std::mutex* locks = new std::mutex[PARTITION_COUNT];
    //struct rwlock* locks = new rwlock[PARTITION_COUNT];
//    CLock** locks = new CLock*[PARTITION_COUNT];
    //std::recursive_mutex mutexes[PARTITION_COUNT];
    
    PartitionedMap() {
        for (int i = 0; i < PARTITION_COUNT; i++) {
            maps[i] = 0;
//            locks[i] = new CLock();
      //      rwlock_init(&locks[i]);
      		//locks[i] = new std::mutex();
        }
    }
};

struct ByteArray {
	uint8_t* bytes;
	int len;
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

  class PartitionResultsBytes {
  public:
    void*** keys;
    ByteArray** values;
    int count = -1;
    int posWithinPartition = 0;
    void** key = 0;
    ByteArray* value = 0;
    int partition = 0;

    PartitionResultsBytes(int numRecordsPerPartition) {
    	values = new ByteArray*[numRecordsPerPartition];
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

    ByteArray* getValue() {
      return value;
    }

    int getPartition() {
      return partition;
    }
  };

struct sb_utf8str {
    void* bytes;
    int len;
};

int doCompare(const struct my_node* p, const struct my_node* q) {
	if (p == 0) {
		printf("doCompare, null  p");
		fflush(stdout);
	}
	if (q == 0) {
		printf("doCompare, null q");
		fflush(stdout);
	}
	if (p->key == 0) {
		printf("doCompare, null p->key");
		fflush(stdout);
	}
	if (q->key == 0) {
		printf("doCompare, null q->key");
		fflush(stdout);
	}
//	printf("lhs=%ld, rhs=%ld\n", (long)p->key[0], (long)q->key[0]);
//	fflush(stdout);
//	if (((long)p->key[0] < (long)q->key[0])) {
//		return 0;
//	}
//	if (((long)p->key[0] > (long)q->key[0])) {
//		return 1;
//	}
//	return 0;
    int cmp0 = (((long)q->key[0] < (long)p->key[0]) - ((long)p->key[0] < (long)q->key[0]));
    if (cmp0 != 0) {
        return cmp0;
    }
    return 0;
//    struct sb_utf8str* str0 = (struct sb_utf8str*)p->key[1]; 
//    struct sb_utf8str* str1 = (struct sb_utf8str*)q->key[1]; 
//    if (str0->len < str1->len) {
//        return -1;
//    } 
//    else if(str0->len > str1->len) {
//        return 1; 
//    } 
//    return utf8ncasecmp(str0->bytes, str1->bytes, str0->len);  
}

int keyCompare(void** p, void** q) {
    int cmp0 = (((long)q[0] < (long)p[0]) - ((long)p[0] < (long)q[0])); 
    if (cmp0 != 0) { 
        return cmp0;
    }
    return 0;
//    struct sb_utf8str* str0 = (struct sb_utf8str*)p[1]; 
//    struct sb_utf8str* str1 = (struct sb_utf8str*)q[1]; 
//    if (str0->len < str1->len) {
//        return -1;
//    } 
//    else if(str0->len > str1->len) {
//        return 1; 
//    } 
//    return utf8ncasecmp(str0->bytes, str1->bytes, str0->len);  
}

bool arrayComparator(void** i1, void** i2) 
{ 
    return keyCompare(i1, i1) < 0;
}

  long readLong(uint8_t* b) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + 0] & 0xFF);
    }
    return result;
  }

//long readLong(uint8_t* bytes) {
//    return (((long)bytes[0] << 56) +
//            ((long)(bytes[1] & 255) << 48) +
//            ((long)(bytes[2] & 255) << 40) +
//            ((long)(bytes[3] & 255) << 32) +
//            ((long)(bytes[4] & 255) << 24) +
//            ((bytes[5] & 255) << 16) +
//            ((bytes[6] & 255) <<  8) +
//            ((bytes[7] & 255) <<  0));
//}

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
    return ret;
}

void** getSerializedKey(JNIEnv *env, jbyteArray jkeyBytes) {
    jsize keyLen = env->GetArrayLength(jkeyBytes);

   jboolean isCopy = false;
   jbyte *keyBytes = env->GetByteArrayElements(jkeyBytes, &isCopy);
    
   void ** ret = deserializeKey((uint8_t*)keyBytes, (int)keyLen);
     
   env->ReleaseByteArrayElements(jkeyBytes, keyBytes, 0);
   return ret;
}

#define my_cmp(p, q) \
    doCompare(p, q)

KAVL_INIT(my, struct my_node, head, my_cmp)

float timedifference_msec(struct timeval t0, struct timeval t1)
{
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}

jclass cInteger;
jmethodID longValue;

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_initIndex
  (JNIEnv * env, jobject obj) {

    cInteger = (env)->FindClass("java/lang/Long");
    longValue = (env)->GetMethodID(cInteger, "longValue", "()J");

    PartitionedMap *newMap = new PartitionedMap();
    return (long)newMap;
}

void** getKey(JNIEnv * env, jobjectArray key) {
        void** newKey = new void*[2];
        
    jobject objInteger = (env)->GetObjectArrayElement(key, 0);    
    long i = (env)->CallLongMethod(objInteger, longValue);
        newKey[0] = (void*)i;

    jbyteArray objStr = (jbyteArray)(env)->GetObjectArrayElement(key, 1);    
     //const char *str = (env)->GetStringUTFChars((jstring)objStr, 0);
     
     jbyte* str = env->GetByteArrayElements(objStr, NULL);
     
     sb_utf8str* kstr = new sb_utf8str();
     kstr->bytes = utf8dup(str);
     kstr->len = env->GetArrayLength(objStr);
     newKey[1] = kstr;
     
     env->ReleaseByteArrayElements(objStr, str, JNI_ABORT);
     env->DeleteLocalRef(objStr);
     //ReleaseStringUTFChars(env, (jstring)objStr, str);
     return newKey;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_putBytes
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jbyteArray value) {
    struct my_node *q, *p = new my_node;

    void** startKey = getSerializedKey(env, startKeyBytes);

	jsize bytesLen = env->GetArrayLength(value);

	jboolean isCopy = false;
	jbyte *srcBytes = env->GetByteArrayElements(value, &isCopy);

	ByteArray* byteArray = new ByteArray();

	byteArray->bytes = new uint8_t[bytesLen];
	byteArray->len = bytesLen;

	memcpy(byteArray->bytes, srcBytes, bytesLen);

	env->ReleaseByteArrayElements(value, srcBytes, 0);

	if (startKey == 0) {
		printf("put, null key");
		fflush(stdout);
	}

    long retValue = -1;
        //void** newKey = getKey(env, key);
//        std::string str = std::to_string();
//        char * cstr = new char [str.length()+1];
//        std::strcpy (cstr, str.c_str());
//
//        newKey[1] = cstr;

        p->key = startKey;
        p->value = (long)byteArray;

        PartitionedMap* map = (PartitionedMap*)indexId;

        bool replaced = false;
        int partition = (long)p->key[0] % PARTITION_COUNT;
//        if (partition != 0) {
//            printf("partition non 0");
//            fflush(stdout);
//        }
        //synchronized (map->mutexes[partition]) {
        {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
        //wlock lock = wlock(map->locks[partition]);
            q = kavl_insert(my, &map->maps[partition], p, 0);
            if (p != q && q != 0) {
//            	printf("replaced\n");
//            	fflush(stdout);
                retValue = 0;//q->value;
                q->value = (long)byteArray;
                replaced = true;
//                delete[] q->key;
//                delete q;         // if already present, free
            }
        }
        if (replaced) {
        	delete[] p->key;
            delete p;
        }
   return 0;//retValue;
}


JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put
  (JNIEnv * env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jlong value) {
        struct my_node *q, *p = new my_node;

    void** startKey = getSerializedKey(env, startKeyBytes);

	if (startKey == 0) {
		printf("put, null key");
		fflush(stdout);
	}

    long retValue = -1;
        //void** newKey = getKey(env, key);
//        std::string str = std::to_string();
//        char * cstr = new char [str.length()+1];
//        std::strcpy (cstr, str.c_str());
//
//        newKey[1] = cstr;

        p->key = startKey;
        p->value = value;

        PartitionedMap* map = (PartitionedMap*)indexId;

        bool replaced = false;
        int partition = (long)p->key[0] % PARTITION_COUNT;
//        if (partition != 0) {
//            printf("partition non 0");
//            fflush(stdout);
//        }
        //synchronized (map->mutexes[partition]) {
        {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
        //wlock lock = wlock(map->locks[partition]);
            q = kavl_insert(my, &map->maps[partition], p, 0);
            if (p != q && q != 0) {
//            	printf("replaced\n");
//            	fflush(stdout);
                retValue = q->value;
                q->value = value;
                replaced = true;
//                delete[] q->key;
//                delete q;         // if already present, free
            }
        }
        if (replaced) {
        	delete[] p->key;
            delete p;
        }
   return retValue;
        //}
}


int nextEntries(PartitionedMap* map, int count, int numRecordsPerPartition, int partition, void*** keys, long* values, void** currKey, bool tail) {
    struct my_node keyNode;
    keyNode.key = currKey;
    int currOffset = 0;
    kavl_itr_t(my) itr;
    int countReturned = 0;
    
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
//        rlock lock = rlock(map->locks[partition]);

        if (tail) {
            kavl_itr_find(my, map->maps[partition], &keyNode, &itr);
        }
        else {
            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr);
        }

        bool found = 0;
        do {
            const struct my_node *p = kavl_at(&itr);
            if (p == NULL) {
                return countReturned;
            }
//            printf("found=%ld", (long)p->key[0]);
//            fflush(stdout);

            keys[currOffset] = p->key;
            values[currOffset] = p->value;
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

        } while (found);
    }
    return countReturned;
}

void getNextEntryFromPartition(
        PartitionedMap* map, std::vector<PartitionResults*> currResults,
        int count, int numRecordsPerPartition,
        PartitionResults* entry, int partition, void** currKey, jboolean first, bool tail) {
    if (entry->count == -1 || entry->posWithinPartition == numRecordsPerPartition) {
        entry->posWithinPartition = 0;
        entry->count = nextEntries(map, count, numRecordsPerPartition, partition, entry->keys, entry->values, currKey, tail);
//        printf("count=%d\n", entry->count);
//        fflush(stdout);
        if (entry->count == 0) {
            entry->key = 0;
            entry->value = 0;
            entry->partition = partition;
            entry->posWithinPartition = 0;
            return;
        }

        if (!first || !tail) {
            if (keyCompare((void**)entry->keys[0], currKey) == 0) {
                entry->posWithinPartition = 1;
            }
        }
    }
    if (entry->posWithinPartition > entry->count - 1) {
        entry->count = 0;
        entry->key = 0;
        entry->value = 0;
        entry->partition = partition;
        entry->posWithinPartition = 0;
        return;
    }
    entry->key = entry->keys[entry->posWithinPartition];
    entry->value = entry->values[entry->posWithinPartition];
//    printf("key=%ld, partition=%d, pos=%d", (long)entry->key[0], partition, entry->posWithinPartition);
//    fflush(stdout);
    entry->posWithinPartition++;
    entry->partition = partition;
}

  bool partitionResultsComparator(PartitionResults* p, PartitionResults* q) {
    if (p->key == 0) {
        return false;
    }
    if (q->key == 0) {
        return true;
    }
    int cmp = keyCompare(p->key, q->key);
    if (cmp < 0) {
        return true;
    }
    return false;
  }
  
bool next(PartitionedMap* map, std::vector<PartitionResults*> currResults,
	int count, int numRecordsPerPartition, void*** keys, long* values,
    int offset, int* posInTopLevelArray, int* retCount, bool tail) {

    int partitionsChecked = 0;
    void** currKey = 0;
    long currValue = 0;
    int lowestPartition = 0;
    void** lowestKey = 0;
    for (int i = 0; i < PARTITION_COUNT; i++) {
        if (currResults[i]->count == 0) {
            continue;
        }
        if (tail) {
            if (lowestKey == 0 || keyCompare(currResults[i]->key, lowestKey) < 0) {
                lowestKey = currResults[i]->key;
                lowestPartition = i;
            }
        }
        else {
            if (lowestKey == 0 || keyCompare(currResults[i]->key, lowestKey) > 0) {
                lowestKey = currResults[i]->key;
                lowestPartition = i;
            }
        }
    }

    *posInTopLevelArray = lowestPartition;

    currKey = currResults[*posInTopLevelArray]->getKey();
    currValue = currResults[*posInTopLevelArray]->getValue();
    getNextEntryFromPartition(map, currResults, count, numRecordsPerPartition, currResults[*posInTopLevelArray],
        *posInTopLevelArray, currKey, false, tail);

    if (currKey == 0) {
        return false;
    }
//    printf("curr_key=%ld\n", (long)currKey[0]);
//    fflush(stdout);
    keys[offset] = currKey;
    values[offset] = currValue;
    (*retCount)++;
    return true;
}


jbyteArray tailHeadBlock(JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jint count,
    jboolean first, bool tail) {

	int numRecordsPerPartition = ceil((double)count / (double)PARTITION_COUNT) + 2;

    PartitionedMap* map = (PartitionedMap*)indexId;

   int posInTopLevelArray = 0;
   
   void** startKey = getSerializedKey(env, startKeyBytes);

   std::vector<PartitionResults*> currResults(PARTITION_COUNT);

   for (int partition = 0; partition < PARTITION_COUNT; partition++) {
       currResults[partition] = new PartitionResults(numRecordsPerPartition);
       getNextEntryFromPartition(map, currResults, count, numRecordsPerPartition, currResults[partition], partition, startKey, first, tail);
   }

    void*** keys = new void**[count];
    long* values = new long[count];
    int retCount = 0;
    int perPart = numRecordsPerPartition - 2;

    for (int i = 0; i < PARTITION_COUNT * perPart; i++) {
        if (!next(map, currResults, count, numRecordsPerPartition, keys, values, i, &posInTopLevelArray, &retCount, tail)) {
            break;
        }
    }

    int totalBytes = 0;
    void** serializedKeys = new void*[count];
    int* serializedKeyLens = new int[count];
    for (int i = 0; i < retCount; i++) {
        int size = 0;
        if (i >= count) {
            break;
        }
        if (keys[i] == 0) {
            break;
        }
        uint8_t* bytes = serializeKey(keys[i], &size);
        serializedKeys[i] = bytes;
        totalBytes += size;
        serializedKeyLens[i] = size;
    }

    totalBytes += 4 + (4 * retCount) + (8 * retCount);

    jbyte* bytes = new jbyte[totalBytes];
    
    jbyteArray ret = env->NewByteArray(totalBytes);

    uint8_t* lenBuffer = new uint8_t[4];
    int offset = 0;
    int totalOffset = 0;
    writeInt(lenBuffer, retCount, &offset);
    memcpy(&bytes[totalOffset], lenBuffer, offset);
    totalOffset += offset;
    for (int i = 0; i < retCount; i++) {
        offset = 0;
        writeInt(lenBuffer, serializedKeyLens[i], &offset);
        memcpy(&bytes[totalOffset], lenBuffer, offset);
        totalOffset += offset;
        
        memcpy(&bytes[totalOffset], serializedKeys[i], serializedKeyLens[i]); 
        totalOffset += serializedKeyLens[i];
        offset = 0;
        writeLong((uint8_t*)&bytes[totalOffset], values[i], &offset);
        totalOffset += offset;
    }

    env->SetByteArrayRegion(ret, 0, totalBytes, bytes);

    delete[] bytes;
    
    for (int i = 0; i < retCount; i++) {
        free(serializedKeys[i]);
    }

    delete[] keys;
    delete[] values;
    delete[] serializedKeys;
    delete[] serializedKeyLens;
    delete[] lenBuffer;
    delete[] startKey;
    
    for (int i = 0; i < PARTITION_COUNT; i++) {
        delete[] currResults[i]->keys;
        delete[] currResults[i]->values;
        delete currResults[i];
    }

    return ret;
}


int nextEntriesBytes(PartitionedMap* map, int count, int numRecordsPerPartition, int partition, void*** keys, ByteArray** values, void** currKey, bool tail) {
    struct my_node keyNode;
    keyNode.key = currKey;
    int currOffset = 0;
    kavl_itr_t(my) itr;
    int countReturned = 0;

    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
//        rlock lock = rlock(map->locks[partition]);

        if (tail) {
            kavl_itr_find(my, map->maps[partition], &keyNode, &itr);
        }
        else {
            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr);
        }

        bool found = 0;
        do {
            const struct my_node *p = kavl_at(&itr);
            if (p == NULL) {
                return countReturned;
            }
//            printf("found=%ld", (long)p->key[0]);
//            fflush(stdout);

            keys[currOffset] = p->key;
            values[currOffset] = new ByteArray();
//            values[currOffset]->len = ((ByteArray*)p->value)->len;
//            values[currOffset]->bytes = new uint8_t[((ByteArray*)p->value)->len];
//            memcpy(values[currOffset]->bytes, ((ByteArray*)p->value)->bytes, ((ByteArray*)p->value)->len);
			values[currOffset]->len = ((ByteArray*)p->value)->len;
			values[currOffset]->bytes = ((ByteArray*)p->value)->bytes;
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

        } while (found);
    }
    return countReturned;
}

void getNextEntryFromPartitionBytes(
        PartitionedMap* map, std::vector<PartitionResultsBytes*> currResults,
        int count, int numRecordsPerPartition,
        PartitionResultsBytes* entry, int partition, void** currKey, jboolean first, bool tail) {
    if (entry->count == -1 || entry->posWithinPartition == numRecordsPerPartition) {
        entry->posWithinPartition = 0;
        entry->count = nextEntriesBytes(map, count, numRecordsPerPartition, partition, entry->keys, entry->values, currKey, tail);
//        printf("count=%d\n", entry->count);
//        fflush(stdout);
        if (entry->count == 0) {
            entry->key = 0;
            entry->value = 0;
            entry->partition = partition;
            entry->posWithinPartition = 0;
            return;
        }

        if (!first || !tail) {
            if (keyCompare((void**)entry->keys[0], currKey) == 0) {
            	//printf("shifted: key=%ld", (long)currKey[0]);
                entry->posWithinPartition = 1;
            }
        }
    }
    if (entry->posWithinPartition > entry->count - 1) {
        entry->count = 0;
        entry->key = 0;
        entry->value = 0;
        entry->partition = partition;
        entry->posWithinPartition = 0;
        return;
    }
    entry->key = entry->keys[entry->posWithinPartition];
    entry->value = entry->values[entry->posWithinPartition];
//    printf("key=%ld, partition=%d, pos=%d", (long)entry->key[0], partition, entry->posWithinPartition);
//    fflush(stdout);
    entry->posWithinPartition++;
    entry->partition = partition;
}

bool nextBytes(PartitionedMap* map, std::vector<PartitionResultsBytes*> currResults,
	int count, int numRecordsPerPartition, void*** keys, ByteArray** values,
    int offset, int* posInTopLevelArray, int* retCount, bool tail) {

    int partitionsChecked = 0;
    void** currKey = 0;
    ByteArray* currValue = 0;
    int lowestPartition = 0;
    void** lowestKey = 0;
    for (int i = 0; i < PARTITION_COUNT; i++) {
        if (currResults[i]->count == 0) {
            continue;
        }
        if (tail) {
            if (lowestKey == 0 || keyCompare(currResults[i]->key, lowestKey) < 0) {
                lowestKey = currResults[i]->key;
                lowestPartition = i;
            }
        }
        else {
            if (lowestKey == 0 || keyCompare(currResults[i]->key, lowestKey) > 0) {
                lowestKey = currResults[i]->key;
                lowestPartition = i;
            }
        }
    }

    *posInTopLevelArray = lowestPartition;

    currKey = currResults[*posInTopLevelArray]->getKey();
    currValue = currResults[*posInTopLevelArray]->getValue();
    getNextEntryFromPartitionBytes(map, currResults, count, numRecordsPerPartition, currResults[*posInTopLevelArray],
        *posInTopLevelArray, currKey, false, tail);

    if (currKey == 0) {
        return false;
    }
//    printf("curr_key=%ld\n", (long)currKey[0]);
//    fflush(stdout);
    keys[offset] = currKey;
    values[offset] = currValue;
    (*retCount)++;
    return true;
}


JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlockBytes
	(JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jint count, jboolean first) {

	bool tail = 1;
	int numRecordsPerPartition = ceil((double)count / (double)PARTITION_COUNT) + 2;

    PartitionedMap* map = (PartitionedMap*)indexId;

   int posInTopLevelArray = 0;

   void** startKey = getSerializedKey(env, startKeyBytes);

   std::vector<PartitionResultsBytes*> currResults(PARTITION_COUNT);

//	printf("getting keys: key=%ld\n", (long)startKey[0]);
//	fflush(stdout);
   for (int partition = 0; partition < PARTITION_COUNT; partition++) {
       currResults[partition] = new PartitionResultsBytes(numRecordsPerPartition);
       getNextEntryFromPartitionBytes(map, currResults, count, numRecordsPerPartition, currResults[partition], partition, startKey, first, tail);
//       printf("partition=%d, key=%ld\n", partition, (long)currResults[partition]->key[0]);
//       fflush(stdout);
   }

    void*** keys = new void**[count];
    ByteArray** values = new ByteArray*[count];
    int retCount = 0;
    int perPart = numRecordsPerPartition - 2;

    for (int i = 0; i < PARTITION_COUNT * perPart; i++) {
        if (!nextBytes(map, currResults, count, numRecordsPerPartition, keys, values, i, &posInTopLevelArray, &retCount, tail)) {
            break;
        }
    }

    int totalBytes = 0;
    void** serializedKeys = new void*[count];
    int* serializedKeyLens = new int[count];
    for (int i = 0; i < retCount; i++) {
        int size = 0;
        if (i >= count) {
            break;
        }
        if (keys[i] == 0) {
            break;
        }
        uint8_t* bytes = serializeKey(keys[i], &size);
        serializedKeys[i] = bytes;
        totalBytes += size;
        serializedKeyLens[i] = size;
        totalBytes += values[i]->len;
    }

    totalBytes += 4 + (4 * retCount) + (4 * retCount);

    jbyte* bytes = new jbyte[totalBytes];

    jbyteArray ret = env->NewByteArray(totalBytes);

    uint8_t* lenBuffer = new uint8_t[4];
    int offset = 0;
    int totalOffset = 0;
    writeInt(lenBuffer, retCount, &offset);
    memcpy(&bytes[totalOffset], lenBuffer, offset);
    totalOffset += offset;
    for (int i = 0; i < retCount; i++) {
        offset = 0;
        writeInt(lenBuffer, serializedKeyLens[i], &offset);
        memcpy(&bytes[totalOffset], lenBuffer, offset);
        totalOffset += offset;

        memcpy(&bytes[totalOffset], serializedKeys[i], serializedKeyLens[i]);
        totalOffset += serializedKeyLens[i];
        offset = 0;

        writeInt(lenBuffer, values[i]->len, &offset);
        memcpy(&bytes[totalOffset], lenBuffer, offset);
        totalOffset += offset;

        memcpy(&bytes[totalOffset], values[i]->bytes, values[i]->len);
        totalOffset += values[i]->len;
    }

    env->SetByteArrayRegion(ret, 0, totalBytes, bytes);

    delete[] bytes;

    for (int i = 0; i < retCount; i++) {
        free(serializedKeys[i]);
    }

    delete[] keys;
    for (int i = 0; i < count; i++) {
    	delete values[i];
    }
    delete[] values;
    delete[] serializedKeys;
    delete[] serializedKeyLens;
    delete[] lenBuffer;
    delete[] startKey;

    for (int i = 0; i < PARTITION_COUNT; i++) {
        delete[] currResults[i]->keys;
        delete[] currResults[i]->values;
        delete currResults[i];
    }

    return ret;
}



JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlock
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jint count, jboolean first) {
    return tailHeadBlock(env, obj, indexId, startKeyBytes, count, first, true);
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_headBlock
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jint count, jboolean first) {
    return tailHeadBlock(env, obj, indexId, startKeyBytes, count, first, false);
}



JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_remove
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
   void** key = getSerializedKey(env, keyBytes);

	if (key == 0) {
		printf("remove, null key");
		fflush(stdout);
	}
    long retValue = -1;
    int partition = (long)key[0] % PARTITION_COUNT;
    struct my_node keyNode;
    keyNode.key = key;
    {
            std::lock_guard<std::mutex> lock(map->locks[partition]);
//
//        wlock lock = wlock(map->locks[partition]);

        struct my_node* nodeRemoved = kavl_erase_my(&map->maps[partition], &keyNode, 0);
        if (nodeRemoved != 0) {
            retValue = nodeRemoved->value;
            delete[] nodeRemoved->key;
            delete nodeRemoved;
        }
    }
    delete[] key;
    return retValue;

}

void freeNode(struct my_node* p) {
    delete[] p->key;
    delete p;
}

JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_clear
  (JNIEnv *env, jobject obj, jlong indexId) {
  PartitionedMap* map = (PartitionedMap*)indexId;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        while (true) {
            if (map->maps[partition] == 0) {
                break;
            }
            kavl_free(my_node, head, map->maps[partition], freeNode);

            map->maps[partition] = 0;

//            struct my_node* p = kavl_erase_first(my, &map->maps[partition]);
//            if (p == 0) {
//                break;
//            }
//            delete[] p->key;
//            delete p;
        }
    }
}

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_get
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray startKeyBytes) {
    long offset = 0;
    kavl_itr_t(my) itr;

    void** startKey = getSerializedKey(env, startKeyBytes);
    
    int partition = (long)startKey[0] % PARTITION_COUNT;

    long retValue = -1;
    struct my_node keyNode;
    keyNode.key = startKey;
    PartitionedMap* map = (PartitionedMap*)indexId;
    {
        std::lock_guard<std::mutex> lock(map->locks[partition]);
//        rlock lock = rlock(map->locks[partition]);

        if (0 != kavl_itr_find(my, map->maps[partition], &keyNode, &itr)) {
            const struct my_node *p = kavl_at(&itr);
            if (p != 0) {
                retValue = p->value;
            }
        }

    }
    delete[] startKey;
    return retValue;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    const struct my_node *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
          std::lock_guard<std::mutex> lock(map->locks[partition]);
//          rlock lock = rlock(map->locks[partition]);

            kavl_itr_find(my, map->maps[partition], &keyNode, &itr);

            p = kavl_at(&itr);
            if (p != NULL) {
                if (keyCompare(p->key, key) == 0) {
                    if (kavl_itr_next(my, &itr)) {
                        p = kavl_at(&itr);
                    }
                    else {
                        p = 0;
                    }
                }
            }
            if (lowest == 0) {
                lowest = p;
            }
            else if (p != 0 && keyCompare(p->key, lowest->key) < 0) {
                lowest = p;
            }
        }
    }

    delete[] key;

    if (lowest != NULL) {
        return serializeKeyValue(env, lowest);
    }
   
   return 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    const struct my_node *highest = 0;
    //printf("lowerEntry: key=%ld\n", (long)key[0]);

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
          std::lock_guard<std::mutex> lock(map->locks[partition]);
//          rlock lock = rlock(map->locks[partition]);

            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr);
            p = kavl_at(&itr);
            if (p != NULL) {
//                printf("key=%ld\n", (long)p->key[0]);
                if (keyCompare(p->key, key) == 0) {
                    if (kavl_itr_prev(my, &itr)) {
                        p = kavl_at(&itr);
                    }
                    else {
                        p = 0;
                    }
                }
            }
//            else {
//                printf("prev=null\n");
//            }
            if (highest == 0) {
                highest = p;
            }
            else if (p != 0 && keyCompare(p->key, highest->key) > 0) {
                highest = p;
            }
        }
    }
    delete[] key;

    if (highest != 0) {
//        printf("lower=%ld", (long)highest->key[0]);
//        fflush(stdout);
        return serializeKeyValue(env, highest);
    }

   return 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_floorEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    const struct my_node *highest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
//            rlock lock = rlock(map->locks[partition]);
        std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_find_prev(my, map->maps[partition], &keyNode, &itr);

            p = kavl_at(&itr);
            if (highest == 0) {
                highest = p;
            }
            else if (p != 0 && keyCompare(p->key, highest->key) > 0) {
                highest = p;
            }
        }
    }
    delete[] key;

    if (highest != 0) {
//        printf("lower=%ld", (long)highest->key[0]);
//        fflush(stdout);
        return serializeKeyValue(env, highest);
    }

   return 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_ceilingEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct my_node keyNode;
    keyNode.key = key;
    const struct my_node *p = 0;
    const struct my_node *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
  //          rlock lock = rlock(map->locks[partition]);
        std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_find(my, map->maps[partition], &keyNode, &itr);

            p = kavl_at(&itr);
            if (lowest == 0) {
                lowest = p;
            }
            else if (p != 0 && keyCompare(p->key, lowest->key) < 0) {
                lowest = p;
            }
        }
    }
    delete[] key;

    if (lowest != 0) {
//        printf("lower=%ld", (long)lowest->key[0]);
//        fflush(stdout);
        return serializeKeyValue(env, lowest);
    }

   return 0;
}


JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_lastEntry2
  (JNIEnv *env, jobject obj, jlong indexId) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    const struct my_node *p = 0;
    const struct my_node *highest = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
//            rlock lock = rlock(map->locks[partition]);
        std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_last(my, map->maps[partition], &itr);

            p = kavl_at(&itr);
        }
        if (highest == 0) {
            highest = p;
        }
        else if (p != 0 && keyCompare(p->key, highest->key) > 0) {
            highest = p;
        }
    }

    if (highest != 0) {
//        printf("highest=%ld", (long)highest->key[0]);
//        fflush(stdout);
        return serializeKeyValue(env, highest);
    }

   return 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_firstEntry2
  (JNIEnv *env, jobject obj, jlong indexId) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    const struct my_node *p = 0;
    const struct my_node *lowest = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        kavl_itr_t(my) itr;
        {
//            rlock lock = rlock(map->locks[partition]);
        std::lock_guard<std::mutex> lock(map->locks[partition]);

            kavl_itr_first(my, map->maps[partition], &itr);

            p = kavl_at(&itr);
        }
        if (lowest == 0) {
            lowest = p;
        }
        else if (p != 0 && keyCompare(p->key, lowest->key) < 0) {
            lowest = p;
        }
    }

    if (lowest != 0) {
        return serializeKeyValue(env, lowest);
    }

   return 0;
}




