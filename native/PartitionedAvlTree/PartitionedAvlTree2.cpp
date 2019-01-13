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
#include "utf8.h"
//#include "kavl.h"

#include <jni.h>        // JNI header provided by JDK#include <stdio.h>      // C Standard IO Header
#include "com_sonicbase_index_NativePartitionedTree.h"


#include <pthread.h>

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

class rlock
{
    rwlock &m_;

public:
    rlock(rwlock &m)
      : m_(m)
    {
        reader_lock(&m_);
    }
    ~rlock()
    {
        reader_unlock(&m_);
    }
};

class wlock
{
    rwlock &m_;

public:
    wlock(rwlock &m)
      : m_(m)
    {
        writer_lock(&m_);
    }
    ~wlock()
    {
        writer_unlock(&m_);
    }
};

#define synchronized(m) \
    for(std::unique_lock<std::recursive_mutex> lk(m); lk; lk.unlock())


struct node
{
    long key;
    long data;

    int height;

    struct node* left = 0;
    struct node* right = 0;
};


const int PARTITION_COUNT = 32; // 64 for 17mil tailBlock reads/sec
const int NUM_RECORDS_PER_PARTITION = 16  + 2;
const int BLOCK_SIZE = (PARTITION_COUNT * NUM_RECORDS_PER_PARTITION);

struct PartitionedMap {
    int id = 0;
    struct node * maps[PARTITION_COUNT];
    struct rwlock* locks = new rwlock[PARTITION_COUNT];
    //std::recursive_mutex mutexes[PARTITION_COUNT];

    PartitionedMap() {
        for (int i = 0; i < PARTITION_COUNT; i++) {
            maps[i] = 0;
            rwlock_init(&locks[i]);
        }
    }
};

  class PartitionResults {
  public:
    long* keys = new long[NUM_RECORDS_PER_PARTITION];
    long values[NUM_RECORDS_PER_PARTITION];
    int count = -1;
    int posWithinPartition = 0;
    long key = 0;
    long value = 0;
    int partition = 0;

    PartitionResults() {
  //      key = new void*[2];
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            keys[i] = 0;
            values[i] = 0;
        }
    }
    long getKey() {
      return  key;
    }

    long getValue() {
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

int doCompare(const struct node* p, const struct node* q) {
	if (p == 0) {
		printf("doCompare, null  p");
		fflush(stdout);
	}
	if (q == 0) {
		printf("doCompare, null q");
		fflush(stdout);
	}
//	if (p->key == 0) {
//		printf("doCompare, null p->key");
//		fflush(stdout);
//	}
//	if (q->key == 0) {
//		printf("doCompare, null q->key");
//		fflush(stdout);
//	}
//	printf("lhs=%ld, rhs=%ld\n", (long)p->key[0], (long)q->key[0]);
//	fflush(stdout);
//	if (((long)p->key[0] < (long)q->key[0])) {
//		return 0;
//	}
//	if (((long)p->key[0] > (long)q->key[0])) {
//		return 1;
//	}
//	return 0;
    int cmp0 = (((long)q->key < (long)p->key) - ((long)p->key < (long)q->key));
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

int keyCompare(long p, long q) {
    int cmp0 = (((long)q < (long)p) - ((long)p < (long)q));
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

uint8_t* serializeKey(long key, int* size) {
    uint8_t* ret = new uint8_t[8];
    int offset = 0;
    writeLong(ret, key, &offset);
    *size = 8;
    return ret;
}

jbyteArray serializeKeyValue(JNIEnv* env, const struct node* p) {
    int size = 0;
    uint8_t* keyBytes = serializeKey(p->key, &size);
    int totalBytes = 4 + size + 8;

    jbyte* bytes = new jbyte[totalBytes];
    jbyteArray ret = env->NewByteArray(totalBytes);

    int offset = 0;
    writeInt((uint8_t*)&bytes[0], 1, &offset);
    memcpy(&bytes[4], keyBytes, size);
    writeLong((uint8_t*)&bytes[size + 4], p->data, &offset);

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

float timedifference_msec(struct timeval t0, struct timeval t1)
{
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}

//#####################################################################################################################

typedef struct node node;

node* new_node(long key, long data)
{
    node* p = new node();

    p -> key    = key;
    p -> data   = data;
    p -> height = 1;
    p -> left   = NULL;
    p -> right  = NULL;

    return p;
}

int max(int a, int b)
{
    return a > b ? a : b;
}

int height(node* p)
{
    return p ? p -> height : 0;
}

void recalc(node* p)
{
    p -> height = 1 + max(height(p -> left), height(p -> right));
}

node* rotate_right(node* p)
{
    node* q = p -> left;

    p -> left = q -> right;
    q -> right = p;

    recalc(p);
    recalc(q);

    return q;
}

node* rotate_left(node* p)
{
    node* q = p -> right;
    p -> right = q -> left;
    q -> left = p;

    recalc(p);
    recalc(q);

    return q;
}

node* balance(node* p)
{
    recalc(p);

    if ( height(p -> left) - height(p -> right) == 2 )
    {
        if ( height(p -> left -> right) > height(p -> left -> left) )
            p -> left = rotate_left(p -> left);
        return rotate_right(p);
    }
    else if ( height(p -> right) - height(p -> left) == 2 )
    {
        if ( height(p -> right -> left) > height(p -> right -> right) )
            p -> right = rotate_right(p -> right);
        return rotate_left(p);
    }

    return p;
}

#define MAX_STACK_DEPTH 64

	struct avl_itr {
		const node *stack[MAX_STACK_DEPTH], **top, *right, *left;/* _right_ points to the right child of *top */
	};

	const node* avl_itr_at(avl_itr *itr) {
		return (itr)->top < (itr)->stack? 0 : *(itr)->top;
	}

	int avl_itr_find(const node *root, const node *x, struct avl_itr *itr) {
		const node *p = root;
		itr->top = itr->stack - 1;
		while (p != 0) {
			int cmp;
			cmp = keyCompare(x->key, p->key);
			if (cmp < 0) *++itr->top = p, p = p->left;
			else if (cmp > 0) p = p->right;
			else break;
		}
		if (p) {
			*++itr->top = p;
			itr->right = p->right;
			return 1;
		} else if (itr->top >= itr->stack) {
			itr->right = (*itr->top)->right;
			return 0;
		} else return 0;
	}

int avl_itr_find_prev(const node* root, const node *x, struct avl_itr *itr) {
		const node *p = root;
		itr->top = itr->stack - 1;
		while (p != 0) {
			int cmp;
			cmp = keyCompare(x->key, p->key);
			if (cmp > 0) *++itr->top = p, p = p->right;
			else if (cmp < 0) p = p->left;
			else break;
		}
		if (p) {
			*++itr->top = p;
			itr->left = p->left;
			return 1;
		} else if (itr->top >= itr->stack) {
			itr->left = (*itr->top)->left;
			return 0;
		} else return 0;
	}

int avl_itr_prev(struct avl_itr *itr) {
		for (;;) {
			const node *p;
			for (p = itr->left, --itr->top; p; p = p->right)
				*++itr->top = p;
			if (itr->top < itr->stack) return 0;
			itr->left = (*itr->top)->left;
			return 1;
		}
	}

int avl_itr_next(struct avl_itr *itr) {
	for (;;) {
		const node *p;
		for (p = itr->right, --itr->top; p; p = p->left)
			*++itr->top = p;
		if (itr->top < itr->stack) return 0;
		itr->right = (*itr->top)->right;
		return 1;
	}
}

void avl_itr_first(const node *root, struct avl_itr *itr) {
		const node *p;
		for (itr->top = itr->stack - 1, p = root; p; p = p->left)
			*++itr->top = p;
		itr->right = (*itr->top)->right;
	}

void avl_itr_last(const node *root, struct avl_itr *itr) {
		const node *p;
		for (itr->top = itr->stack - 1, p = root; p; p = p->right)
			*++itr->top = p;
		itr->right = (*itr->top)->left;
	}


node* get(node* p, long key)
{
    if ( !p )
        return NULL;

    if ( key < p -> key )
        return get(p -> left, key);
    else if ( key > p -> key )
        return get(p -> right, key);
    else
        return p;
}

node* insert(struct node* p, long key, long data, long* prevData)
{
    if ( !p ) {
        return new_node(key, data);
	}

    if ( key < p -> key )
        p -> left = insert(p-> left, key, data, prevData);
    else if ( key > p -> key )
        p -> right = insert(p-> right, key, data, prevData);
    else {
		*prevData = p->data;
        p -> data = data;
	}

    return balance(p);
}

node* find_min(node* p)
{
    if ( p -> left != NULL )
        return find_min(p -> left);
    else
        return p;
}

node* remove_min(node* p)
{
    if ( p -> left == NULL )
        return p -> right;

    p -> left = remove_min(p -> left);
    return balance(p);
}

node* remove_item(node* p, long* existingData, long key)
{
    if ( !p )
        return NULL;

    if ( key < p -> key )
        p -> left = remove_item(p -> left, existingData, key);
    else if ( key > p -> key )
        p -> right = remove_item(p -> right, existingData, key);
    else
    {
        node* l = p -> left;
        node* r = p -> right;
        *existingData = p->data;
        delete p;

        if ( r == NULL )
            return l;

        node* m = find_min(r);
        m -> left = l;
        m -> right = remove_min(r);

        return balance(m);
    }

	return balance(p);
}

void free_tree(node* p)
{
    if ( !p )
        return;

    free_tree(p -> left);
    free_tree(p -> right);
    delete p;
}

//#####################################################################################################################

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

JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put
  (JNIEnv * env, jobject obj, jlong indexId, jbyteArray startKeyBytes, jlong value) {
        struct node *q;

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

        PartitionedMap* map = (PartitionedMap*)indexId;

        bool replaced = false;
        int partition = (long)startKey[0] % PARTITION_COUNT;
//        if (partition != 0) {
//            printf("partition non 0");
//            fflush(stdout);
//        }
        //synchronized (map->mutexes[partition]) {
        {
        wlock lock = wlock(map->locks[partition]);
            map->maps[partition] = insert(map->maps[partition], (long)startKey[0], value, &retValue);
//            if (q != 0) {
//            	printf("replaced\n");
//            	fflush(stdout);
//                retValue = q->data;
//                replaced = true;
////                delete[] q->key;
////                delete q;         // if already present, free
//            }
        }
//        if (replaced) {
//            delete q;
//        }
   return retValue;
        //}
}


int nextEntries(PartitionedMap* map, int partition, long* keys, long* values, long currKey, bool tail) {
    struct node keyNode;
    keyNode.key = currKey;
    int currOffset = 0;
    avl_itr itr;
    int countReturned = 0;

    {
        rlock lock = rlock(map->locks[partition]);

		const node *currNode = 0;
        if (tail) {
        	avl_itr_find(map->maps[partition], &keyNode, &itr);
        }
        else {
        	avl_itr_find(map->maps[partition], &keyNode, &itr);
        }

        bool found = 0;
        do {
        	currNode = avl_itr_at(&itr);
			if (currNode == 0) {
				return countReturned;
			}

            keys[currOffset] = currNode->key;
            values[currOffset] = currNode->data;
            currOffset++;
            countReturned++;

            if (currOffset >= NUM_RECORDS_PER_PARTITION) {
                break;
            }
            bool found = 0;
            if (tail) {
				found = avl_itr_next(&itr);
			}
			else {
				found = avl_itr_next(&itr);
		  	}
			if (!found) {
				break;
			}
        } while (true);
    }
    return countReturned;
}

void getNextEntryFromPartition(
        PartitionedMap* map, std::vector<PartitionResults*> currResults,
        PartitionResults* entry, int partition, long currKey, jboolean first, bool tail) {
    if (entry->count == -1 || entry->posWithinPartition == NUM_RECORDS_PER_PARTITION) {
        entry->posWithinPartition = 0;
        entry->count = nextEntries(map, partition, entry->keys, entry->values, currKey, tail);
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
            if (keyCompare(entry->keys[0], currKey) == 0) {
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

bool next(PartitionedMap* map, std::vector<PartitionResults*> currResults, long* keys, long* values,
    int offset, int* posInTopLevelArray, int* retCount, bool tail) {

    int partitionsChecked = 0;
    long currKey = 0;
    long currValue = 0;
    int lowestPartition = 0;
    long lowestKey = 0;
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
    getNextEntryFromPartition(map, currResults, currResults[*posInTopLevelArray],
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

    PartitionedMap* map = (PartitionedMap*)indexId;

   int posInTopLevelArray = 0;

   void** startKey = getSerializedKey(env, startKeyBytes);

   std::vector<PartitionResults*> currResults(PARTITION_COUNT);

   for (int partition = 0; partition < PARTITION_COUNT; partition++) {
       currResults[partition] = new PartitionResults();
       getNextEntryFromPartition(map, currResults, currResults[partition], partition, (long)startKey[0], first, tail);
   }

    long* keys = new long[BLOCK_SIZE];
    long* values = new long[BLOCK_SIZE];
    int retCount = 0;
    int perPart = NUM_RECORDS_PER_PARTITION - 2;

    for (int i = 0; i < PARTITION_COUNT * perPart; i++) {
        if (!next(map, currResults, keys, values, i, &posInTopLevelArray, &retCount, tail)) {
            break;
        }
    }

    int totalBytes = 0;
    void** serializedKeys = new void*[BLOCK_SIZE];
    int* serializedKeyLens = new int[BLOCK_SIZE];
    for (int i = 0; i < retCount; i++) {
        int size = 0;
        if (i >= BLOCK_SIZE) {
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
    {
        wlock lock = wlock(map->locks[partition]);

        map->maps[partition] = remove_item(map->maps[partition], &retValue, (long)key[0]);
    }
    delete[] key;
    return retValue;

}


JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_clear
  (JNIEnv *env, jobject obj, jlong indexId) {
  PartitionedMap* map = (PartitionedMap*)indexId;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        while (true) {
            if (map->maps[partition] == 0) {
                break;
            }
            free_tree(map->maps[partition]);

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

    void** startKey = getSerializedKey(env, startKeyBytes);

    int partition = (long)startKey[0] % PARTITION_COUNT;

    long retValue = -1;
    PartitionedMap* map = (PartitionedMap*)indexId;
    {
        rlock lock = rlock(map->locks[partition]);

        node* ret = get(map->maps[partition], (long)startKey[0]);

		if (ret != 0) {
			retValue = ret->data;
		}

    }
    delete[] startKey;
    return retValue;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_higherEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct node keyNode;
    keyNode.key = (long)key[0];
    const struct node *p = 0;
    const struct node *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            if (!avl_itr_find(map->maps[partition], &keyNode, &itr)) {
            	printf("really didn't find\n");
            }

            p = avl_itr_at(&itr);
            if (p == 0) {
            	printf("didn't find one\n");
            }
            else {
            	printf("found one=%ld\n", p->key);
                if (keyCompare(p->key, (long)key[0]) == 0) {
	            	printf("compared equal, one=%ld\n", p->key);
                    if (avl_itr_next(&itr)) {
	            		printf("got next, one=%ld\n", p->key);
                        p = avl_itr_at(&itr);
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

    if (lowest != 0) {
    	printf("returning key=%ld\n", lowest->key);
        return serializeKeyValue(env, lowest);
    }

   return 0;
}

JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_lowerEntry
  (JNIEnv *env, jobject obj, jlong indexId, jbyteArray keyBytes) {
    PartitionedMap* map = (PartitionedMap*)indexId;
    void** key = getSerializedKey(env, keyBytes);
    struct node keyNode;
    keyNode.key = (long)key[0];
    const struct node *p = 0;
    const struct node *highest = 0;
    //printf("lowerEntry: key=%ld\n", (long)key[0]);

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            avl_itr_find_prev(map->maps[partition], &keyNode, &itr);
            p = avl_itr_at(&itr);
            if (p != NULL) {
//                printf("key=%ld\n", (long)p->key[0]);
                if (keyCompare(p->key, (long)key[0]) == 0) {
                    if (avl_itr_prev(&itr)) {
                        p = avl_itr_at(&itr);
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
    struct node keyNode;
    keyNode.key = (long)key[0];
    const struct node *p = 0;
    const struct node *highest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            avl_itr_find_prev(map->maps[partition], &keyNode, &itr);

            p = avl_itr_at(&itr);
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
    struct node keyNode;
    keyNode.key = (long)key[0];
    const struct node *p = 0;
    const struct node *lowest = 0;

    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            avl_itr_find(map->maps[partition], &keyNode, &itr);

            p = avl_itr_at(&itr);
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
    const struct node *p = 0;
    const struct node *highest = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            avl_itr_last(map->maps[partition], &itr);

            p = avl_itr_at(&itr);
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
    const struct node *p = 0;
    const struct node *lowest = 0;
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
        avl_itr itr;
        {
            rlock lock = rlock(map->locks[partition]);

            avl_itr_first(map->maps[partition], &itr);

            p = avl_itr_at(&itr);
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




