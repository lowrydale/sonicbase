#ifndef SONICBASE_H
#define SONICBASE_H
#include "Comparator.h"
#include "Comparable.h"
#include "ObjectPool.h"
#include "BigDecimal.h"


using namespace skiplist;

extern jmethodID gNativePartitionedTree_logError_mid;
extern jclass gNativePartitioned_class;
extern jclass gObject_class;

extern jclass gLong_class;
extern jmethodID gLong_longValue_mid;
extern jmethodID gLong_valueOf_mid;

extern jclass gShort_class;
extern jmethodID gShort_shortValue_mid;
extern jmethodID gShort_valueOf_mid;

extern jclass gInt_class;
extern jmethodID gInt_intValue_mid;
extern jmethodID gInt_valueOf_mid;

extern jclass gByte_class;
extern jmethodID gByte_byteValue_mid;
extern jmethodID gByte_valueOf_mid;

extern jclass gBool_class;
extern jmethodID gBool_boolValue_mid;
extern jmethodID gBool_valueOf_mid;

extern jclass gDouble_class;
extern jmethodID gDouble_doubleValue_mid;
extern jmethodID gDouble_valueOf_mid;

extern jclass gFloat_class;
extern jmethodID gFloat_floatValue_mid;
extern jmethodID gFloat_valueOf_mid;

extern jclass gTimestamp_class;
extern jmethodID gTimestamp_ctor;
extern jmethodID gTimestamp_setNanos_mid;
extern jmethodID gTimestamp_getTime_mid;
extern jmethodID gTimestamp_getNanos_mid;
extern jmethodID gTimestamp_toString_mid;
extern jmethodID gTimestamp_valueOf_mid;

extern jclass gDate_class;
extern jmethodID gDate_ctor;
extern jmethodID gDate_getTime_mid;

extern jclass gTime_class;
extern jmethodID gTime_ctor;
extern jmethodID gTime_getTime_mid;

extern jclass gBigDecimal_class;
extern jmethodID gBigDecimal_ctor;
extern jmethodID gBigDecimal_toPlainString_mid;



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

#ifdef _WIN32
#define smin(x, y) min(x, y)
#else
#define smin(x, y) std::min(x, y)
#endif

struct ByteArray {
	uint8_t* bytes;
	int len;
};

struct sb_utf8str {
    uint16_t *bytes;
    int len;
};

long readLong(std::vector<jbyte> &vector, int offset);

jboolean writeLong(long long int value, jbyte *bytes, int *offset, int len);

jboolean writeInt(int value, jbyte *p, int *offset, int len);

jboolean writeShort(int value, jbyte *p, int *offset, int len);

long long int readUnsignedVarLong(std::vector<jbyte> &vector, int *offset);

long long int readVarLong(std::vector<jbyte> &vector, int *offset);

void writeVarLong(long long int value, jbyte *bytes, int *offset);

uint64_t doubleToLongBits(double value);

unsigned float_to_bits(float x);

class KAVLComparator {
public:
	virtual int compare(void *o1, void *o2) = 0;
};

class KeyComparator : public KAVLComparator {
	public:
		int fieldCount = 0;
		KAVLComparator **comparators = 0;

	KeyComparator(JNIEnv *env, long indexId, int fieldCount, int *dataTypes);

	int compare(void *o1, void *o2);
};

class KeyImpl : public PoolableObject {
public:

	KeyImpl() {
	}

	virtual bool equals(void *o) {
		return compareTo((KeyImpl*)o) == 0;
	}

	virtual int compareTo(KeyImpl* o) = 0;

	virtual int hashCode(int *dataTypes) = 0;

	virtual int hashCode() {
		return 0;
	}

	virtual bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) = 0;

	virtual PoolableObject *allocate(int count) = 0;

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) = 0;

	virtual void free(int *dataTypes) = 0;

	virtual jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) = 0;

};


class Key : public Comparable<Key*>, public PoolableObject {
public:
	KeyImpl *key;

	Key() {
	}

	PoolableObject *allocate(int count) {
		return new Key[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((Key*)array)[offset];
	}


	virtual char *className() {
		return "Key";
	};

	virtual bool equals(void *o) {
		//printf("equas - base: key=%ld, o=%ld, o->key=%ld\n", key, o, ((Key*)o)->key);
		//fflush(stdout);
		return key->equals(((Key*)o)->key);
	}

	virtual int compareTo(Key* o) {
		//printf("compare - base: key=%ld, keyclass=%s\n", key, ((PoolableObject*)key)->className());
		//fflush(stdout);
		return key->compareTo(o->key);
	}

	virtual int hashCode(int *dataTypes) {
		return key->hashCode(dataTypes);
	}

	virtual int hashCode() {
		return 0;
	}

	virtual bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		return key->serializeKey(dataTypes, bytes, offset, len);
	}

	virtual void free(int *dataTypes) {
		return;
		//return key->free(dataTypes);
	}

	virtual jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		return key->toJavaKey(env, dataTypes);
	}

};

class CompoundKey : public KeyImpl {
public:
	uint8_t len;
	void **key;
	KeyComparator *comparator;

	CompoundKey() {
	}

	CompoundKey(void **key, uint8_t len, KeyComparator *comparator) {
		this->key = key;
		this->len = len;
		this->comparator = comparator;
	}

	virtual PoolableObject *allocate(int count) {
		return new CompoundKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((CompoundKey*)array)[offset];
	}

	virtual char *className() {
		return "CompoundKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		for (int i = 0; i < this->len; i++) {
    		if (offset[0] > len) {
    			return false;
    		}
    		if (key[i] == 0) {
    			bytes[offset[0]++] = 0;
    			continue;
    		}
    		else {
    			bytes[offset[0]++] = 1;
    		}
    		int type = dataTypes[i];
    		switch (type) {
    		case ROWID:
    		case BIGINT:
    		{
    			if (!writeLong(*((jlong*)key[i]), bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case SMALLINT:
    		{
    			if (!writeShort(*((jshort*)key[i]), bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case INTEGER:
    		{
    			if (!writeInt(*((jint*)key[i]), bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case TINYINT:
    		{
    			if (offset[0] > len) {
    				return false;
    			}
    			bytes[offset[0]++] = *(jbyte*)key[i];
    		}
    		break;
    		case DATE:
    		{
    			if (!writeLong(*(jlong*)key[i], bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case TIME:
    		{
    			if (!writeLong(*(jlong*)key[i], bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case TIMESTAMP:
    		{
    			if (!writeLong(((jlong*)key[i])[0], bytes, offset, len)) {
    				return false;
    			}
    			if (!writeInt((int)((jlong*)key[i])[1], bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case DOUBLE:
    		case FLOAT:
    		{
    			uint64_t bits;
    			memcpy(&bits, (jdouble*)key[i], sizeof bits);
    			if (!writeLong(bits, bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case REAL:
    		{
    			uint32_t bits;
    			memcpy(&bits, (jfloat*)key[i], sizeof bits);
    			if (!writeInt(bits, bytes, offset, len)) {
    				return false;
    			}
    		}
    		break;
    		case NUMERIC:
    		case DECIMAL:
    		{
    			std::string *s = (std::string*)key[i];
    			const char *str = ((std::string*)key[i])->c_str();
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
    			const uint16_t *str = ((sb_utf8str*)key[i])->bytes;
    			int slen = ((sb_utf8str*)key[i])->len;
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
    	return true;
	}

	virtual int compareTo(KeyImpl* o) {
		//printf("compoundKey - compare begin\n");
		//fflush(stdout);
		int keyLen = len <= ((CompoundKey*)o)->len ? len : ((CompoundKey*)o)->len;
    	for (int i = 0; i < keyLen; i++) {
    	  int value = comparator->comparators[i]->compare(key[i], ((CompoundKey*)o)->key[i]);
    	  if (value != 0) {
		//printf("compoundKey - compare end\n");
		//fflush(stdout);
    		return value;
    	  }
    	}
		//printf("compoundKey - compare end\n");
		//fflush(stdout);
    	return 0;
	}

	void free(int *dataTypes)  {
		if (key != 0) {
    		for (int i = 0; i < len; i++) {
    			if (key[i] == 0) {
    				continue;
    			}
    			int type = dataTypes[i];

    			switch (type) {
    			case ROWID:
    			case BIGINT:
    			case DATE:
    			case TIME:
    				delete (jlong*)key[i];
    				break;
    			case SMALLINT:
    				delete (jshort*)key[i];
    				break;
    			case INTEGER:
    				delete (jint*)key[i];
    				break;
    			case TINYINT:
    				delete (jbyte*)key[i];
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
    				jlong *entry = (jlong*)key[i];
    				delete[] entry;
    			}
    			break;
    			case DOUBLE:
    			case FLOAT:
    			{
    				delete (jdouble*)key[i];
    			}
    			break;
    			case REAL:
    			{
    				delete (jfloat*)key[i];
    			}
    			break;
    			default:
    			{
					printf("Unsupported datatype in deleteKey: type=%d", type);
    			}

    			}

    		}
    		delete[] key;
    	}
	}

	int hashCode(int *dataTypes) {
	uint32_t hashCode = 1;
    	for (int i = 0; i < len; i++) {
    		int type = dataTypes[i];
    		uint32_t currHashCode = 0;
    		if (key[i] == 0) {
    			continue;
    		}
    		switch (type) {
    			case ROWID:
    			case BIGINT:
    			case DATE:
    			case TIME:
    			{
    				currHashCode = (int)(*((jlong*)key[i]) ^ (*((jlong*)key[i]) >> 32));
    			}
    			break;
    			case INTEGER:
    			{
    				currHashCode = *(jint*)key[i];
    			}
    			break;
    			case SMALLINT:
    			{
    				currHashCode = *(jshort*)key[i];
    			}
    			break;
    			case TINYINT:
    			{
    				currHashCode = *(jbyte*)key[i];
    			}
    			break;
    			case BOOLEAN:
    			case BIT:
    			{
    				currHashCode = *(jboolean*)key[i] ? 1231 : 1237;
    			}
    			break;
    			case TIMESTAMP:
    			{
    				//printf("hash: entering");
    				//fflush(stdout);
    				//long l = ( ((long*)(key[i]))[0]);
    				//printf("hash: key=%ld", (long)l);
    				//fflush(stdout);
    				currHashCode = (int)( ((jlong*)key[i])[0] ^ (((jlong*)key[i])[0] >> 32));
    				//currHashCode = 31 * ((long*)key[i])[1] ^ (((long*)key[i])[1] >> 32) + currHashCode;
    			}
    			break;
    			case DOUBLE:
    			case FLOAT:
    			{
    				jlong bits = doubleToLongBits(*(jdouble *)key[i]);
    				currHashCode = (jint)(bits ^ (bits >> 32));
    			}
    			break;
    			case REAL:
    			{
    				jint bits = float_to_bits(*(jfloat *)key[i]);
    				currHashCode = bits;
    			}
    			break;
    			case NUMERIC:
    			case DECIMAL:
    			{
    				std::vector<uint8_t> myVector(((std::string*)key[i])->begin(), ((std::string*)key[i])->end());
    				uint8_t *p = &myVector[0];
    				currHashCode = 1;
    				size_t len = ((std::string*)key[i])->length();
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

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(len, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		for (int i = 0; i < len; i++) {
    		if (key[i] == 0) {
    			continue;
    		}
    		int type = dataTypes[i];
    		switch (type) {
    			case ROWID:
    			case BIGINT:
    			{
    				jobject obj = env->CallStaticObjectMethod(gLong_class, gLong_valueOf_mid, *((jlong*)key[i]));
    			    env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case SMALLINT:
    			{
    				jobject obj = env->CallStaticObjectMethod(gShort_class, gShort_valueOf_mid, *((jshort*)key[i]));
    			    env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case INTEGER:
    			{
    				jobject obj = env->CallStaticObjectMethod(gInt_class, gInt_valueOf_mid, *(jint*)key[i]);
    			    env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case TINYINT:
    			{
    				jobject obj = env->CallStaticObjectMethod(gInt_class, gByte_valueOf_mid, *(jbyte*)key[i]);
    			    env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case DATE:
    			{
    				jobject obj = env->NewObject(gDate_class, gDate_ctor, *(jlong*)key[i]);
    				env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case TIME:
    			{
    				jobject obj = env->NewObject(gTime_class, gTime_ctor, *(jlong*)key[i]);
    				env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case TIMESTAMP:
    			{
    				jobject obj = env->NewObject(gTimestamp_class, gTimestamp_ctor, ((jlong*)key[i])[0]);
                    env->CallVoidMethod(obj, gTimestamp_setNanos_mid, (int)((jlong*)key[i])[1]);
    				env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case DOUBLE:
    			case FLOAT:
    			{
    				jobject obj = env->CallStaticObjectMethod(gDouble_class, gDouble_valueOf_mid, *(jdouble*)key[i]);
    			    env->SetObjectArrayElement(ret, i, obj);
    			}
    			break;
    			case REAL:
    			{
    				jobject obj = env->CallStaticObjectMethod(gFloat_class, gFloat_valueOf_mid, *(jfloat*)key[i]);
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
    					jcharArray arr = env->NewCharArray(((sb_utf8str*)key[i])->len);
    					env->SetCharArrayRegion(arr, 0, ((sb_utf8str*)key[i])->len, (jchar*)((sb_utf8str*)key[i])->bytes);
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
};


class LongComparator : public KAVLComparator {

	public:
		static int doCompare(void* o1, void *o2) {
			//printf("compare long\n");
			//fflush(stdout);
			if (o1 == 0 || o2 == 0) {
				return 0;
			}
			if (*((jlong*) o1) < *((jlong*) o2)) {
			  return -1;
			}
			else {
			  return *((jlong*) o1) > *((jlong*) o2) ? 1 : 0;
			}
		}

		int compare(void *o1, void *o2) {
			return LongComparator::doCompare(o1, o2);
		}
};

class Utf8Comparator : public KAVLComparator {

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

	static int doCompare(void *o1, void *o2) {
	  	//printf("compare string\n");//: str1=%s, str2=%s\n", b1, b2);
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

	int compare(void *o1, void *o2) {
		return Utf8Comparator::doCompare(o1, o2);
	}

};


class ShortComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return ShortComparator::doCompare(o1, o2);
		}
};

class ByteComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return ByteComparator::doCompare(o1, o2);
		}
};

class BooleanComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return BooleanComparator::doCompare(o1, o2);
		}
};


class IntComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return IntComparator::doCompare(o1, o2);
		}
};

class DoubleComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return DoubleComparator::doCompare(o1, o2);
		}
};

class FloatComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return FloatComparator::doCompare(o1, o2);
		}
};

class TimestampComparator : public KAVLComparator {
	public:
		static int doCompare(void *o1, void *o2) {
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

		int compare(void *o1, void *o2) {
			return TimestampComparator::doCompare(o1, o2);
		}
};

class BigDecimalComparator : public KAVLComparator {

	public:
		static int doCompare(void *o1, void *o2) {
			if (o1 == 0 || o2 == 0) {
				return 0;
			}
			int ret = BigDecimal::compareTo(*(std::string*)o1, *(std::string*)o2);
			//printf("compare: lhs=%s, rhs=%s, ret=%d]\n", ((std::string*)o1)->c_str(), ((std::string*)o2)->c_str(), ret);
			//fflush(stdout);
			return ret;
		}

		int compare(void *o1, void *o2) {
			return BigDecimalComparator::doCompare(o1, o2);
		}
};



class DateKey : public KeyImpl {
public:
	jlong longKey;

	DateKey() {
	}

	DateKey(jlong key) {
		this->longKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new DateKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((DateKey*)array)[offset];
	}

	virtual char *className() {
		return "DateKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeLong(longKey, bytes, offset, len)) {
			return false;
		}
		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return LongComparator::doCompare(&longKey, &((DateKey*)o)->longKey);
	}

	int hashCode(int *dataTypes) {
    	return (int)(longKey ^ (longKey >> 32));
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		jobject obj = env->NewObject(gDate_class, gDate_ctor, longKey);
		env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class TimeKey : public KeyImpl {
public:
	jlong longKey;

	TimeKey() {
	}

	TimeKey(jlong key) {
		this->longKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new TimeKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((TimeKey*)array)[offset];
	}

	virtual char *className() {
		return "TimeKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeLong(longKey, bytes, offset, len)) {
			return false;
		}
		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return LongComparator::doCompare(&longKey, &((TimeKey*)o)->longKey);
	}

	int hashCode(int *dataTypes) {
    	return (int)(longKey ^ (longKey >> 32));
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		jobject obj = env->NewObject(gTime_class, gTime_ctor, longKey);
		env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class LongKey : public KeyImpl {
public:
	jlong longKey;

	LongKey() {
	}

	LongKey(jlong key) {
		this->longKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new LongKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((LongKey*)array)[offset];
	}

	virtual char *className() {
		return "LongKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeLong(longKey, bytes, offset, len)) {
			return false;
		}
		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return LongComparator::doCompare(&longKey, &((LongKey*)o)->longKey);
	}

	int hashCode(int *dataTypes) {
    	return (int)(longKey ^ (longKey >> 32));
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gLong_class, gLong_valueOf_mid, longKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class IntKey : public KeyImpl {
public:
	jint intKey;

	IntKey() {
	}

	IntKey(jint key) {
		this->intKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new IntKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((IntKey*)array)[offset];
	}

	virtual char *className() {
		return "IntKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeInt(intKey, bytes, offset, len)) {
			return false;
		}
		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return IntComparator::doCompare(&intKey, &((IntKey*)o)->intKey);
	}

	int hashCode(int *dataTypes) {
    	return intKey;
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gInt_class, gInt_valueOf_mid, intKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class ShortKey : public KeyImpl {
public:
	jshort shortKey;

	ShortKey() {
	}

	ShortKey(jshort key) {
		this->shortKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new ShortKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((ShortKey*)array)[offset];
	}

	virtual char *className() {
		return "ShortKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeShort(shortKey, bytes, offset, len)) {
			return false;
		}
		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return ShortComparator::doCompare(&shortKey, &((ShortKey*)o)->shortKey);
	}

	int hashCode(int *dataTypes) {
    	return shortKey;
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gShort_class, gShort_valueOf_mid, shortKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class ByteKey : public KeyImpl {
public:
	jbyte byteKey;

	ByteKey() {
	}

	ByteKey(jbyte key) {
		this->byteKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new ByteKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((ByteKey*)array)[offset];
	}

	virtual char *className() {
		return "ByteKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		bytes[offset[0]++] = byteKey;

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return ByteComparator::doCompare(&byteKey, &((ByteKey*)o)->byteKey);
	}

	int hashCode(int *dataTypes) {
    	return byteKey;
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gByte_class, gByte_valueOf_mid, byteKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class BigDecimalKey : public KeyImpl {
public:
	std::string *bigDecimalKey;

	BigDecimalKey() {
	}

	BigDecimalKey(std::string *key) {
		this->bigDecimalKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new BigDecimalKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((BigDecimalKey*)array)[offset];
	}

	virtual char *className() {
		return "BigDecimalKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		std::string *s = bigDecimalKey;
		const char *str = bigDecimalKey->c_str();
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

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return BigDecimalComparator::doCompare(bigDecimalKey, ((BigDecimalKey*)o)->bigDecimalKey);
	}

	int hashCode(int *dataTypes) {
		//todo: optimize
		std::vector<uint8_t> myVector(bigDecimalKey->begin(), bigDecimalKey->end());
		uint8_t *p = &myVector[0];
		int currHashCode = 1;
		size_t len = bigDecimalKey->length();
		for (int j = 0; j < len; j++) {
			currHashCode = 31 * currHashCode + p[j];
		}
		return currHashCode;
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		jstring jstr = env->NewStringUTF(bigDecimalKey->c_str());
		jobject obj = env->NewObject(gBigDecimal_class, gBigDecimal_ctor, jstr);
		env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {
		delete bigDecimalKey;
	}

};

class StringKey : public KeyImpl {
public:
	sb_utf8str stringKey;

	StringKey() {
	}

//	StringKey(sb_utf8str *key) {
//		this->stringKey = key;
//	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new StringKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((StringKey*)array)[offset];
	}

	virtual char *className() {
		return "StringKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		const uint16_t *str = stringKey.bytes;
		int slen = stringKey.len;
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

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return Utf8Comparator::doCompare(&stringKey, &((StringKey*)o)->stringKey);
	}

	int hashCode(int *dataTypes) {
		sb_utf8str *str = &stringKey;
		uint16_t *bytes = str->bytes;
		int currHashCode = 1;
		int len = str->len;
		for (int j = 0; j < len; j++) {
			currHashCode = 31 * currHashCode + bytes[j];
		}
		return currHashCode;
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		if (stringKey.len != 0) {
			jcharArray arr = env->NewCharArray(stringKey.len);
			env->SetCharArrayRegion(arr, 0, stringKey.len, (jchar*)stringKey.bytes);
			env->SetObjectArrayElement(ret, 0, (jobject)arr);
			//printf("key=" + std::wstring(((sb_utf8str*)key[i])->bytes));
		}

    	return ret;
	}

	virtual void free(int *dataTypes) {
	}

};

class TimestampKey : public KeyImpl {
public:
	jlong* timestampKey;

	TimestampKey() {
	}

	TimestampKey(jlong* key) {
		this->timestampKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new TimestampKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((TimestampKey*)array)[offset];
	}

	virtual char *className() {
		return "TimestampKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		if (!writeLong(timestampKey[0], bytes, offset, len)) {
			return false;
		}
		if (!writeInt((jint)timestampKey[1], bytes, offset, len)) {
			return false;
		}

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return TimestampComparator::doCompare(timestampKey, ((TimestampKey*)o)->timestampKey);
	}

	int hashCode(int *dataTypes) {
		return (int)(timestampKey[0] ^ (timestampKey[1] >> 32));
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

		jobject obj = env->NewObject(gTimestamp_class, gTimestamp_ctor, timestampKey[0]);
		env->CallVoidMethod(obj, gTimestamp_setNanos_mid, (int)timestampKey[1]);
		env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {
		delete[] timestampKey;
	}

};



class DoubleKey : public KeyImpl {
public:
	jdouble doubleKey;

	DoubleKey() {
	}

	DoubleKey(jdouble key) {
		this->doubleKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new DoubleKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((DoubleKey*)array)[offset];
	}

	virtual char *className() {
		return "DoubleKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		uint64_t bits;
		memcpy(&bits, &doubleKey, sizeof bits);
		if (!writeLong(bits, bytes, offset, len)) {
			return false;
		}

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return DoubleComparator::doCompare(&doubleKey, &((DoubleKey*)o)->doubleKey);
	}

	int hashCode(int *dataTypes) {
		jlong bits = doubleToLongBits(doubleKey);
		return (jint)(bits ^ (bits >> 32));
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gDouble_class, gDouble_valueOf_mid, doubleKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};

class FloatKey : public KeyImpl {
public:
	jfloat floatKey;

	FloatKey() {
	}

	FloatKey(jfloat key) {
		this->floatKey = key;
	}

	virtual PoolableObject *allocate(int count) {
		//printf("allocate - class=%s, count=%d\n", className(), count);
		//fflush(stdout);
		return new FloatKey[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((FloatKey*)array)[offset];
	}

	virtual char *className() {
		return "FloatKey";
	};

	bool serializeKey(int* dataTypes, jbyte *bytes, int *offset, int len) {
		if (offset[0] >= len) {
			return false;
		}
		bytes[offset[0]++] = 1;

		uint32_t bits;
		memcpy(&bits, &floatKey, sizeof bits);
		if (!writeInt(bits, bytes, offset, len)) {
			return false;
		}

		return true;
	}

	virtual int compareTo(KeyImpl* o) {
		return FloatComparator::doCompare(&floatKey, &((FloatKey*)o)->floatKey);
	}

	int hashCode(int *dataTypes) {
		return float_to_bits(floatKey);
	}

	jobjectArray toJavaKey(JNIEnv *env, int *dataTypes) {
		jobjectArray ret = env->NewObjectArray(1, gObject_class, 0);
        if (ret == 0) {
        	return 0;
        }

    	jobject obj = env->CallStaticObjectMethod(gFloat_class, gFloat_valueOf_mid, floatKey);
    	env->SetObjectArrayElement(ret, 0, obj);

    	return ret;
	}

	virtual void free(int *dataTypes) {

	}

};


class MyValue : public Comparable<MyValue*>, public PoolableObject {
public:
	long value;
	MyValue();

	MyValue(long v);

	virtual bool equals(void *o);

	int compareTo(MyValue o);

	int compareTo(MyValue* o);

	virtual int hashCode();

	PoolableObject *allocate(int count) {
		return new MyValue[count];
	}

	virtual PoolableObject *getObjectAtOffset(PoolableObject *array, int offset) {
		return &((MyValue*)array)[offset];
	}

	virtual char *className() {
		return "MyValue";
	};

};


extern Key *javaKeyToNativeKey(JNIEnv *env, PooledObjectPool *keyPool, PooledObjectPool *keyImplPool, int *dataTypes, jobjectArray jKey, KeyComparator *comparator);

extern uint32_t hashKey(JNIEnv* env, int* dataTypes, Key *key);

extern void deleteKey(JNIEnv *env, int *dataTypes, Key *key);

extern jboolean serializeKeyValue(JNIEnv *env, int* dataTypes, jbyte *bytes, int len, int *offset, Key *key, uint64_t value);

extern jobjectArray nativeKeyToJavaKey(JNIEnv *env, int *dataTypes, Key *key);

extern PooledObjectPool *createKeyPool(JNIEnv * env, int *dataTypes, int fieldCount);


jint throwException(JNIEnv *env, const char *message);

#endif