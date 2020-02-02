/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_sonicbase_index_NativeSkipListMap */

#ifndef _Included_com_sonicbase_index_NativeSkipListMap
#define _Included_com_sonicbase_index_NativeSkipListMap
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    initIndex
 * Signature: ([I)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_initIndex
  (JNIEnv *, jobject, jintArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    put
 * Signature: (J[Ljava/lang/Object;J)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3Ljava_lang_Object_2J
  (JNIEnv *, jobject, jlong, jobjectArray, jlong);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    put
 * Signature: (J[[Ljava/lang/Object;[J[J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_put__J_3_3Ljava_lang_Object_2_3J_3J
  (JNIEnv *, jobject, jlong, jobjectArray, jlongArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    get
 * Signature: (J[Ljava/lang/Object;)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_get
  (JNIEnv *, jobject, jlong, jobjectArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    remove
 * Signature: (J[Ljava/lang/Object;)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativeSkipListMap_remove
  (JNIEnv *, jobject, jlong, jobjectArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_clear
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    getResultsBytes
 * Signature: (J[Ljava/lang/Object;IZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativeSkipListMap_getResultsBytes
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    getResultsObjects
 * Signature: (J[Ljava/lang/Object;IZ[[Ljava/lang/Object;[J)I
 */
JNIEXPORT jint JNICALL Java_com_sonicbase_index_NativeSkipListMap_getResultsObjects
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    tailBlockArray
 * Signature: (J[Ljava/lang/Object;IZ[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_tailBlockArray
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean, jbyteArray, jint);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    headBlockArray
 * Signature: (J[Ljava/lang/Object;IZ[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_headBlockArray
  (JNIEnv *, jobject, jlong, jobjectArray, jint, jboolean, jbyteArray, jint);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    higherEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_higherEntry
  (JNIEnv *, jobject, jlong, jobjectArray, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    lowerEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_lowerEntry
  (JNIEnv *, jobject, jlong, jobjectArray, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    floorEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_floorEntry
  (JNIEnv *, jobject, jlong, jobjectArray, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    ceilingEntry
 * Signature: (J[Ljava/lang/Object;[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_ceilingEntry
  (JNIEnv *, jobject, jlong, jobjectArray, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    lastEntry2
 * Signature: (J[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_lastEntry2
  (JNIEnv *, jobject, jlong, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    firstEntry2
 * Signature: (J[[Ljava/lang/Object;[J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_sonicbase_index_NativeSkipListMap_firstEntry2
  (JNIEnv *, jobject, jlong, jobjectArray, jlongArray);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    delete
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_delete
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_sonicbase_index_NativeSkipListMap
 * Method:    sortKeys
 * Signature: (J[[Ljava/lang/Object;Z)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativeSkipListMap_sortKeys
  (JNIEnv *, jobject, jlong, jobjectArray, jboolean);

#ifdef __cplusplus
}
#endif
#endif
