/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_sonicbase_index_NativePartitionedTree */

#ifndef _Included_com_sonicbase_index_NativePartitionedTree
#define _Included_com_sonicbase_index_NativePartitionedTree
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    initIndex
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_initIndex
  (JNIEnv *, jobject);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    put
 * Signature: (J[BJ)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_put
  (JNIEnv *, jobject, jlong, jbyteArray, jlong);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    putBytes
 * Signature: (J[B[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_putBytes
  (JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    get
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_get
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    remove
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_com_sonicbase_index_NativePartitionedTree_remove
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_sonicbase_index_NativePartitionedTree_clear
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    tailBlock
 * Signature: (J[BIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlock
  (JNIEnv *, jobject, jlong, jbyteArray, jint, jboolean);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    tailBlockBytes
 * Signature: (J[BIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_tailBlockBytes
  (JNIEnv *, jobject, jlong, jbyteArray, jint, jboolean);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    headBlock
 * Signature: (J[BIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_headBlock
  (JNIEnv *, jobject, jlong, jbyteArray, jint, jboolean);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    higherEntry
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_higherEntry
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    lowerEntry
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_lowerEntry
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    floorEntry
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_floorEntry
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    ceilingEntry
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_ceilingEntry
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    lastEntry2
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_lastEntry2
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_sonicbase_index_NativePartitionedTree
 * Method:    firstEntry2
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_sonicbase_index_NativePartitionedTree_firstEntry2
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
