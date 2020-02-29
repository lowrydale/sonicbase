#pragma once

#include "AbstractMap.h"
#include "ConcurrentNavigableMap.h"
#include "AtomicJava.h"
#include "Comparator.h"
#include "Comparable.h"
#include "Map.h"
#include "SortedMap.h"
#include "ArrayList.h"
#include "Iterator.h"
#include "Set.h"
#include "NavigableSet.h"
#include "Collection.h"
#include "NavigableMap.h"
#include "AbstractSet.h"
#include "AbstractCollection.h"
#include "Serializable.h"
#include <limits>
#include <stdexcept>
#include "exceptionhelper.h"
#include <cassert>
#include <random>
#include <iostream>
#include "ObjectPool.h"
#include "sonicbase.h"

#ifndef INT_MAX
#define INT_MAX  ((1 << (sizeof(int)*8 - 2)) - 1 + (1 << (sizeof(int)*8 - 2)))
#endif

#define OBJECT_POOL_SIZE 2000000

//JAVA TO C++ CONVERTER NOTE: Forward class declarations:

namespace skiplist { template<typename K, typename V>class HeadIndex; }
namespace skiplist { template<typename E>class KeySet; }
namespace skiplist { template<typename K1, typename V1>class EntrySet; }
namespace skiplist { template<typename E>class Values; }

namespace skiplist
{

	/*
	 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
	 *
	 * This code is free software; you can redistribute it and/or modify it
	 * under the terms of the GNU General Public License version 2 only, as
	 * published by the Free Software Foundation.  Oracle designates this
	 * particular file as subject to the "Classpath" exception as provided
	 * by Oracle in the LICENSE file that accompanied this code.
	 *
	 * This code is distributed in the hope that it will be useful, but WITHOUT
	 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
	 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
	 * version 2 for more details (a copy is included in the LICENSE file that
	 * accompanied this code).
	 *
	 * You should have received a copy of the GNU General Public License version
	 * 2 along with this work; if not, write to the Free Software Foundation,
	 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
	 *
	 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
	 * or visit www.oracle.com if you need additional information or have any
	 * questions.
	 */

	/*
	 * This file is available under and governed by the GNU General Public
	 * License version 2 only, as published by the Free Software Foundation.
	 * However, the following notice accompanied the original version of this
	 * file:
	 *
	 * Written by Doug Lea with assistance from members of JCP JSR-166
	 * Expert Group and released to the public domain, as explained at
	 * http://creativecommons.org/licenses/publicdomain
	 */

	/**
	 * A scalable concurrent {@link ConcurrentNavigableMap} implementation.
	 * The map is sorted according to the {@linkplain Comparable natural
	 * ordering} of its keys, or by a {@link Comparator} provided at map
	 * creation time, depending on which constructor is used.
	 *
	 * <p>This class implements a concurrent variant of <a
	 * href="http://www.cs.umd.edu/~pugh/">SkipLists</a> providing
	 * expected average <i>log(n)</i> time cost for the
	 * <tt>containsKey</tt>, <tt>get</tt>, <tt>put</tt> and
	 * <tt>remove</tt> operations and their variants.  Insertion, removal,
	 * update, and access operations safely execute concurrently by
	 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
	 * elements reflecting the state of the map at some point at or since
	 * the creation of the beginIterator.  They do <em>not</em> throw {@link
	 * ConcurrentModificationException}, and may proceed concurrently with
	 * other operations. Ascending key ordered views and their iterators
	 * are faster than descending ones.
	 *
	 * <p>All <tt>skiplist.Map.Entry</tt> pairs returned by methods in this class
	 * and its views represent snapshots of mappings at the time they were
	 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
	 * method. (Note however that it is possible to change mappings in the
	 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
	 * <tt>replace</tt>, depending on exactly which effect you need.)
	 *
	 * <p>Beware that, unlike in most collections, the <tt>size</tt>
	 * method is <em>not</em> a constant-time operation. Because of the
	 * asynchronous nature of these maps, determining the current number
	 * of elements requires a traversal of the elements.  Additionally,
	 * the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
	 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
	 * atomically. For example, an beginIterator operating concurrently with a
	 * <tt>putAll</tt> operation might view only some of the added
	 * elements.
	 *
	 * <p>This class and its views and iterators implement all of the
	 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
	 * interfaces. Like most other concurrent collections, this class does
	 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
	 * null return values cannot be reliably distinguished from the absence of
	 * elements.
	 *
	 * <p>This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author Doug Lea
	 * @param <K> the type of keys maintained by this map
	 * @param <V> the type of mapped values
	 * @since 1.6
	 */
	template<typename K, typename V>
	class ConcurrentSkipListMap : public AbstractMap<K, V>, public ConcurrentNavigableMap<K, V>
	{
		/*
		 * This class implements a tree-like two-dimensionally linked skip
		 * list in which the index levels are represented in separate
		 * nodes from the base nodes holding data.  There are two reasons
		 * for taking this approach instead of the usual array-based
		 * structure: 1) Array based implementations seem to encounter
		 * more complexity and overhead 2) We can use cheaper algorithms
		 * for the heavily-traversed index lists than can be used for the
		 * base lists.  Here's a picture of some of the basics for a
		 * possible list with 2 levels of index:
		 *
		 * Head nodes          Index nodes
		 * +-+    right        +-+                      +-+
		 * |2|---------------->| |--------------------->| |->null
		 * +-+                 +-+                      +-+
		 *  | down              |                        |
		 *  v                   v                        v
		 * +-+            +-+  +-+       +-+            +-+       +-+
		 * |1|----------->| |->| |------>| |----------->| |------>| |->null
		 * +-+            +-+  +-+       +-+            +-+       +-+
		 *  v              |    |         |              |         |
		 * Nodes  next     v    v         v              v         v
		 * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
		 * | |->|A|->|B|->|C|->|D|->|E|->|F|->|G|->|H|->|I|->|J|->|K|->null
		 * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
		 *
		 * The base lists use a variant of the HM linked ordered set
		 * algorithm. See Tim Harris, "A pragmatic implementation of
		 * non-blocking linked lists"
		 * http://www.cl.cam.ac.uk/~tlh20/publications.html and Maged
		 * Michael "High Performance Dynamic Lock-Free Hash Tables and
		 * List-Based Sets"
		 * http://www.research.ibm.com/people/m/michael/pubs.htm.  The
		 * basic idea in these lists is to mark the "next" pointers of
		 * deleted nodes when deleting to avoid conflicts with concurrent
		 * insertions, and when traversing to keep track of triples
		 * (predecessor, node, successor) in order to detect when and how
		 * to unlink these deleted nodes.
		 *
		 * Rather than using mark-bits to mark list deletions (which can
		 * be slow and space-intensive using AtomicMarkedReference), nodes
		 * use direct CAS'able next pointers.  On deletion, instead of
		 * marking a pointer, they splice in another node that can be
		 * thought of as standing for a marked pointer (indicating this by
		 * using otherwise impossible field values).  Using plain nodes
		 * acts roughly like "boxed" implementations of marked pointers,
		 * but uses new nodes only when nodes are deleted, not for every
		 * link.  This requires less space and supports faster
		 * traversal. Even if marked references were better supported by
		 * JVMs, traversal using this technique might still be faster
		 * because any search need only read ahead one more node than
		 * otherwise required (to check for trailing marker) rather than
		 * unmasking mark bits or whatever on each read.
		 *
		 * This approach maintains the essential property needed in the HM
		 * algorithm of changing the next-pointer of a deleted node so
		 * that any other CAS of it will fail, but implements the idea by
		 * changing the pointer to point to a different node, not by
		 * marking it.  While it would be possible to further squeeze
		 * space by defining marker nodes not to have key/value fields, it
		 * isn't worth the extra type-testing overhead.  The deletion
		 * markers are rarely encountered during traversal and are
		 * normally quickly garbage collected. (Note that this technique
		 * would not work well in systems without garbage collection.)
		 *
		 * In addition to using deletion markers, the lists also use
		 * nullness of value fields to indicate deletion, in a style
		 * similar to typical lazy-deletion schemes.  If a node's value is
		 * null, then it is considered logically deleted and ignored even
		 * though it is still reachable. This maintains proper control of
		 * concurrent replace vs delete operations -- an attempted replace
		 * must fail if a delete beat it by nulling field, and a delete
		 * must return the last non-null value held in the field. (Note:
		 * Null, rather than some special marker, is used for value fields
		 * here because it just so happens to mesh with the skiplist.Map API
		 * requirement that method get returns null if there is no
		 * mapping, which allows nodes to remain concurrently readable
		 * even when deleted. Using any other marker value here would be
		 * messy at best.)
		 *
		 * Here's the sequence of events for a deletion of node n with
		 * predecessor b and successor f, initially:
		 *
		 *        +------+       +------+      +------+
		 *   ...  |   b  |------>|   n  |----->|   f  | ...
		 *        +------+       +------+      +------+
		 *
		 * 1. CAS n's value field from non-null to null.
		 *    From this point on, no public operations encountering
		 *    the node consider this mapping to exist. However, other
		 *    ongoing insertions and deletions might still modify
		 *    n's next pointer.
		 *
		 * 2. CAS n's next pointer to point to a new marker node.
		 *    From this point on, no other nodes can be appended to n.
		 *    which avoids deletion errors in CAS-based linked lists.
		 *
		 *        +------+       +------+      +------+       +------+
		 *   ...  |   b  |------>|   n  |----->|marker|------>|   f  | ...
		 *        +------+       +------+      +------+       +------+
		 *
		 * 3. CAS b's next pointer over both n and its marker.
		 *    From this point on, no new traversals will encounter n,
		 *    and it can eventually be GCed.
		 *        +------+                                    +------+
		 *   ...  |   b  |----------------------------------->|   f  | ...
		 *        +------+                                    +------+
		 *
		 * A failure at step 1 leads to simple retry due to a lost race
		 * with another operation. Steps 2-3 can fail because some other
		 * thread noticed during a traversal a node with null value and
		 * helped out by marking and/or unlinking.  This helping-out
		 * ensures that no thread can become stuck waiting for progress of
		 * the deleting thread.  The use of marker nodes slightly
		 * complicates helping-out code because traversals must track
		 * consistent reads of up to four nodes (b, n, marker, f), not
		 * just (b, n, f), although the next field of a marker is
		 * immutable, and once a next field is CAS'ed to point to a
		 * marker, it never again changes, so this requires less care.
		 *
		 * Skip lists add indexing to this scheme, so that the base-level
		 * traversals start close to the locations being found, inserted
		 * or deleted -- usually base level traversals only traverse a few
		 * nodes. This doesn't change the basic algorithm except for the
		 * need to make sure base traversals start at predecessors (here,
		 * b) that are not (structurally) deleted, otherwise retrying
		 * after processing the deletion.
		 *
		 * Index levels are maintained as lists with volatile next fields,
		 * using CAS to link and unlink.  Races are allowed in index-list
		 * operations that can (rarely) fail to link in a new index node
		 * or delete one. (We can't do this of course for data nodes.)
		 * However, even when this happens, the index lists remain sorted,
		 * so correctly serve as indices.  This can impact performance,
		 * but since skip lists are probabilistic anyway, the net result
		 * is that under contention, the effective "p" value may be lower
		 * than its nominal value. And race windows are kept small enough
		 * that in practice these failures are rare, even under a lot of
		 * contention.
		 *
		 * The fact that retries (for both base and index lists) are
		 * relatively cheap due to indexing allows some minor
		 * simplifications of retry logic. Traversal restarts are
		 * performed after most "helping-out" CASes. This isn't always
		 * strictly necessary, but the implicit backoffs tend to help
		 * reduce other downstream failed CAS's enough to outweigh restart
		 * cost.  This worsens the worst case, but seems to improve even
		 * highly contended cases.
		 *
		 * Unlike most skip-list implementations, index insertion and
		 * deletion here require a separate traversal pass occuring after
		 * the base-level action, to add or remove index nodes.  This adds
		 * to single-threaded overhead, but improves contended
		 * multithreaded performance by narrowing interference windows,
		 * and allows deletion to ensure that all index nodes will be made
		 * unreachable upon return from a public remove operation, thus
		 * avoiding unwanted garbage retention. This is more important
		 * here than in some other data structures because we cannot null
		 * out node fields referencing user keys since they might still be
		 * read by other ongoing traversals.
		 *
		 * Indexing uses skip list parameters that maintain good search
		 * performance while using sparser-than-usual indices: The
		 * hardwired parameters k=1, p=0.5 (see method randomLevel) mean
		 * that about one-quarter of the nodes have indices. Of those that
		 * do, half have one level, a quarter have two, and so on (see
		 * Pugh's Skip List Cookbook, sec 3.4).  The expected total space
		 * requirement for a map is slightly less than for the current
		 * implementation of java.util.TreeMap.
		 *
		 * Changing the level of the index (i.e, the height of the
		 * tree-like structure) also uses CAS. The head index has initial
		 * level/height of one. Creation of an index with height greater
		 * than the current level adds a level to the head index by
		 * CAS'ing on a new top-most head. To maintain good performance
		 * after a lot of removals, deletion methods heuristically try to
		 * reduce the height if the topmost levels appear to be empty.
		 * This may encounter races in which it possible (but rare) to
		 * reduce and "lose" a level just as it is about to contain an
		 * index (that will then never be encountered). This does no
		 * structural harm, and in practice appears to be a better option
		 * than allowing unrestrained growth of levels.
		 *
		 * The code for all this is more verbose than you'd like. Most
		 * operations entail locating an element (or position to insert an
		 * element). The code to do this can't be nicely factored out
		 * because subsequent uses require a snapshot of predecessor
		 * and/or successor and/or value fields which can't be returned
		 * all at once, at least not without creating yet another object
		 * to hold them -- creating such little objects is an especially
		 * bad idea for basic internal search operations because it adds
		 * to GC overhead.  (This is one of the few times I've wished Java
		 * had macros.) Instead, some traversal code is interleaved within
		 * insertion and removal operations.  The control logic to handle
		 * all the retry conditions is sometimes twisty. Most search is
		 * broken into 2 parts. findPredecessor() searches index nodes
		 * only, returning a base-level predecessor of the key. findNode()
		 * finishes out the base-level search. Even with this factoring,
		 * there is a fair amount of near-duplication of code to handle
		 * variants.
		 *
		 * For explanation of algorithms sharing at least a couple of
		 * features with this one, see Mikhail Fomitchev's thesis
		 * (http://www.cs.yorku.ca/~mikhail/), Keir Fraser's thesis
		 * (http://www.cl.cam.ac.uk/users/kaf24/), and Hakan Sundell's
		 * thesis (http://www.cs.chalmers.se/~phs/).
		 *
		 * Given the use of tree-like index nodes, you might wonder why
		 * this doesn't use some kind of search tree instead, which would
		 * support somewhat faster search operations. The reason is that
		 * there are no known efficient lock-free insertion and deletion
		 * algorithms for search trees. The immutability of the "down"
		 * links of index nodes (as opposed to mutable "left" fields in
		 * true trees) makes this tractable using only CAS operations.
		 *
		 * Notation guide for local variables
		 * Node:         b, n, f    for  predecessor, node, successor
		 * Index:        q, r, d    for index node, right, down.
		 *               t          for another index node
		 * Head:         h
		 * Levels:       j
		 * Keys:         k, key
		 * Values:       v, value
		 * Comparisons:  c
		 */

	private:
		static constexpr long long serialVersionUID = -8627078645895051609LL;

		/**
		 * Special value used to identify base-level header
		 */
//JAVA TO C++ CONVERTER TODO TASK: C++ does not allow initialization of static non-const/integral fields in their declarations:
		//inline const static void *BASE_HEADER = (void*)1;
		#define BASE_HEADER (void*)1

		/**
		 * Initializes or resets state. Needed by constructors, clone,
		 * clear, readObject. and ConcurrentSkipListSet.clone.
		 * (Note that comparatorField must be separately initialized.)
		 */
	public:
		void *map;
		PooledObjectPool<Key*> *keyPool;
		PooledObjectPool<KeyImpl*> *keyImplPool;
		PooledObjectPool<MyValue*> *valuePool;
		int *dataTypes;

		virtual void initialize()
		{
			//BASE_HEADER = new void*;
			keySet_Conflict = NULL;
			entrySet_Conflict = NULL;
			values_Conflict = NULL;
			descendingMap_Conflict = NULL;
			
			std::mt19937 rng;
			rng.seed(std::random_device()());
			std::uniform_int_distribution<std::mt19937::result_type> dist(0, INT_MAX);

			int r = dist(rng);

			randomSeed = r | 0x0100; // ensure nonzero

			Node<K, V> *node = new Node<K, V>(NULL, BASE_HEADER, NULL);
			node->addRef();
			atomicHead.set(new HeadIndex<K, V>(node, NULL, NULL, 1));
		}

		~ConcurrentSkipListMap() {
			Iterator<typename Map<K,V>::template Entry<K, V>*> *iterator = entrySet()->beginIterator();
			while (iterator->hasNext()) {
				typename Map<K,V>:: template Entry<K,V> *entry = iterator->nextEntry();
				remove(entry->getKey());
				delete entry;
				//delete entry->getKey();
				//delete entry->getValue();
			}
			if (descendingMap_Conflict != NULL) {
				delete descendingMap_Conflict;
			}
		}

		/**
		 * compareAndSet head node
		 */
	private:

		/* ---------------- Nodes -------------- */

		/**
		 * Nodes hold keys and values, and are singly linked in sorted
		 * order, possibly with some intervening marker nodes. The list is
		 * headed by a dummy node accessible as head.node. The value field
		 * is declared only as Object because it takes special non-V
		 * values for marker and header nodes.
		 */
	public:
		template<typename K1, typename V1>
		class Node : public PoolableObject<Node<K1, V1>*>
		{
		public:
			K1 key;
//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: volatile AtomicJava<Object> value;
			AtomicJava<void*> atomicValue;
//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: volatile AtomicJava<Node<K,V>> next;
			AtomicJava<Node<K1, V1>*> atomicNext;
			std::atomic<unsigned long> refCount;

			Node() {
				refCount.store(0);
				incrementNodeCount();
			}


			void addRef() {
				unsigned long c = refCount++;

			}

			void deleteRef(void *map, PooledObjectPool<Node<K1, V1>*> *nodePool) {
				unsigned long count = --refCount;
				if (count == 0) {
					pushNodeDelete(map, this);
					//if (this->atomicNext.get() != 0) {
						//this->atomicNext.get()->deleteRef(map, nodePool);
					//}
					//refCount.store(-99999999);
					//nodePool->free(this);
				}
			}

			Node<K1, V1> *allocate(int count) {
				return new Node[count];
			}

			Node<K1, V1> *allocate() {
				return new Node();
			}

			virtual Node<K1, V1> *getObjectAtOffset(Node<K1, V1> *array, int offset) {
				return &((Node*)array)[offset];
			}

				virtual const char *className() {
            		return "Node";
            	};


			~Node() {
				decrementNodeCount();
			}

			/**
			 * Creates a new regular node.
			 */
			Node(K1 key, const void *value, Node<K1, V1> *next)
			{
				refCount.store(0);
				incrementNodeCount();
				this->key = key;
				this->atomicValue.set((void*)value);
				this->atomicNext.set((Node<K1, V1>*)next);
				if (next != 0) {
					next->addRef();
				}
			}

			void init(K1 key, const void *value, Node<K1, V1> *next)
			{
				refCount.store(0);
				this->key = key;
				this->atomicValue.set((void*)value);
				this->atomicNext.set((Node<K1, V1>*)next);
				if (next != 0) {
					next->addRef();
				}
			}

			/**
			 * Creates a new marker node. A marker is distinguished by
			 * having its value field point to itself.  Marker nodes also
			 * have null keys, a fact that is exploited in a few places,
			 * but this doesn't distinguish markers from the base-level
			 * header node (head.node), which also has a null key.
			 */
			Node(Node<K1, V1> *next) : key(0)
			{
				refCount.store(0);
				incrementNodeCount();
				this->atomicValue.set(this);
				this->atomicNext.set((Node<K1, V1>*)next);
				if (next != 0) {
					next->addRef();
				}
			}

			void init(Node<K1, V1> *next) {
				refCount.store(0);
				key = 0;
				this->atomicValue.set(this);
				this->atomicNext.set((Node<K1, V1>*)next);
				if (next != 0) {
					next->addRef();
				}
			}
			/**
			 * compareAndSet value field
			 */
			bool casValue(void *cmp, void *val)
			{
				bool ret = atomicValue.compareAndSet(cmp, val);
				return ret;
			}

			/**
			 * compareAndSet next field
			 */
			bool casNext(Node<K1, V1> *cmp, Node<K1, V1> *val)
			{
				if (this == val) {
					printf("this === val\n");
				}
				bool ret = atomicNext.compareAndSet(cmp, val);
				return ret;
			}

			/**
			 * Returns true if this node is a marker. This method isn't
			 * actually called in any current code checking for markers
			 * because callers will have already read value field and need
			 * to use that read (not another done here) and so directly
			 * test if value points to node.
			 * @param n a possibly null reference to a node
			 * @return true if this node is a marker node
			 */
			bool isMarker()
			{
				return atomicValue.get() == this;
			}

			/**
			 * Returns true if this node is the header of base-level list.
			 * @return true if this node is header node
			 */
			bool isBaseHeader()
			{
				return atomicValue.get() == BASE_HEADER;
			}

			/**
			 * Tries to append a deletion marker to this node.
			 * @param f the assumed current successor of this node
			 * @return true if successful
			 */
			bool appendMarker(void *map, PooledObjectPool<Node<K1, V1>*> *nodePool, Node<K1, V1> *f)
			{
				//return true;

				Node<K1, V1> *next = NULL;// f == NULL ? NULL : f->atomicNext.get();
				Node<K1, V1> *z = (Node<K1, V1>*)nodePool->allocate();
				z->init(f);
				next = z;
				next->addRef();
				bool ret = casNext(f, next);
				if (!ret) {
					next->deleteRef(map, nodePool);
				}
				else {
					if (f != 0) {
						f->deleteRef(map, nodePool);
					}
				}
				return ret;
			}

			/**
			 * Helps out a deletion by appending marker or unlinking from
			 * predecessor. This is called during traversals when value
			 * field seen to be null.
			 * @param b predecessor
			 * @param f successor
			 */


			void helpDelete(void *map, Node<K1, V1> *b, Node<K1, V1> *f, PooledObjectPool<Node<K1, V1>*> *nodePool)
			{
				/*
				 * Rechecking links and then doing only one of the
				 * help-out stages per call tends to minimize CAS
				 * interference among helping threads.
				 */
				if (f == atomicNext.get()/*next*/ && this == b->atomicNext.get()/*next*/)
				{
					if (f == NULL)// || f->atomicValue.get() != f) // not already marked
					{
						//Node<K1, V1> *z = (Node<K1, V1>*)nodePool->allocate();
						//z->init(f);
						if (!appendMarker(map, nodePool, f)) {
							//pushNodeDelete(map, z);
						}
					}
					else
					{
						Node<K1, V1> *next = f->atomicNext.get();

						if (next != 0) {
							next->addRef();
						}
						if (b->casNext(this, next)) {
							//f->deleteRef(map, nodePool);
							this->key = 0;
							
							
							Node<K, V> *toDel = atomicNext.get();
							atomicNext.set(0);
							//if (atomicNext.compareAndSet(toDel, 0)) {
							//	if (toDel != 0) {
							//		toDel->deleteRef(map, nodePool);
							//	}
							//}

							this->deleteRef(map, nodePool);
							
							//pushRefDelete(map, this);
							
							//this->key = 0;
							//pushNodeDelete(map, this);
							//next = f->atomicNext.get();
						}
						else {
							if (next != 0) {
								next->deleteRef(map, nodePool);
							}
						}
					}
				}
			}

			/**
			 * Returns value if this node contains a valid key-value pair,
			 * else null.
			 * @return this node's value if it isn't a marker or header or
			 * is deleted, else null.
			 */
			V1 getValidValue()
			{
				auto v = atomicValue.get();
				if (v == this || v == BASE_HEADER)
				{
					return NULL;
				}
				return static_cast<V1>(v);
			}

			/**
			 * Creates and returns a new SimpleImmutableEntry holding current
			 * mapping if this node holds a valid value, else null.
			 * @return new entry or null
			 */
			typename AbstractMap<K1,V1>::template SimpleImmutableEntry<K1, V1> *createSnapshot()
			{
				V1 v = getValidValue();
				if (v == NULL)
				{
					return NULL;
				}
				return new typename AbstractMap<K1,V1>::template SimpleImmutableEntry<K1, V1>(key, v);
			}
		};

		void deleteNode(Node<K, V> *n) {
			//((Key*)n->key)->key->free(dataTypes);
			//keyImplPool->free(((Key*)n->key)->key);
			//keyPool->free((PoolableObject*)n->key);
			//valuePool->free((PoolableObject*)n->value);
			//
			//nodePool->free(n);

			//n->key = 0;
			//pushNodeDelete(map, n);
		}

		PooledObjectPool<Node<K,V>*> *nodePool = new PooledObjectPool<Node<K,V>*>(new Node<K, V>());

		//static NonSafeObjectPool<Node<K, V>> &nodePool = getNodePool();


		/* ---------------- Indexing -------------- */

		/**
		 * Index nodes represent the levels of the skip list.  Note that
		 * even though both Nodes and Indexes have forward-pointing
		 * fields, they have different types and are handled in different
		 * ways, that can't nicely be captured by placing field in a
		 * shared abstract class.
		 */
	public:
		template<typename K1, typename V1>
		class Index : public PoolableObject<Index<K1, V1>*>
		{
		public:
			Node<K1, V1> * node;
			Index<K1, V1> * down;
//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: volatile AtomicJava<Index<K,V>> right;
			AtomicJava<Index<K1, V1>*> atomicRight;

			Index() {
				incrementIndexCount();
			}

			~Index() {
				decrementIndexCount();
			}

			/**
			 * Creates index node with given values.
			 */
			Index(Node<K1, V1> *node, Index<K1, V1> *down, Index<K1, V1> *right) : node(node), down(down)
			{
				this->atomicRight.set(right);
				incrementIndexCount();
			}

			void init(Node<K1, V1> *node, Index<K1, V1> *down, Index<K1, V1> *right)
			{
				//node->addRef();
				this->node = node;
				this->down = down;
				this->atomicRight.set(right);
			}

			virtual Index<K1, V1> * allocate(int count) {
				return new Index<K1, V1>[count];
			}

			virtual Index<K1, V1> * allocate() {
				return new Index<K1, V1>();
			}

			virtual Index<K1, V1> * getObjectAtOffset(Index<K1, V1> *array, int offset) {
				return &((Index<K1, V1> *)array)[offset];

			}

			virtual const char *className() {
				return "Index";
			}


			virtual int hashCode() {
				return (int)(size_t)this;
			}

			virtual bool equals(void *o) {
				return o == this;
			}

			/**
			 * compareAndSet right field
			 */
			virtual bool casRight(Index<K1, V1> *cmp, Index<K1, V1> *val)
			{
				bool ret = atomicRight.compareAndSet(cmp, val);
				return ret;
			}

			/**
			 * Returns true if the node this indexes has been deleted.
			 * @return true if indexed node is known to be deleted
			 */
			virtual bool indexesDeletedNode()
			{
				return node->atomicValue.get() == NULL;
			}

			/**
			 * Tries to CAS newSucc as successor.  To minimize races with
			 * unlink that may lose this index node, if the node being
			 * indexed is known to be deleted, it doesn't try to link in.
			 * @param succ the expected current successor
			 * @param newSucc the new successor
			 * @return true if successful
			 */
			virtual bool link(void *map, Index<K1, V1> *succ, Index<K1, V1> *newSucc)
			{
				Node<K1, V1> *n = node;
				newSucc->atomicRight.set(succ);
				if (n->atomicValue.get() != NULL) {
				 if (casRight(succ, newSucc)) {
				 	//right = newSucc;
					 //pushIndexDelete(map, succ);
				 	return true;
				 }

				}
				return false;
			}

			/**
			 * Tries to CAS right field to skip over apparent successor
			 * succ.  Fails (forcing a retraversal by caller) if this node
			 * is known to be deleted.
			 * @param succ the expected current successor
			 * @return true if successful
			 */
			virtual bool unlink(void *map, PooledObjectPool<Node<K1, V1>*> *nodePool, Index<K1, V1> *succ)
			{
				bool ret = !indexesDeletedNode() && casRight(succ, succ->atomicRight.get());
				if (ret) {
					//pushNodeDelete(map, succ->node);
					
					//succ->node->deleteRef(map, nodePool);
					pushIndexDelete(map, succ);
					
					//delete succ;
					//right = succ->right;
				}
				return ret;
			}
		};

		PooledObjectPool<Index<K, V>*> *indexPool = new PooledObjectPool<Index<K, V>*>(new Index<K, V>());


		/* ---------------- Head nodes -------------- */

		/**
		 * Nodes heading each level keep track of their level.
		 */
	public:
		template<typename K1, typename V1>
		class HeadIndex : public Index<K1, V1>
		{
		public:
			const int level;
			HeadIndex(const Node<K1, V1> *node, const Index<K1, V1> *down, const Index<K1, V1> *right, int level) : 
				Index<K1,V1>((Node<K1, V1> *)node, (Index<K1, V1> *)down, (Index<K1, V1> *)right), level(level)
			{
			}
		};

		bool casHead(HeadIndex<K, V> *cmp, HeadIndex<K, V> *val)
		{
			bool ret = atomicHead.compareAndSet(cmp, val);
			return ret;
		}

		/* ---------------- Comparison utilities -------------- */

		/**
		 * Represents a key with a comparatorField as a skiplist.Comparable.
		 *
		 * Because most sorted collections seem to use natural ordering on
		 * Comparables (Strings, Integers, etc), most internal methods are
		 * geared to use them. This is generally faster than checking
		 * per-comparison whether to use comparatorField or comparable because
		 * it doesn't require a (skiplist.Comparable) cast for each comparison.
		 * (Optimizers can only sometimes remove such redundant checks
		 * themselves.) When Comparators are used,
		 * ComparableUsingComparators are created so that they act in the
		 * same way as natural orderings. This penalizes use of
		 * Comparators vs Comparables, which seems like the right
		 * tradeoff.
		 */
	public:
		template<typename K1>
		class ComparableUsingComparator : public Comparable<K1>
		{
		public:
			const K1 actualKey;
			Comparator<K1> *const cmp;
			ComparableUsingComparator(K1 key, Comparator<K1> *cmp) : actualKey(key), cmp(cmp)
			{
			}
			int compareTo(K1 k2)
			{
				return cmp->compare(actualKey, k2);
			}
		};

		/**
		 * If using comparatorField, return a ComparableUsingComparator, else
		 * cast key as skiplist.Comparable, which may cause ClassCastException,
		 * which is propagated back to caller.
		 */
	private:
		Comparable<K> * comparable(void *key) throw(ClassCastException)
  		{
			if (key == NULL)
			{
				throw NullPointerException();
			}
			if (comparatorField != NULL)
			{
				return new ComparableUsingComparator<K>(static_cast<K>(key), comparatorField);
			}
			else
			{
				return static_cast<Comparable<K>*>(key);
			}
		}

		/**
		 * Compares using comparatorField or natural ordering. Used when the
		 * ComparableUsingComparator approach doesn't apply.
		 */
	public:
		virtual int compare(K k1, K k2) throw(ClassCastException)
		{
			Comparator<K> *cmp = comparatorField;
			if (cmp != NULL)
			{
				return cmp->compare(k1, k2);
			}
			else
			{
				return (static_cast<Comparable<K>*>(k1))->compareTo(k2);
			}
		}

		/**
		 * Returns true if given key greater than or equal to least and
		 * strictly less than fence, bypassing either test if least or
		 * fence are null. Needed mainly in submap operations.
		 */
		virtual bool inHalfOpenRange(K key, K least, K fence)
		{
			if (key == NULL)
			{
				throw NullPointerException();
			}
			return ((least == NULL || compare(key, least) >= 0) && (fence == NULL || compare(key, fence) < 0));
		}

		/**
		 * Returns true if given key greater than or equal to least and less
		 * or equal to fence. Needed mainly in submap operations.
		 */
		virtual bool inOpenRange(K key, K least, K fence)
		{
			if (key == NULL)
			{
				throw NullPointerException();
			}
			return ((least == NULL || compare(key, least) >= 0) && (fence == NULL || compare(key, fence) <= 0));
		}

		/* ---------------- Traversal -------------- */

		/**
		 * Returns a base-level node with key strictly less than given key,
		 * or the base-level header if there is no such node.  Also
		 * unlinks indexes to deleted nodes found along the way.  Callers
		 * rely on this side-effect of clearing indices to deleted nodes.
		 * @param key the key
		 * @return a predecessor of key
		 */
	private:
		Node<K, V> *findPredecessor(Comparable<K> *key)
		{
			if (key == NULL)
			{
				throw NullPointerException(); // don't postpone errors
			}
			for (;;)
			{
				Index<K, V> *q = atomicHead.get();//head;
				Index<K, V> *r = q->atomicRight.get();//right;
				for (;;)
				{

					if (r != NULL)
					{
						Node<K, V> *n = r->node;
						if (n == 0) {
							printf("n == 0\n");
							//q->unlink(map, nodePool, r);
							//break;
						}
						//n->addRef();
						K k = n->key;
						//if (k == 0) {
							//printf("k == 0\n");
							//break;
						//}
						if (k == 0 || n->atomicValue.get() == NULL)
						{
							if (!q->unlink(map, nodePool, r))
							{
								//delete r->node->key;
								//delete (V)r->node->value.get();
								//delete r->node;
								//delete r;
							
								//n->deleteRef(map, nodePool);
								break; // restart
							}
							//delete r->node->key;
							//delete (V)r->node->value.get();
							//delete r->node;
							//r->node->key = 0;
							//pushNodeDelete(map, r->node);
							//delete r;

							r = q->atomicRight.get();//right; // reread r
							//n->deleteRef(map, nodePool);
							continue;
						}
						//printf("findPred - compare begin\n");
						//fflush(stdout);
						if (key->compareTo(k) > 0)
						{
						//printf("findPred - compare end\n");
						//fflush(stdout);
							q = r;
							r = r->atomicRight.get();//right;
							//n->deleteRef(map, nodePool);
							continue;
						}
						//printf("findPred - compare end\n");
						//fflush(stdout);
						
						//n->deleteRef(map, nodePool);

					}
					Index<K, V> *d = q->down;
					if (d != NULL)
					{
						q = d;
						r = d->atomicRight.get();//right;
					}
					else
					{
						Node<K, V> *n = q->node;
						if (n == 0) {
							printf("n == 0\n");
						}
						//n->addRef();
						return n;
					}
				}
			}
		}

		/**
		 * Returns node holding key or null if no such, clearing out any
		 * deleted nodes seen along the way.  Repeatedly traverses at
		 * base-level looking for key starting at predecessor returned
		 * from findPredecessor, processing base-level deletions as
		 * encountered. Some callers rely on this side-effect of clearing
		 * deleted nodes.
		 *
		 * Restarts occur, at traversal step centered on node n, if:
		 *
		 *   (1) After reading n's next field, n is no longer assumed
		 *       predecessor b's current successor, which means that
		 *       we don't have a consistent 3-node snapshot and so cannot
		 *       unlink any subsequent deleted nodes encountered.
		 *
		 *   (2) n's value field is null, indicating n is deleted, in
		 *       which case we help out an ongoing structural deletion
		 *       before retrying.  Even though there are cases where such
		 *       unlinking doesn't require restart, they aren't sorted out
		 *       here because doing so would not usually outweigh cost of
		 *       restarting.
		 *
		 *   (3) n is a marker or n's predecessor's value field is null,
		 *       indicating (among other possibilities) that
		 *       findPredecessor returned a deleted node. We can't unlink
		 *       the node because we don't know its predecessor, so rely
		 *       on another call to findPredecessor to notice and return
		 *       some earlier predecessor, which it will do. This check is
		 *       only strictly needed at beginning of loop, (and the
		 *       b.value check isn't strictly needed at all) but is done
		 *       each iteration to help avoid contention with other
		 *       threads by callers that will fail to be able to change
		 *       links, and so will retry anyway.
		 *
		 * The traversal loops in doPut, doRemove, and findNear all
		 * include the same three kinds of checks. And specialized
		 * versions appear in findFirst, and findLast and their
		 * variants. They can't easily share code because each uses the
		 * reads of fields held in locals occurring in the orders they
		 * were performed.
		 *
		 * @param key the key
		 * @return node holding key, or null if no such
		 */
		Node<K, V> *findNode(Comparable<K> * key)
		{
			for (;;)
			{
				Node<K, V> *b = findPredecessor(key);
				Node<K, V> *n = b->atomicNext.get();//next;
				if (n != 0) {
					//n->addRef();
				}
				for (;;)
				{
					if (n == NULL)
					{
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						return NULL;
					}
					Node<K, V> *f = n->atomicNext.get();//next;
					if (f != 0) {
						//f->addRef();
					}
					if (n != b->atomicNext.get()) // inconsistent read
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						break;
					}
					void *v = /*&*/n->atomicValue.get();//value;
					if (v == NULL)
					{ // n is deleted
						n->helpDelete(map, b, f, nodePool);
						deleteNode(n);
						if (f != 0) {
							//f->deleteRef(map, nodePool);;
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						break;
					}
					if (v == n || b->atomicValue.get() == NULL) // b is deleted
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						break;
					}
					int c = key->compareTo(n->key);
					if (c == 0)
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						return n;
					}
					if (c < 0)
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						return NULL;
					}
					//b->deleteRef(map, nodePool);
					if (n != 0) {
						//n->addRef();
					}
					b = n;
					if (n != 0) {
						//n->deleteRef(map, nodePool);
					}
					if (f != 0) {
						//f->addRef();
					}
					n = f;
				}
			}
		}

		/**
		 * Specialized variant of findNode to perform skiplist.Map.get. Does a weak
		 * traversal, not bothering to fix any deleted index nodes,
		 * returning early if it happens to see key in index, and passing
		 * over any deleted base nodes, falling back to getUsingFindNode
		 * only if it would otherwise return value from an ongoing
		 * deletion. Also uses "bound" to eliminate need for some
		 * comparisons (see Pugh Cookbook). Also folds uses of null checks
		 * and node-skipping because markers have null keys.
		 * @param okey the key
		 * @return the value, or null if absent
		 */
		V doGet(K okey)
		{
			Comparable<K> *key = comparable(okey);
			Node<K, V> *bound = NULL;
			Index<K, V> *q = atomicHead.get();
			Index<K, V> *r = q->atomicRight.get();
			Node<K, V> *n;
			K k;
			int c;
			for (;;)
			{
				Index<K, V> *d;
				// Traverse rights
				if (r != NULL && (n = r->node) != bound && (k = n->key) != NULL)
				{
					if ((c = key->compareTo(k)) > 0)
					{
						q = r;
						r = r->atomicRight.get();
						continue;
					}
					else if (c == 0)
					{
						void *v = n->atomicValue.get();
						return (v != NULL)? static_cast<V>(v): getUsingFindNode(key);
					}
					else
					{
						bound = n;
					}
				}

				// Traverse down
				if ((d = q->down) != NULL)
				{
					q = d;
					r = d->atomicRight.get();
				}
				else
				{
					break;
				}
			}

			// Traverse nexts
			for (n = q->node->atomicNext.get(); n != NULL; n = n->atomicNext.get())
			{
				if ((k = n->key) != NULL)
				{
					if ((c = key->compareTo(k)) == 0)
					{
						void *v = n->atomicValue.get();
						return (v != NULL)? static_cast<V>(v): getUsingFindNode(key);
					}
					else if (c < 0)
					{
						break;
					}
				}
			}
			return NULL;
		}

		/**
		 * Performs map.get via findNode.  Used as a backup if doGet
		 * encounters an in-progress deletion.
		 * @param key the key
		 * @return the value, or null if absent
		 */
		V getUsingFindNode(Comparable<K> *key)
		{
			/*
			 * Loop needed here and elsewhere in case value field goes
			 * null just as it is about to be returned, in which case we
			 * lost a race with a deletion, so must retry.
			 */
			for (;;)
			{
				Node<K, V> *n = findNode(key);
				if (n == NULL)
				{
					return NULL;
				}
				void *v = n->atomicValue.get();
				if (v != NULL)
				{
					return static_cast<V>(v);
				}
			}
		}

		/* ---------------- Insertion -------------- */

		/**
		 * Main insertion method.  Adds element if not present, or
		 * replaces value if present and onlyIfAbsent is false.
		 * @param kkey the key
		 * @param vadlue  the value that must be associated with key
		 * @param onlyIfAbsent if should not insert if already present
		 * @return the old value, or null if newly inserted
		 */
		V doPut(K kkey, V value, bool onlyIfAbsent)
		{
			int callCount = 0;
			//printf("comparable\n");
			//fflush(stdout);
			Comparable<K> *key = comparable(kkey);
			for (;;)
			{
			//if (callCount++ > 1000) {
			//	printf("findPred\n");
				//fflush(stdout);
			//	}
				Node<K, V> *b = findPredecessor(key);
				//if (b == 0) {
				//	continue;
				//}
				//printf("findPred - end\n");
				//fflush(stdout);
				Node<K, V> *n = b->atomicNext.get();//next;
				if (n != 0) {
					//n->addRef();
				}
				for (;;)
				{
			//if (callCount++ > 1000) {
			//	printf("inner\n");
				//fflush(stdout);
			//	}
					if (n != NULL)
					{
						Node<K, V> *f = n->atomicNext.get();//next;
						if (f != 0) {
							//f->addRef();
						}
						if (n != b->atomicNext.get()) // inconsistent read
						{
							//printf("n != b->next\n");
							//fflush(stdout);
							if (f != 0) {
							//	f->deleteRef(map, nodePool);
							}
							if (n != 0) {
								//n->deleteRef(map, nodePool);
							}
							//b->deleteRef(map, nodePool);
							break;
						}
						void *v = n->atomicValue.get();//value;
						if (v == NULL)
						{ // n is deleted
							//printf("helpDelete\n");
							//fflush(stdout);

							n->helpDelete(map, b, f, nodePool);
							deleteNode(n);
							if (f != 0) {
							//	f->deleteRef(map, nodePool);
							}
							if (n != 0) {
								//n->deleteRef(map, nodePool);
							}
							//b->deleteRef(map, nodePool);
							break;
						}
						if (v == n || b->atomicValue.get() == NULL) // b is deleted
						{
							//printf("v == n || b->value == NULL\n");
							//fflush(stdout);
							if (f != 0) {
							//	f->deleteRef(map, nodePool);
							}
							if (n != 0) {
								//n->deleteRef(map, nodePool);
							}
							//b->deleteRef(map, nodePool);
							break;
						}

						//printf("compareTo - begin\n");
						//fflush(stdout);

						int c = key->compareTo(n->key);
						if (c > 0)
						{
							if (b == n) {
								printf("doPut b == n\n");
								break;
							}
							if (n == f) {
								printf("doPut n == f\n");
								//n->atomicNext.set(0);
								break;
							}
							if (b == f) {
								printf("doPut b == f\n");
								break;
							}
							//printf("compareTo - c > 0\n");
						//fflush(stdout);
							if (n != 0) {
							//	n->addRef();
							}
							//b->deleteRef(map, nodePool);
							b = n;
							if (f != 0) {
								//f->addRef();
							}
							if (n != 0) {
							//	n->deleteRef(map, nodePool);
							}
							n = f;
							continue;
						}
						if (c == 0)
						{
							void *prev = v;
							if (onlyIfAbsent || n->casValue(prev, value))
							{
								if (!onlyIfAbsent) {
					//				n->value = value;
								}
								//((Key*)key)->key->free(dataTypes);
								//keyImplPool->free(((Key*)key)->key);
								//keyPool->free(kkey);
								if (f != 0) {
							//		f->deleteRef(map, nodePool);
								}
								if (n != 0) {
									//n->deleteRef(map, nodePool);
								}
							//	b->deleteRef(map, nodePool);
								return static_cast<V>(prev);
							}
							else
							{
						//printf("compareTo - c == 0, break\n");
						//fflush(stdout);
								if (f != 0) {
							//		f->deleteRef(map, nodePool);
								}
								if (n != 0) {
									//n->deleteRef(map, nodePool);
								}
							//	b->deleteRef(map, nodePool);
								break; // restart if lost race to replace value
							}
						}
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						// else c < 0; fall through
					}

					//printf("before node\n");
					//fflush(stdout);

						//printf("newNode - begin\n");
						//fflush(stdout);
					Node<K, V> *z = (Node<K,V>*)nodePool->allocate();
					//printf("got allocation\n");
					//fflush(stdout);
					z->init(kkey, value, n);
					//printf("inited node\n");
					//fflush(stdout);
					//printf("after node\n");
					//fflush(stdout);

						//printf("newNode - end\n");
						//fflush(stdout);

					z->addRef();
					if (!b->casNext(n, z))
					{
						z->atomicValue.set(0);
						z->key = 0;
						z->deleteRef(map, nodePool);

						//printf("!b->casNext(n, z)\n");
						//fflush(stdout);

						//f->deleteRef(nodePool);
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						break; // restart if lost race to append to b
					}
					if (n != 0) {
						n->deleteRef(map, nodePool);
					}
					//b->next = z;

					//std::mt19937 rng;
					//rng.seed(std::random_device()());
					//std::uniform_int_distribution<std::mt19937::result_type> dist(0, INT_MAX);

					int x = randomSeed;
					x ^= x << 13;
					x ^= static_cast<int>(static_cast<unsigned int>(x) >> 17);
					randomSeed = x ^= x << 5;

					int r = abs(x);//rand() % INT_MAX;//dist(rng);
//					r ^= r << 13;   // xorshift
//					r ^= r >> 17;
//					r ^= r << 5;

					//if enabled, puts eventually hang
			        //if ((r & 0x80000001) == 0)  // test highest and lowest bits
					if ((r & 0x00000001) == 0)  // test highest and lowest bits
					{
						int level = 1, max;
						while (((r >>= 1) & 1) != 0)
						{
							//printf("level++\n");
							++level;
							}

						//level = randomLevel();
						//if (level > 0)
						//{
							insertIndex(key, z, level);
						//}
					}
					//f->deleteRef(map, nodePool);
					if (n != 0) {
						//n->deleteRef(map, nodePool);
					}
					//b->deleteRef(map, nodePool);
					return NULL;
				}
			}
		}

		/**
		 * Returns a random level for inserting a new node.
		 * Hardwired to k=1, p=0.5, max 31 (see above and
		 * Pugh's "Skip List Cookbook", sec 3.4).
		 *
		 * This uses the simplest of the generators described in George
		 * Marsaglia's "Xorshift RNGs" paper.  This is not a high-quality
		 * generator but is acceptable here.
		 */
		int randomLevel()
		{
			int x = randomSeed;
			x ^= x << 13;
			x ^= static_cast<int>(static_cast<unsigned int>(x) >> 17);
			randomSeed = x ^= x << 5;
			//if ((x & 0x8001) != 0) // test highest and lowest bits
			//{
			//	return 0;
			//}
			int level = 1;
			while (((x = static_cast<int>(static_cast<unsigned int>(x) >> 1)) & 1) != 0)
			{
				++level;
			}
			

			/*
			std::mt19937 rng;
			rng.seed(std::random_device()());
			std::uniform_int_distribution<std::mt19937::result_type> dist(0, 17);

			int level = dist(rng);
			*/
			return level;
		}

		/**
		 * Creates and adds index nodes for the given node.
		 * @param z the node
		 * @param level the level of the index
		 */
		void insertIndex(Comparable<K> *key, Node<K, V> *z, int level)
		{
			HeadIndex<K, V> *h = atomicHead.get();//head;
			int max = h->level;

			if (level <= max)
			{
				Index<K, V> *idx = NULL;
				for (int i = 1; i <= level; ++i)
				{
					Index<K, V> *prev = idx;
					idx = indexPool->allocate();
					idx->init(z, prev, NULL);// new Index<K, V>(z, idx, NULL);
				}
				addIndex(key, idx, h, level);

			}
			else
			{ // Add a new level
				/*
				 * To reduce interference by other threads checking for
				 * empty levels in tryReduceLevel, new levels are added
				 * with initialized right pointers. Which in turn requires
				 * keeping levels in an array to access them while
				 * creating new head index nodes from the opposite
				 * direction.
				 */
				level = max + 1;
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Index<K,V>[] idxs = (Index<K,V>[])new Index[level+1];
				Index<K, V> **idxs = static_cast<Index<K, V>**>(new Index<K,V>*[level + 1]);
				Index<K, V> *idx = NULL;
				for (int i = 1; i <= level; ++i)
				{
					Index<K, V> *prev = idx;
					idxs[i] = idx = indexPool->allocate();
					idxs[i]->init(z, prev, NULL);// new Index<K, V>(z, idx, NULL);
				}

				HeadIndex<K, V> *oldh;
				int k;
				for (;;)
				{
					//printf("inner insert index\n");
					oldh = atomicHead.get();//head;
					int oldLevel = oldh->level;
					if (level <= oldLevel)
					{ // lost race to add level
						k = level;
						break;
					}
					HeadIndex<K, V> *newh = oldh;
					Node<K, V> *oldbase = oldh->node;
					for (int j = oldLevel + 1; j <= level; ++j)
					{
						newh = new HeadIndex<K, V>(oldbase, newh, idxs[j], j);
					}
					if (casHead(oldh, newh))
					{
						//head = newh;
						oldh = newh;
						k = oldLevel;
						idx = idxs[level = oldLevel];
						break;
					}
				}
				//addIndex(idxs[k], oldh, k);
				addIndex(key, idx, oldh, level);
			}
		}

		/**
		 * Adds given index nodes from given level down to 1.
		 * @param idx the topmost index node being inserted
		 * @param h the value of head to use to insert. This must be
		 * snapshotted by callers to provide correct insertion level
		 * @param indexLevel the level of the index
		 */
		void addIndexNew(Index<K, V> *idx, HeadIndex<K, V> *h, int indexLevel)
		{
			// Track next level to insert in case of retries
			int insertionLevel = indexLevel;
			Comparable<K> *key = comparable(idx->node->key);
			if (key == NULL)
			{
				throw NullPointerException();
			}

			// Similar to findPredecessor, but adding index nodes along
			// path to key.
			for (;;)
			{
				//printf("addIndex\n");
				int j = h->level;
				Index<K, V> *q = h;
				Index<K, V> *r = q->atomicRight.get();//right;
				Index<K, V> *t = idx;
				for (;;)
				{
				//printf("inner addIndex\n");
					if (q == NULL || t == NULL) {
						return;
					}

					if (r != NULL)
					{
						Node<K, V> *n = r->node;
						// compare before deletion check avoids needing recheck
						int c = key->compareTo(n->key);
						if (n->value == NULL)
						{
							if (!q->unlink(map, nodePool, r))
							{
								break;
							}
							r = q->atomicRight.get();//right;
							continue;
						}
						if (c > 0)
						{
							q = r;
							r = r->atomicRight.get();//right;
							continue;
						}
					}

					if (j == insertionLevel)
					{
						if (!q->link(map, r, t))
						{
							break; // restart
						}
						// Don't insert index if node already deleted
						if (t->indexesDeletedNode())
						{
							findNode(key); // cleans up
							return;
						}
						if (--insertionLevel == 0)
						{
							// need final deletion check before return
							//if (t->indexesDeletedNode())
							//{
							//	findNode(key);
							//}
							return;
						}
					}

					if (--j >= insertionLevel && j < indexLevel)
					{
						t = t->down;
					}
					q = q->down;
					r = q->atomicRight.get();//right;
				}
			}
		}

		void addIndex(Comparable<K> *key, Index<K, V> *idx, HeadIndex<K, V> *h, int indexLevel)
		{
			// Track next level to insert in case of retries
			int insertionLevel = indexLevel;
			if (key == NULL)
			{
				throw NullPointerException();
			}

			// Similar to findPredecessor, but adding index nodes along
			// path to key.
			for (;;)
			{
				int j = h->level;
				Index<K, V> *q = h;
				Index<K, V> *r = q->atomicRight.get();//right;
				Index<K, V> *t = idx;
				for (;;)
				{
					if (q == NULL || t == NULL)
						return;

					if (r != NULL)
					{
						Node<K, V> *n = r->node;
						// compare before deletion check avoids needing recheck
						int c = key->compareTo(n->key);
						if (n->atomicValue.get() == NULL)
						{
							if (!q->unlink(map, nodePool, r))
							{
								break;
							}
							r = q->atomicRight.get();//right;
							continue;
						}
						if (c > 0)
						{
							q = r;
							r = r->atomicRight.get();//right;
							continue;
						}
					}

					if (j == insertionLevel)
					{
						if (!q->link(map, r, t))
						{
							break; // restart
						}
						// Don't insert index if node already deleted
						if (t->indexesDeletedNode())
						{
							findNode(key); // cleans up
							return;
						}
						if (--insertionLevel == 0)
						{
							// need final deletion check before return
//							if (t->indexesDeletedNode())
//							{
//								findNode(key);
//							}
							return;
						}
					}

					if (--j >= insertionLevel && j < indexLevel)
					{
						t = t->down;
					}
					q = q->down;
					r = q->atomicRight.get();//right;
				}
			}
		}

		/* ---------------- Deletion -------------- */
		/**
		 * Main deletion method. Locates node, nulls value, appends a
		 * deletion marker, unlinks predecessor, removes associated index
		 * nodes, and possibly reduces head index level.
		 *
		 * Index nodes are cleared out simply by calling findPredecessor.
		 * which unlinks indexes to deleted nodes found along path to key,
		 * which will include the indexes to this node.  This is done
		 * unconditionally. We can't check beforehand whether there are
		 * index nodes because it might be the case that some or all
		 * indexes hadn't been inserted yet for this node during initial
		 * search for it, and we'd like to ensure lack of garbage
		 * retention, so must call to be sure.
		 *
		 * @param okey the key
		 * @param value if non-null, the value that must be
		 * associated with key
		 * @return the node, or null if not found
		 */
	public:
		virtual V doRemove(void *okey, void *value)
		{
			int callCount = 0;
			Comparable<K> *key = comparable(okey);
			for (;;)
			{
						//if (callCount++ > 1000) {

				 //printf("outer\n");
				 //fflush(stdout);
//}

				Node<K, V> *b = findPredecessor(key);
				Node<K, V> *n = b->atomicNext.get();//next;
				if (n != 0) {
					//n->addRef();
				}
				for (;;)
				{
					if (n == NULL)
					{
						//b->deleteRef(map, nodePool);
						return NULL;
					}
					Node<K, V> *f = n->atomicNext.get();//next;
			 		if (f != 0) {
						//f->addRef();
					}
					if (n != b->atomicNext.get()/*next*/) // inconsistent read
					{
						//printf("n != b->next\n");
						//fflush(stdout);
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						break;
					}
					void *v = n->atomicValue.get();//value;
					if (v == NULL)
					{ // n is deleted
						n->helpDelete(map, b, f, nodePool);
						deleteNode(n);
						//printf("v == NULL\n");
						//fflush(stdout);
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						break;
					}
					if (v == n || b->atomicValue.get()/*value*/ == NULL) // b is deleted
					{
						//printf("v == n || b->value == NULL\n");
						//fflush(stdout);
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						break;
					}
					int c = key->compareTo(n->key);
					if (c < 0)
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
				//		b->deleteRef(map, nodePool);
						return NULL;
					}
					if (c > 0)
					{
						//printf("c > 0\n");
						//fflush(stdout);

						//b->deleteRef(map, nodePool);
						if (n != 0) {
							//n->addRef();
						}
						b = n;
						if (f != 0) {
							//f->addRef();
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						n = f;
						continue;
					}
					if (value != NULL && !((V)value)->equals(v))
					{
						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						if (n != 0) {
							//n->deleteRef(map, nodePool);
						}
						//b->deleteRef(map, nodePool);
						return NULL;
					}
					void *prev = v;
					if (!n->casValue(prev, NULL))
					{
						//printf("!n->casValue(prev, NULL)\n");
						//fflush(stdout);

						if (f != 0) {
							//f->deleteRef(map, nodePool);
						}
						break;
					}
					//n->value = NULL;
					//delete (V)prev;

					//Node<K, V> *z = (Node<K, V>*)nodePool->allocate();
					//z->init(f);

					//bool appended = n->appendMarker(f, z);
					bool appended = n->appendMarker(map, nodePool, f);

					if (f != 0) {
						f->addRef();
					}
					if (!appended)
					{
						//if (!appended) {
						//	pushNodeDelete(map, z);
						//}
						findNode(key); // Retry via findNode
						if (f != 0) {
							f->deleteRef(map, nodePool);
						}
					}
					else if (!b->casNext(n, f))
					{
						findNode(key); // Retry via findNode
						if (f != 0) {
							f->deleteRef(map, nodePool);
						}
					}
					else
					{
						pushRefDelete(map, n);
						//n->atomicNext.get()->deleteRef(map, nodePool);
						//n->atomicNext.set(0);

						//n->atomicNext.set(0);
						//n->deleteRef(map, nodePool);
						//pushNodeDelete(map, n);

//						b->next = f;
						Node<K, V> * x = findPredecessor(key); // Clean index
						if (atomicHead.get()->atomicRight.get()/*right*/ == NULL)
						{
							tryReduceLevel();
						}
						if (x != 0) {
							//x->deleteRef(map, nodePool);
						}
					}
					
					//((Key*)n->key)->key->free(dataTypes);
					//keyImplPool->free(((Key*)n->key)->key);
					//keyPool->free(n->key);
					//nodePool->free(n);


					if (f != 0) {
						//f->deleteRef(map, nodePool);
					}
					if (n != 0) {
						//n->deleteRef(map, nodePool);
					}
					//b->deleteRef(map, nodePool);
					return static_cast<V>(prev);
				}
				if (n != 0) {
					//n->deleteRef(map, nodePool);
				}
				//b->deleteRef(map, nodePool);
			}
		}

		/**
		 * Possibly reduce head level if it has no nodes.  This method can
		 * (rarely) make mistakes, in which case levels can disappear even
		 * though they are about to contain index nodes. This impacts
		 * performance, not correctness.  To minimize mistakes as well as
		 * to reduce hysteresis, the level is reduced by one only if the
		 * topmost three levels look empty. Also, if the removed level
		 * looks non-empty after CAS, we try to change it back quick
		 * before anyone notices our mistake! (This trick works pretty
		 * well because this method will practically never make mistakes
		 * unless current thread stalls immediately before first CAS, in
		 * which case it is very unlikely to stall again immediately
		 * afterwards, so will recover.)
		 *
		 * We put up with all this rather than just let levels grow
		 * because otherwise, even a small map that has undergone a large
		 * number of insertions and removals will have a lot of levels,
		 * slowing down access more than would an occasional unwanted
		 * reduction.
		 */
	private:
		void tryReduceLevel()
		{
			HeadIndex<K, V> *h = atomicHead.get();
			HeadIndex<K, V> *d;
			HeadIndex<K, V> *e;
			if (h->level > 3 && (d = static_cast<ConcurrentSkipListMap::HeadIndex<K, V>*>(h->down)) != NULL && 
				(e = static_cast<ConcurrentSkipListMap::HeadIndex<K, V>*>(d->down)) != NULL && e->atomicRight.get() == NULL &&
				d->atomicRight.get() == NULL && h->atomicRight.get() == NULL && casHead(h, d) && h->atomicRight.get() != NULL) // recheck
			{
				if (casHead(d, h)) { // try to backout
					//pushIndexDelete(map, d);
				}
			}
		}

		/* ---------------- Finding and removing first element -------------- */

		/**
		 * Specialized variant of findNode to get first valid node.
		 * @return first node or null if empty
		 */
	public:
		virtual Node<K, V> *findFirst()
		{
			for (;;)
			{
				Node<K, V> *b = atomicHead.get()->node;
				Node<K, V> *n = b->atomicNext.get();
				if (n == NULL)
				{
					return NULL;
				}
				if (n->atomicValue.get() != NULL)
				{
					return n;
				}
				n->helpDelete(map, b, n->atomicNext.get(), nodePool);
				deleteNode(n);
			}
		}

		/**
		 * Removes first entry; returns its snapshot.
		 * @return null if empty, else snapshot of first entry
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *doRemoveFirstEntry()
		{
			for (;;)
			{
				Node<K, V> *b = atomicHead.get()->node;
				Node<K, V> *n = b->atomicNext.get();
				if (n == NULL)
				{
					return NULL;
				}
				Node<K, V> *f = n->atomicNext.get();
				if (n != b->atomicNext.get())
				{
					continue;
				}
				void *v = n->atomicValue.get();
				if (v == NULL)
				{
					n->helpDelete(map, b, f, nodePool);
					deleteNode(n);
					continue;
				}
				void *prev = v;
				if (!n->casValue(prev, NULL))
				{
					continue;
				}
				valuePool->free((V)prev);

				//Node<K, V> *z = (Node<K, V>*)nodePool->allocate();
				//z->init(f);

				if (f != 0) {
					f->addRef();
				}
				if (b == f) {
					printf("bogus\n");
				}
				if (!n->appendMarker(map, nodePool, f)) {
					//pushNodeDelete(map, z);
					findFirst(); // retry
					if (f != 0) {
						f->deleteRef(map, nodePool);
					}
				}
				else if (!b->casNext(n, f)) {
					findFirst(); // retry
					if (f != 0) {
						f->deleteRef(map, nodePool);
					}
				}
				else {
					n->deleteRef(map, nodePool);
					//pushNodeDelete(map, n);
				}
				clearIndexToFirst();
				return new typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>(n->key, static_cast<V>(v));
			}
		}

		/**
		 * Clears out index nodes associated with deleted first entry.
		 */
	private:
		void clearIndexToFirst()
		{
			for (;;)
			{
				Index<K, V> *q = atomicHead.get();
				for (;;)
				{
					Index<K, V> *r = q->atomicRight.get();
					if (r != NULL && r->indexesDeletedNode() && !q->unlink(map, nodePool, r))
					{
						break;
					}
					if ((q = q->down) == NULL)
					{
						if (atomicHead.get()->atomicRight.get() == NULL)
						{
							tryReduceLevel();
						}
						return;
					}
				}
			}
		}


		/* ---------------- Finding and removing last element -------------- */

		/**
		 * Specialized version of find to get last valid node.
		 * @return last node or null if empty
		 */
	public:
		virtual Node<K, V> *findLast()
		{
			/*
			 * findPredecessor can't be used to traverse index level
			 * because this doesn't use comparisons.  So traversals of
			 * both levels are folded together.
			 */
			Index<K, V> *q = atomicHead.get();
			for (;;)
			{
				Index<K, V> *d, *r;
				if ((r = q->atomicRight.get()) != NULL)
				{
					if (r->indexesDeletedNode())
					{
						q->unlink(map, nodePool, r);
						q = atomicHead.get(); // restart
					}
					else
					{
						q = r;
					}
				}
				else if ((d = q->down) != NULL)
				{
					q = d;
				}
				else
				{
					Node<K, V> *b = q->node;
					Node<K, V> *n = b->atomicNext.get();
					for (;;)
					{
						if (n == NULL)
						{
							return (b->isBaseHeader())? NULL : b;
						}
						Node<K, V> *f = n->atomicNext.get(); // inconsistent read
						if (n != b->atomicNext.get())
						{
							break;
						}
						void *v = n->atomicValue.get();
						if (v == NULL)
						{ // n is deleted
							n->helpDelete(map, b, f, nodePool);
							deleteNode(n);
							break;
						}
						if (v == n || b->atomicValue.get() == NULL) // b is deleted
						{
							break;
						}
						b = n;
						n = f;
					}
					q = atomicHead.get(); // restart
				}
			}
		}

		/**
		 * Specialized variant of findPredecessor to get predecessor of last
		 * valid node.  Needed when removing the last entry.  It is possible
		 * that all successors of returned node will have been deleted upon
		 * return, in which case this method can be retried.
		 * @return likely predecessor of last node
		 */
	private:
		Node<K, V> *findPredecessorOfLast()
		{
			for (;;)
			{
				Index<K, V> *q = atomicHead.get();
				for (;;)
				{
					Index<K, V> *d, *r;
					if ((r = q->atomicRight.get()) != NULL)
					{
						if (r->indexesDeletedNode())
						{
							q->unlink(map, nodePool, r);
							break; // must restart
						}
						// proceed as far across as possible without overshooting
						if (r->node->atomicNext.get() != NULL)
						{
							q = r;
							continue;
						}
					}
					if ((d = q->down) != NULL)
					{
						q = d;
					}
					else
					{
						return q->node;
					}
				}
			}
		}

		/**
		 * Removes last entry; returns its snapshot.
		 * Specialized variant of doRemove.
		 * @return null if empty, else snapshot of last entry
		 */
	public:
		virtual typename Map<K,V>:: template Entry<K,V> *doRemoveLastEntry()
		{
			for (;;)
			{
				Node<K, V> *b = findPredecessorOfLast();
				Node<K, V> *n = b->atomicNext.get();
				if (n == NULL)
				{
					if (b->isBaseHeader()) // empty
					{
						return NULL;
					}
					else
					{
						continue; // all b's successors are deleted; retry
					}
				}
				for (;;)
				{
					Node<K, V> *f = n->atomicNext.get();
					if (n != b->atomicNext.get()) // inconsistent read
					{
						break;
					}
					void *v = n->atomicValue.get();
					if (v == NULL)
					{ // n is deleted
						n->helpDelete(map, b, f, nodePool);
						deleteNode(n);
						break;
					}
					if (v == n || b->atomicValue.get() == NULL) // b is deleted
					{
						break;
					}
					if (f != NULL)
					{
						b = n;
						n = f;
						continue;
					}
					void * prev = v;
					if (!n->casValue(prev, NULL))
					{
						break;
					}
					valuePool->free((V)prev);
					K key = n->key;
					Comparable<K> *ck = comparable(key);

					//Node<K, V> *z = (Node<K, V>*)nodePool->allocate();
					//z->init(f);

					bool appended = n->appendMarker(map, nodePool, f);
					if (b == f) {
						printf("bogus\n");
					}
					if (f != 0) {
						f->addRef();
					}
					if (!appended || !b->casNext(n, f))
					{
						//if (!appended) {
						//	pushNodeDelete(map, z);
						//}
						findNode(ck); // Retry via findNode
						if (f != 0) {
							f->deleteRef(map, nodePool);
						}
					}
					else
					{
						n->deleteRef(map, nodePool);
						//pushNodeDelete(map, n);
						findPredecessor(ck); // Clean index
						if (atomicHead.get()->atomicRight.get() == NULL)
						{
							tryReduceLevel();
						}
					}
					return new typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>(key, static_cast<V>(v));
				}
			}
		}

		/* ---------------- Relational operations -------------- */

		// Control values OR'ed as arguments to findNear

	private:
		static constexpr int EQ = 1;
		static constexpr int LT = 2;
		static constexpr int GT = 0; // Actually checked as !LT

		/**
		 * Utility for ceiling, floor, lower, higher methods.
		 * @param kkey the key
		 * @param rel the relation -- OR'ed combination of EQ, LT, GT
		 * @return nearest node fitting relation, or null if no such
		 */
	public:
		virtual Node<K, V> *findNear(K kkey, int rel)
		{
			Comparable<K> *key = comparable(kkey);
			for (;;)
			{
				Node<K, V> *b = findPredecessor(key);
				Node<K, V> *n = b->atomicNext.get();//next;
				for (;;)
				{
					if (n == NULL)
					{
						return ((rel & LT) == 0 || b->isBaseHeader())? NULL : b;
					}
					Node<K, V> *f = n->atomicNext.get();//next;
					if (n != b->atomicNext.get()/*next*/) // inconsistent read
					{
						break;
					}
					void *v = n->atomicValue.get();//value;
					if (v == NULL)
					{ // n is deleted
						n->helpDelete(map, b, f, nodePool);
						deleteNode(n);
						break;
					}
					if (v == n || b->atomicValue.get()/*value*/ == NULL) // b is deleted
					{
						break;
					}
					int c = key->compareTo(n->key);
					if ((c == 0 && (rel & EQ) != 0) || (c < 0 && (rel & LT) == 0))
					{
						return n;
					}
					if (c <= 0 && (rel & LT) != 0)
					{
						return (b->isBaseHeader())? NULL : b;
					}
					b = n;
					n = f;
				}
			}
		}

		/**
		 * Returns SimpleImmutableEntry for results of findNear.
		 * @param key the key
		 * @param rel the relation -- OR'ed combination of EQ, LT, GT
		 * @return Entry fitting relation, or null if no such
		 */
		virtual typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V> *getNear(K key, int rel)
		{
			for (;;)
			{
				Node<K, V> *n = findNear(key, rel);
				if (n == NULL)
				{
					return NULL;
				}
				typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V> *e = n->createSnapshot();
				if (e != NULL)
				{
					return e;
				}
			}
		}


		/* ---------------- Constructors -------------- */

		/**
		 * Constructs a new, empty map, sorted according to the
		 * {@linkplain Comparable natural ordering} of the keys.
		 */
		ConcurrentSkipListMap(void *map, PooledObjectPool<Key*> *keyPool, PooledObjectPool<KeyImpl*> *keyImplPool,
			PooledObjectPool<MyValue*> *valuePool, int *dataTypes) : comparatorField(NULL)
		{
			this->map = map;
			this->keyPool = keyPool;
			this->keyImplPool = keyImplPool;
			this->valuePool = valuePool;
			this->dataTypes = dataTypes;
			initialize();
		}

		/**
		 * Constructs a new, empty map, sorted according to the specified
		 * comparatorField.
		 *
		 * @param comparator the comparatorField that will be used to order this map.
		 *        If <tt>null</tt>, the {@linkplain Comparable natural
		 *        ordering} of the keys will be used.
		 */
		ConcurrentSkipListMap(Comparator<K> *comparator) : comparatorField(comparator)
		{
			initialize();
		}

		/**
		 * Constructs a new map containing the same mappings as the given map,
		 * sorted according to the {@linkplain Comparable natural ordering} of
		 * the keys.
		 *
		 * @param  m the map whose mappings are to be placed in this map
		 * @throws ClassCastException if the keys in <tt>m</tt> are not
		 *         {@link Comparable}, or are not mutually comparable
		 * @throws NullPointerException if the specified map or any of its keys
		 *         or values are null
		 */
		ConcurrentSkipListMap(Map<K, V> *m) : comparatorField(NULL)
		{
			initialize();
			putAll(m);
		}

		/**
		 * Constructs a new map containing the same mappings and using the
		 * same ordering as the specified sorted map.
		 *
		 * @param m the sorted map whose mappings are to be placed in this
		 *        map, and whose comparatorField is to be used to sort this map
		 * @throws NullPointerException if the specified sorted map or any of
		 *         its keys or values are null
		 */
		ConcurrentSkipListMap(SortedMap<K, V> *m) : comparatorField(m->comparator())
		{
			initialize();
			buildFromSorted(m);
		}

		/**
		 * Streamlined bulk insertion to initialize from elements of
		 * given sorted map.  Call only from constructor or clone
		 * method.
		 */
	private:
		void buildFromSorted(SortedMap<K, V> *map)
		{
			if (map == NULL)
			{
				throw NullPointerException();
			}

			HeadIndex<K, V> *h = atomicHead.get();
			Node<K, V> *basepred = h->node;

			// Track the current rightmost node at each level. Uses an
			// skiplist.ArrayList to avoid committing to initial or maximum level.
			ArrayList<Index<K, V>*> *preds = new ArrayList<Index<K, V>*>();

			// initialize
			for (int i = 0; i <= h->level; ++i)
			{
				preds->add(NULL);
			}
			Index<K, V> *q = h;
			for (int i = h->level; i > 0; --i)
			{
				preds->set(i, q);
				q = q->down;
			}

			Iterator<typename Map<K,V>::template Entry<K, V>*> *it = map->entrySet()->beginIterator();
			while (it->hasNext())
			{
				typename Map<K,V>:: template Entry<K,V> *e = it->nextEntry();
				int j = randomLevel();
				if (j > h->level)
				{
					j = h->level + 1;
				}
				K k = e->getKey();
				V v = e->getValue();
				if (k == NULL || v == NULL)
				{
					throw NullPointerException();
				}
				Node<K, V> *z = new Node<K, V>(k, v, NULL);
				basepred->next = z;
				z->addRef();
				basepred->atomicNext.set(z);
				basepred = z;
				if (j > 0)
				{
					Index<K, V> *idx = NULL;
					for (int i = 1; i <= j; ++i)
					{
						Index<K, V> *prev = idx;
						idx = indexPool->allocate();
						idx->init(z, prev, NULL);// new Index<K, V>(z, idx, NULL);
						if (i > h->level)
						{
							h = new HeadIndex<K, V>(h->node, h, idx, i);
						}

						if (i < preds->size())
						{
							preds->get(i)->right.set(idx);
							preds->set(i, idx);
						}
						else
						{
							preds->add(idx);
						}
					}
				}
				it++;
			}
			atomicHead.set(h);
		}

		/* ------ skiplist.Map API methods ------ */

		/**
		 * Returns <tt>true</tt> if this map contains a mapping for the specified
		 * key.
		 *
		 * @param key key whose presence in this map is to be tested
		 * @return <tt>true</tt> if this map contains a mapping for the specified key
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key is null
		 */
	public:
		virtual bool containsKey(K key)
		{
			return doGet(key) != NULL;
		}

		/**
		 * Returns the value to which the specified key is mapped,
		 * or {@code null} if this map contains no mapping for the key.
		 *
		 * <p>More formally, if this map contains a mapping from a key
		 * {@code k} to a value {@code v} such that {@code key} compares
		 * equal to {@code k} according to the map's ordering, then this
		 * method returns {@code v}; otherwise it returns {@code null}.
		 * (There can be at most one such mapping.)
		 *
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key is null
		 */
		virtual V get(K key)
		{
			return doGet(key);
		}

		/**
		 * Associates the specified value with the specified key in this map.
		 * If the map previously contained a mapping for the key, the old
		 * value is replaced.
		 *
		 * @param key key with which the specified value is to be associated
		 * @param value value to be associated with the specified key
		 * @return the previous value associated with the specified key, or
		 *         <tt>null</tt> if there was no mapping for the key
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key or value is null
		 */
		virtual V put(K key, V value)
		{
			if (value == NULL)
			{
				throw NullPointerException();
			}
			return doPut(key, value, false);
		}

		/**
		 * Removes the mapping for the specified key from this map if present.
		 *
		 * @param  key key for which mapping should be removed
		 * @return the previous value associated with the specified key, or
		 *         <tt>null</tt> if there was no mapping for the key
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key is null
		 */
		virtual V remove(void *key)
		{
			return doRemove(key, NULL);
		}

		/**
		 * Returns <tt>true</tt> if this map maps one or more keys to the
		 * specified value.  This operation requires time linear in the
		 * map size.
		 *
		 * @param value value whose presence in this map is to be tested
		 * @return <tt>true</tt> if a mapping to <tt>value</tt> exists;
		 *         <tt>false</tt> otherwise
		 * @throws NullPointerException if the specified value is null
		 */
		virtual bool containsValue(void *value)
		{
			if (value == NULL)
			{
				throw NullPointerException();
			}
			for (Node<K, V> *n = findFirst(); n != NULL; n = n->atomicNext.get())
			{
				V v = n->getValidValue();
				if (v != NULL && ((V)value)->equals(v))
				{
					return true;
				}
			}
			return false;
		}

		/**
		 * Returns the number of key-value mappings in this map.  If this map
		 * contains more than <tt>Integer.MAX_VALUE</tt> elements, it
		 * returns <tt>Integer.MAX_VALUE</tt>.
		 *
		 * <p>Beware that, unlike in most collections, this method is
		 * <em>NOT</em> a constant-time operation. Because of the
		 * asynchronous nature of these maps, determining the current
		 * number of elements requires traversing them all to count them.
		 * Additionally, it is possible for the size to change during
		 * execution of this method, in which case the returned result
		 * will be inaccurate. Thus, this method is typically not very
		 * useful in concurrent applications.
		 *
		 * @return the number of elements in this map
		 */
		virtual int size()
		{
			long long count = 0;
			for (Node<K, V> *n = findFirst(); n != NULL; n = n->atomicNext.get())
			{
				if (n->getValidValue() != NULL)
				{
					++count;
				}
			}
			return static_cast<int>(count);
		}

		/**
		 * Returns <tt>true</tt> if this map contains no key-value mappings.
		 * @return <tt>true</tt> if this map contains no key-value mappings
		 */
		virtual bool isEmpty()
		{
			return findFirst() == NULL;
		}

		/**
		 * Removes all of the mappings from this map.
		 */
		virtual void clear()
		{
			initialize();
		}

		/* ---------------- View methods -------------- */

		/*
		 * Note: Lazy initialization works for views because view classes
		 * are stateless/immutable so it doesn't matter wrt correctness if
		 * more than one is created (which will only rarely happen).  Even
		 * so, the following idiom conservatively ensures that the method
		 * returns the one it created if it does so, not one created by
		 * another racing thread.
		 */

		/**
		 * Returns a {@link NavigableSet} view of the keys contained in this map.
		 * The set's beginIterator returns the keys in ascending order.
		 * The set is backed by the map, so changes to the map are
		 * reflected in the set, and vice-versa.  The set supports element
		 * removal, which removes the corresponding mapping from the map,
		 * via the {@code skiplist.Iterator.remove}, {@code skiplist.Set.remove},
		 * {@code removeAll}, {@code retainAll}, and {@code clear}
		 * operations.  It does not support the {@code add} or {@code addAll}
		 * operations.
		 *
		 * <p>The view's {@code beginIterator} is a "weakly consistent" beginIterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the beginIterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 *
		 * <p>This method is equivalent to method {@code navigableKeySet}.
		 *
		 * @return a navigable set view of the keys in this map
		 */
		 virtual Set<K> *keySet()
		 {
			KeySet<K> *ks = (KeySet<K>*)keySet_Conflict;
 			return (ks != NULL) ? (Set<K>*)(AbstractSet<K>*)(typename ConcurrentSkipListMap<K,V>::template KeySet<K>*)ks : 
				(Set<K>*)(AbstractSet<K>*)(typename ConcurrentSkipListMap<K,V>::template KeySet<K>*)(keySet_Conflict = new ConcurrentSkipListMap::KeySet<K>(this));
		 }

		virtual NavigableSet<K> *navigableKeySet()
		{
			KeySet<K> *ks = (KeySet<K>*)keySet_Conflict;
			return (ks != NULL) ? (KeySet<K>*)ks : (keySet_Conflict = new KeySet<K>(this));
		}


		/**
		 * Returns a {@link Collection} view of the values contained in this map.
		 * The collection's beginIterator returns the values in ascending order
		 * of the corresponding keys.
		 * The collection is backed by the map, so changes to the map are
		 * reflected in the collection, and vice-versa.  The collection
		 * supports element removal, which removes the corresponding
		 * mapping from the map, via the <tt>skiplist.Iterator.remove</tt>,
		 * <tt>skiplist.Collection.remove</tt>, <tt>removeAll</tt>,
		 * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
		 * support the <tt>add</tt> or <tt>addAll</tt> operations.
		 *
		 * <p>The view's <tt>beginIterator</tt> is a "weakly consistent" beginIterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the beginIterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 */
		virtual Collection<V> *values()
		{
			Values<V> *vs = (Values<V>*)values_Conflict;
			return (vs != NULL) ? (Values<V>*)vs : 
				(values_Conflict = new Values<V>(this));
		}

		/**
		 * Returns a {@link Set} view of the mappings contained in this map.
		 * The set's beginIterator returns the entries in ascending key order.
		 * The set is backed by the map, so changes to the map are
		 * reflected in the set, and vice-versa.  The set supports element
		 * removal, which removes the corresponding mapping from the map,
		 * via the <tt>skiplist.Iterator.remove</tt>, <tt>skiplist.Set.remove</tt>,
		 * <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt>
		 * operations.  It does not support the <tt>add</tt> or
		 * <tt>addAll</tt> operations.
		 *
		 * <p>The view's <tt>beginIterator</tt> is a "weakly consistent" beginIterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the beginIterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 *
		 * <p>The <tt>skiplist.Map.Entry</tt> elements returned by
		 * <tt>beginIterator.next()</tt> do <em>not</em> support the
		 * <tt>setValue</tt> operation.
		 *
		 * @return a set view of the mappings contained in this map,
		 *         sorted in ascending key order
		 */
		virtual Set<typename Map<K,V>::template Entry<K, V>*> *entrySet()
		{
			ConcurrentSkipListMap::EntrySet<K, V> *es = (ConcurrentSkipListMap::EntrySet<K, V>*) entrySet_Conflict;
			return (es != NULL) ? (skiplist::Set<typename Map<K,V>::template Entry<K, V>*> *)es :
				(skiplist::Set<typename Map<K,V>::template Entry<K, V>*> *)
				(entrySet_Conflict = new ConcurrentSkipListMap::EntrySet<K, V>(this));
		}

		virtual ConcurrentNavigableMap<K, V> *descendingMap()
		{
			ConcurrentNavigableMap<K, V> *dm = descendingMap_Conflict;
			return (dm != NULL) ? dm : (descendingMap_Conflict = new SubMap<K, V>(this, NULL, false, NULL, false, true));
		}

		virtual NavigableSet<K> *descendingKeySet()
		{
			return descendingMap()->navigableKeySet();
		}

		/* ---------------- skiplist.AbstractMap Overrides -------------- */

		/* ------ skiplist.ConcurrentMap API methods ------ */

		/**
		 * {@inheritDoc}
		 *
		 * @return the previous value associated with the specified key,
		 *         or <tt>null</tt> if there was no mapping for the key
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key or value is null
		 */
		virtual V putIfAbsent(K key, V value)
		{
			if (value == NULL)
			{
				throw NullPointerException();
			}
			return doPut(key, value, true);
		}

		/**
		 * {@inheritDoc}
		 *
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key is null
		 */
		virtual bool remove(void *key, void *value)
		{
			if (key == NULL)
			{
				throw NullPointerException();
			}
			if (value == NULL)
			{
				return false;
			}
			return doRemove(key, value) != NULL;
		}

		/**
		 * {@inheritDoc}
		 *
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if any of the arguments are null
		 */
		virtual bool replace(K key, V oldValue, V newValue)
		{
			if (oldValue == NULL || newValue == NULL)
			{
				throw NullPointerException();
			}
			Comparable<K> *k = comparable(key);
			for (;;)
			{
				Node<K, V> *n = findNode(k);
				if (n == NULL)
				{
					return false;
				}
				void *v = n->atomicValue.get();
				if (v != NULL)
				{
					if (!oldValue->equals(v))
					{
						return false;
					}
					void *prev = v;
					if (n->casValue(prev, newValue))
					{
						return true;
					}
					valuePool->free((V)prev);
				}
			}
		}

		/**
		 * {@inheritDoc}
		 *
		 * @return the previous value associated with the specified key,
		 *         or <tt>null</tt> if there was no mapping for the key
		 * @throws ClassCastException if the specified key cannot be compared
		 *         with the keys currently in the map
		 * @throws NullPointerException if the specified key or value is null
		 */
		virtual V replace(K key, V value)
		{
			if (value == NULL)
			{
				throw NullPointerException();
			}
			Comparable<K> *k = comparable(key);
			for (;;)
			{
				Node<K, V> *n = findNode(k);
				if (n == NULL)
				{
					return NULL;
				}
				void *v = n->atomicValue.get();
				if (v != NULL && n->casValue(v, value))
				{
					return (V)v;
				}
			}
		}

		/* ------ skiplist.SortedMap API methods ------ */

		virtual Comparator<K> *comparator()
		{
			return comparatorField;
		}

		virtual K firstKey()
		{
			Node<K, V> *n = findFirst();
			if (n == NULL)
			{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
				throw std::runtime_error("error");
			}
			return n->key;
		}

		virtual K lastKey()
		{
			Node<K, V> *n = findLast();
			if (n == NULL)
			{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
				throw std::runtime_error("error");
			}
			return n->key;
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code fromKey} or {@code toKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *subMap(K fromKey, bool fromInclusive, K toKey, bool toInclusive)
		{
			if (fromKey == NULL || toKey == NULL)
			{
				throw NullPointerException();
			}
			return new SubMap<K, V> (this, fromKey, fromInclusive, toKey, toInclusive, false);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code toKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *headMap(K toKey, bool inclusive)
		{
			if (toKey == NULL)
			{
				throw NullPointerException();
			}
			return new SubMap<K, V> (this, NULL, false, toKey, inclusive, false);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code fromKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *tailMap(K fromKey, bool inclusive)
		{
			if (fromKey == NULL)
			{
				throw NullPointerException();
			}
			return new SubMap<K, V> (this, fromKey, inclusive, NULL, false, false);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code fromKey} or {@code toKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *subMap(K fromKey, K toKey)
		{
			return subMap(fromKey, true, toKey, false);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code toKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *headMap(K toKey)
		{
			return headMap(toKey, false);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if {@code fromKey} is null
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *tailMap(K fromKey)
		{
			return tailMap(fromKey, true);
		}

		/* ---------------- Relational operations -------------- */

		/**
		 * Returns a key-value mapping associated with the greatest key
		 * strictly less than the given key, or <tt>null</tt> if there is
		 * no such key. The returned entry does <em>not</em> support the
		 * <tt>Entry.setValue</tt> method.
		 *
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *lowerEntry(K key)
		{
			return getNear(key, LT);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual K lowerKey(K key)
		{
			Node<K, V> *n = findNear(key, LT);
			return (n == NULL)? NULL : n->key;
		}

		/**
		 * Returns a key-value mapping associated with the greatest key
		 * less than or equal to the given key, or <tt>null</tt> if there
		 * is no such key. The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 *
		 * @param key the key
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *floorEntry(K key)
		{
			return getNear(key, LT | EQ);
		}

		/**
		 * @param key the key
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual K floorKey(K key)
		{
			Node<K, V> *n = findNear(key, LT | EQ);
			return (n == NULL)? NULL : n->key;
		}

		/**
		 * Returns a key-value mapping associated with the least key
		 * greater than or equal to the given key, or <tt>null</tt> if
		 * there is no such entry. The returned entry does <em>not</em>
		 * support the <tt>Entry.setValue</tt> method.
		 *
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *ceilingEntry(K key)
		{
			return getNear(key, GT | EQ);
		}

		/**
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual K ceilingKey(K key)
		{
			Node<K, V> *n = findNear(key, GT | EQ);
			return (n == NULL)? NULL : n->key;
		}

		/**
		 * Returns a key-value mapping associated with the least key
		 * strictly greater than the given key, or <tt>null</tt> if there
		 * is no such key. The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 *
		 * @param key the key
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *higherEntry(K key)
		{
			return getNear(key, GT);
		}

		/**
		 * @param key the key
		 * @throws ClassCastException {@inheritDoc}
		 * @throws NullPointerException if the specified key is null
		 */
		virtual K higherKey(K key)
		{
			Node<K, V> *n = findNear(key, GT);
			return (n == NULL)? NULL : n->key;
		}

		/**
		 * Returns a key-value mapping associated with the least
		 * key in this map, or <tt>null</tt> if the map is empty.
		 * The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *firstEntry()
		{
			for (;;)
			{
				Node<K, V> *n = findFirst();
				if (n == NULL)
				{
					return NULL;
				}
				typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V> *e = n->createSnapshot();
				if (e != NULL)
				{
					return e;
				}
			}
		}

		/**
		 * Returns a key-value mapping associated with the greatest
		 * key in this map, or <tt>null</tt> if the map is empty.
		 * The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *lastEntry()
		{
			for (;;)
			{
				Node<K, V> *n = findLast();
				if (n == NULL)
				{
					return NULL;
				}
				typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V> *e = n->createSnapshot();
				if (e != NULL)
				{
					return e;
				}
			}
		}

		/**
		 * Removes and returns a key-value mapping associated with
		 * the least key in this map, or <tt>null</tt> if the map is empty.
		 * The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *pollFirstEntry()
		{
			return doRemoveFirstEntry();
		}

		/**
		 * Removes and returns a key-value mapping associated with
		 * the greatest key in this map, or <tt>null</tt> if the map is empty.
		 * The returned entry does <em>not</em> support
		 * the <tt>Entry.setValue</tt> method.
		 */
		virtual typename Map<K,V>:: template Entry<K,V> *pollLastEntry()
		{
			return doRemoveLastEntry();
		}


		/* ---------------- Iterators -------------- */

		/**
		 * Base of beginIterator classes:
		 */
	public:
		template<typename T>
		class Iter : public Iterator<T>
		{
		private:
			ConcurrentSkipListMap<K, V> *outerInstance;

			/** the last node returned by next() */
		public:
			Node<K, V> *lastReturned;
			/** the next node to return from next(); */
			Node<K, V> *next;
			/** Cache of next value field to maintain weak consistency */
			V nextValue;

			/** Initializes ascending beginIterator for entire range. */
			Iter(ConcurrentSkipListMap<K, V> *outerInstance) : outerInstance(outerInstance)
			{
				for (;;)
				{
					next = outerInstance->findFirst();
					if (next == NULL)
					{
						break;
					}
					void *x = next->atomicValue.get();
					if (x != NULL && x != next)
					{
						nextValue = static_cast<V>(x);
						break;
					}
				}
			}

			virtual bool hasNext()
			{
				return next != NULL;
			}

			/** Advances next to higher entry. */
			virtual void advance()
			{
				if (next == NULL)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
				lastReturned = next;
				for (;;)
				{
					next = next->atomicNext.get();
					if (next == NULL)
					{
						break;
					}
					void *x = next->atomicValue.get();
					if (x != NULL && x != next)
					{
						nextValue = static_cast<V>(x);
						break;
					}
				}
			}

			virtual void remove()
			{
				Node<K, V> *l = lastReturned;
				if (l == NULL)
				{
					throw IllegalStateException();
				}
				// It would not be worth all of the overhead to directly
				// unlink from here. Using remove is fast enough.
				outerInstance->remove(l->key);
				lastReturned = NULL;
			}

		};

	public:
		class ValueIterator : public Iter<V>
		{
		private:
			ConcurrentSkipListMap<K, V> *outerInstance;

		public:
			ValueIterator(ConcurrentSkipListMap<K, V> *outerInstance) : Iter<V>(outerInstance), outerInstance(outerInstance)
			{
			}

			virtual V nextEntry(V entry) {
				return NULL;
			}

			V nextEntry()
			{
				V v = Iter<V>::nextValue;
				Iter<V>::advance();
				return v;
			}
		};

	public:
		class KeyIterator : public Iter<K>
		{
		private:
			ConcurrentSkipListMap<K, V> *outerInstance;

		public:
			KeyIterator(ConcurrentSkipListMap<K, V> *outerInstance) : Iter<K>(outerInstance), outerInstance(outerInstance)
			{
			}

			virtual K nextEntry(K entry) {
				return NULL;
			}
			K nextEntry()
			{
				Node<K, V> *n = Iter<K>::next;
				Iter<K>::advance();
				return n->key;
			}
		};

	public:
            typename Map<K,V>:: template Entry<K,V> *allocateEntry() {
                return new typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>();
            }
		class EntryIterator : public Iter<typename Map<K,V>:: template Entry<K,V>*>
		{
		private:
			ConcurrentSkipListMap<K, V> *outerInstance;

		public:
			EntryIterator(ConcurrentSkipListMap<K, V> *outerInstance) : Iter<typename Map<K,V>:: template Entry<K,V>*>(outerInstance), outerInstance(outerInstance)
			{
			}

			typename Map<K,V>:: template Entry<K,V> *nextEntry(typename Map<K,V>:: template Entry<K,V> *entry)
                        {
				Node<K, V> *n = Iter<typename Map<K,V>:: template Entry<K,V>*>::next;
				V v = Iter<typename Map<K,V>:: template Entry<K,V>*>::nextValue;
				Iter<typename Map<K,V>:: template Entry<K,V>*>::advance();
                                ((typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>*)entry)->setKey(n->key);
                                entry->setValue(v);
				return entry;
                        }
                
			typename Map<K,V>:: template Entry<K,V> *nextEntry()
			{
				Node<K, V> *n = Iter<typename Map<K,V>:: template Entry<K,V>*>::next;
				V v = Iter<typename Map<K,V>:: template Entry<K,V>*>::nextValue;
				Iter<typename Map<K,V>:: template Entry<K,V>*>::advance();
				return new typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>(n->key, v);
			}
		};

		// Factory methods for iterators needed by ConcurrentSkipListSet etc

	public:
		virtual Iterator<K> *keyIterator()
		{
			return new KeyIterator(this);
		}

		virtual Iterator<V> *valueIterator()
		{
			return new ValueIterator(this);
		}

		virtual Iterator<typename Map<K,V>:: template Entry<K,V>*> *entryIterator()
		{
			return new EntryIterator(this);
		}

	public:
		template<typename E>
		class KeySet : public AbstractSet<E>, public NavigableSet<E>
		{
		private:
			ConcurrentNavigableMap<E, V> *const m;
		public:
			KeySet(NavigableMap<E, V> *map) : m(static_cast<ConcurrentNavigableMap<E, V>*>(map))
			{
			}
			virtual int size()
			{
				return ((AbstractMap<E,V>*)m)->size();
			}
			virtual bool isEmpty()
			{
				return ((AbstractMap<E,V>*)m)->isEmpty();
			}
			virtual bool contains(void *o)
			{
				return ((AbstractMap<E,V>*)m)->containsKey(o);
			}
			virtual bool remove(void *o)
			{
				return ((AbstractMap<E,V>*)m)->remove(o) != NULL;
			}
			virtual void clear()
			{
				((AbstractMap<E,V>*)m)->clear();
			}
			E lower(E e)
			{
				return m->lowerKey(e);
			}
			E floor(E e)
			{
				return m->floorKey(e);
			}
			E ceiling(E e)
			{
				return m->ceilingKey(e);
			}
			E higher(E e)
			{
				return m->higherKey(e);
			}
			Comparator<E> *comparator()
			{
				return m->comparator();
			}
			E first()
			{
				return m->firstKey();
			}
			E last()
			{
				return m->lastKey();
			}
			E pollFirst()
			{
				typename Map<E,V>::template Entry<E, V> *e = m->pollFirstEntry();
				return e == NULL? NULL : e->getKey();
			}
			E pollLast()
			{
				typename Map<E,V>::template Entry<E, V> *e = m->pollLastEntry();
				return e == NULL? NULL : e->getKey();
			}
			virtual Iterator<E> *beginIterator()
			{
				if (dynamic_cast<ConcurrentSkipListMap*>(m) != NULL)
				{
					return (static_cast<ConcurrentSkipListMap<E, V>*>(m))->keyIterator();
				}
				else
				{
					return (static_cast<ConcurrentSkipListMap::SubMap<E, V>*>(m))->keyIterator();
				}
			}
			virtual bool equals(void *o)
			{
				if (o == this)
				{
					return true;
				}
				if (!(static_cast<Set<E>*>(o) != NULL))
				{
					return false;
				}
				Collection<E> *c = static_cast<Collection<E>*>(o);
				try
				{
					return AbstractCollection<E>::containsAll(c) && c->containsAll((AbstractCollection<E>*)(AbstractSet<E>*)this);
				}
				catch (const ClassCastException &unused)
				{
					return false;
				}
				catch (const NullPointerException &unused)
				{
					return false;
				}
			}
			Iterator<E> *descendingIterator()
			{
				return descendingSet()->beginIterator();
			}
			NavigableSet<E> *subSet(E fromElement, bool fromInclusive, E toElement, bool toInclusive)
			{
				return new KeySet<E>((NavigableMap<K, V>*)m->subMap(fromElement, fromInclusive, toElement, toInclusive));
			}
			NavigableSet<E> *headSet(E toElement, bool inclusive)
			{
				return new KeySet<E>((NavigableMap<K, V>*)m->headMap(toElement, inclusive));
			}
			NavigableSet<E> *tailSet(E fromElement, bool inclusive)
			{
				return new KeySet<E>((NavigableMap<K, V>*)m->tailMap(fromElement, inclusive));
			}
			NavigableSet<E> *subSet(E fromElement, E toElement)
			{
				return subSet(fromElement, true, toElement, false);
			}
			NavigableSet<E> *headSet(E toElement)
			{
				return headSet(toElement, false);
			}
			NavigableSet<E> *tailSet(E fromElement)
			{
				return tailSet(fromElement, true);
			}
			NavigableSet<E> *descendingSet()
			{
				return new KeySet((NavigableMap<K, V>*)m->descendingMap());
			}
		};

	public:
		template<typename E>
		class Values : public AbstractCollection<E>
		{
		private:
			ConcurrentNavigableMap<K, E> *const m;
		public:
			Values(ConcurrentNavigableMap<K, E> *map) : m(map)
			{
			}
			virtual Iterator<E> *beginIterator()
			{
				if (dynamic_cast<ConcurrentSkipListMap*>(m) != NULL)
				{
					return (static_cast<ConcurrentSkipListMap<K, E>*>(m))->valueIterator();
				}
				else
				{
					return (static_cast<SubMap<K, E>*>(m))->valueIterator();
				}
			}
			virtual bool isEmpty()
			{
				return ((AbstractMap<K,E>*)m)->isEmpty();
			}
			virtual int size()
			{
				return ((AbstractMap<K,E>*)m)->size();
			}
			virtual bool contains(void *o)
			{
				return ((AbstractMap<K,E>*)m)->containsValue(o);
			}
			virtual void clear()
			{
				((AbstractMap<K,E>*)m)->clear();
			}
		};

	public:
		template<typename K1, typename V1>
		class EntrySet : public AbstractSet <typename Map<K1,V1>::template Entry<K1, V1>*> 
		{
		private:
			ConcurrentNavigableMap<K1, V1> *const m;
		public:
			EntrySet(ConcurrentNavigableMap<K1, V1> *map) : m(map)
			{
			}

			virtual Iterator<typename Map<K1,V1>::template Entry<K1, V1>*> *beginIterator()
			{
				if (dynamic_cast<ConcurrentSkipListMap*>(m) != NULL)
				{
					return (static_cast<ConcurrentSkipListMap<K1, V1>*>(m))->entryIterator();
				}
				else
				{
					return (static_cast<SubMap<K1, V1>*>(m))->entryIterator();
				}
			}

			virtual bool contains(void *o)
			{
				if (!(static_cast<typename Map<K1,V1>::template Entry<K1, V1>*>(o) != NULL))
				{
					return false;
				}
				typename Map<K1,V1>::template Entry<K1, V1> *e = static_cast<typename Map<K1,V1>::template Entry<K1, V1>*>(o);
				V1 v = ((AbstractMap<K1,V1>*)m)->get(e->getKey());
				return v != NULL && v->equals(e->getValue());
			}
			virtual bool remove(void *o)
			{
				if (!(static_cast<typename Map<K1,V1>::template Entry<K1, V1>*>(o) != NULL))
				{
					return false;
				}
				typename Map<K1,V1>::template Entry<K1, V1> *e = static_cast<typename Map<K1,V1>::template Entry<K1, V1>*>(o);
				return ((AbstractMap<K1,V1>*)m)->remove(e->getKey());
			}
			virtual bool isEmpty()
			{
				return ((AbstractMap<K1,V1>*)m)->isEmpty();
			}
			virtual int size()
			{
				return ((AbstractMap<K1,V1>*)m)->size();
			}
			virtual void clear()
			{
				((AbstractMap<K1,V1>*)m)->clear();
			}
			virtual bool equals(void *o)
			{
				if (o == this)
				{
					return true;
				}
				if (!(static_cast<Set<typename Map<K1,V1>::template Entry<K1, V1>*>*>(o) != NULL))
				{
					return false;
				}
				Collection<typename Map<K1,V1>::template Entry<K1, V1>*> *c = static_cast<Collection<typename Map<K1,V1>::template Entry<K1, V1>*>*>(o);
				try
				{
					return AbstractCollection<typename Map<K1,V1>::template Entry<K1, V1>*>::containsAll(c) && c->containsAll((Collection<typename Map<K1,V1>::template Entry<K1, V1>*>*)(AbstractCollection<typename Map<K1,V1>::template Entry<K1, V1>*>*)(AbstractSet<typename Map<K1,V1>::template Entry<K1, V1>*>*)this);
				}
				catch (const ClassCastException &unused)
				{
					return false;
				}
				catch (const NullPointerException &unused)
				{
					return false;
				}
			}
		};

		/**
		 * Submaps returned by {@link ConcurrentSkipListMap} submap operations
		 * represent a subrange of mappings of their underlying
		 * maps. Instances of this class support all methods of their
		 * underlying maps, differing in that mappings outside their range are
		 * ignored, and attempts to add mappings outside their ranges result
		 * in {@link IllegalArgumentException}.  Instances of this class are
		 * constructed only using the <tt>subMap</tt>, <tt>headMap</tt>, and
		 * <tt>tailMap</tt> methods of their underlying maps.
		 *
		 * @serial include
		 */
	public:
		template<typename K1, typename V1>
		class SubMap : public AbstractMap<K1, V1>, public ConcurrentNavigableMap<K1, V1>
		{
		private:
			static constexpr long long serialVersionUID = -7647078645895051609LL;

			/** Underlying map */
			ConcurrentSkipListMap<K1, V1> *const m;
			/** lower bound key, or null if from start */
			const K1 lo;
			/** upper bound key, or null if to end */
			const K1 hi;
			/** inclusion flag for lo */
			const bool loInclusive;
			/** inclusion flag for hi */
			const bool hiInclusive;
			/** direction */
			const bool isDescending;

			// Lazily initialized view holders
			KeySet<K1> *keySetView;
			Set<typename Map<K1,V1>::template Entry<K1, V1>*> *entrySetView;
			Collection<V1> *valuesView;

			/**
			 * Creates a new submap, initializing all fields
			 */
		public:
			SubMap(ConcurrentSkipListMap<K1, V1> *map, K1 fromKey, bool fromInclusive, K1 toKey, bool toInclusive, bool isDescending) : m(map), lo(fromKey), hi(toKey), loInclusive(fromInclusive), hiInclusive(toInclusive), isDescending(isDescending)
			{
				if (fromKey != NULL && toKey != NULL && map->compare(fromKey, toKey) > 0)
				{
					throw std::invalid_argument("inconsistent range");
				}
			}

			/* ----------------  Utilities -------------- */

		private:
			bool tooLow(K1 key)
			{
				if (lo != NULL)
				{
					int c = m->compare(key, lo);
					if (c < 0 || (c == 0 && !loInclusive))
					{
						return true;
					}
				}
				return false;
			}

			bool tooHigh(K1 key)
			{
				if (hi != NULL)
				{
					int c = m->compare(key, hi);
					if (c > 0 || (c == 0 && !hiInclusive))
					{
						return true;
					}
				}
				return false;
			}

			bool inBounds(K1 key)
			{
				return !tooLow(key) && !tooHigh(key);
			}

			void checkKeyBounds(K1 key) throw(std::invalid_argument)
			{
				if (key == NULL)
				{
					throw NullPointerException();
				}
				if (!inBounds(key))
				{
					throw std::invalid_argument("key out of range");
				}
			}

			/**
			 * Returns true if node key is less than upper bound of range
			 */
			bool isBeforeEnd(ConcurrentSkipListMap::Node<K1, V1> *n)
			{
				if (n == NULL)
				{
					return false;
				}
				if (hi == NULL)
				{
					return true;
				}
				K k = n->key;
				if (k == NULL) // pass by markers and headers
				{
					return true;
				}
				int c = m->compare(k, hi);
				if (c > 0 || (c == 0 && !hiInclusive))
				{
					return false;
				}
				return true;
			}

			/**
			 * Returns lowest node. This node might not be in range, so
			 * most usages need to check bounds
			 */
			ConcurrentSkipListMap::Node<K1, V1> *loNode()
			{
				if (lo == NULL)
				{
					return m->findFirst();
				}
				else if (loInclusive)
				{
					return m->findNear(lo, m->GT | m->EQ);
				}
				else
				{
					return m->findNear(lo, m->GT);
				}
			}

			/**
			 * Returns highest node. This node might not be in range, so
			 * most usages need to check bounds
			 */
			ConcurrentSkipListMap::Node<K1, V1> *hiNode()
			{
				if (hi == NULL)
				{
					return m->findLast();
				}
				else if (hiInclusive)
				{
					return m->findNear(hi, m->LT | m->EQ);
				}
				else
				{
					return m->findNear(hi, m->LT);
				}
			}

			/**
			 * Returns lowest absolute key (ignoring directonality)
			 */
			K1 lowestKey()
			{
				ConcurrentSkipListMap::Node<K1, V1> *n = loNode();
				if (isBeforeEnd(n))
				{
					return n->key;
				}
				else
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			/**
			 * Returns highest absolute key (ignoring directonality)
			 */
			K1 highestKey()
			{
				ConcurrentSkipListMap::Node<K1, V1> *n = hiNode();
				if (n != NULL)
				{
					K1 last = n->key;
					if (inBounds(last))
					{
						return last;
					}
				}
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
				throw std::runtime_error("error");
			}

			typename Map<K1,V1>:: template Entry<K1,V1> *lowestEntry()
			{
				for (;;)
				{
					ConcurrentSkipListMap::Node<K1, V1> *n = loNode();
					if (!isBeforeEnd(n))
					{
						return NULL;
					}
					typename Map<K1,V1>:: template Entry<K1,V1> *e = n->createSnapshot();
					if (e != NULL)
					{
						return e;
					}
				}
			}

			typename Map<K1,V1>:: template Entry<K1,V1> *highestEntry()
			{
				for (;;)
				{
					ConcurrentSkipListMap::Node<K1, V1> *n = hiNode();
					if (n == NULL || !inBounds(n->key))
					{
						return NULL;
					}
					typename Map<K1,V1>:: template Entry<K1,V1> *e = n->createSnapshot();
					if (e != NULL)
					{
						return e;
					}
				}
			}

			typename Map<K1,V1>:: template Entry<K1,V1> *removeLowest()
			{
				for (;;)
				{
					Node<K1, V1> *n = loNode();
					if (n == NULL)
					{
						return NULL;
					}
					K k = n->key;
					if (!inBounds(k))
					{
						return NULL;
					}
					V v = m->doRemove(k, NULL);
					if (v != NULL)
					{
						return new typename AbstractMap<K1, V1>::template SimpleImmutableEntry<K1, V1>(k, v);
					}
				}
			}

			typename Map<K1,V1>:: template Entry<K1,V1> *removeHighest()
			{
				for (;;)
				{
					Node<K1, V1> *n = hiNode();
					if (n == NULL)
					{
						return NULL;
					}
					K1 k = n->key;
					if (!inBounds(k))
					{
						return NULL;
					}
					V v = m->doRemove(k, NULL);
					if (v != NULL)
					{
						return new typename AbstractMap<K1, V1>::template SimpleImmutableEntry<K1, V1>(k, v);
					}
				}
			}

			/**
			 * Submap version of skiplist.ConcurrentSkipListMap.getNearEntry
			 */
			typename Map<K1,V1>:: template Entry<K1,V1> *getNearEntry(K1 key, int rel)
			{
				if (isDescending)
				{ // adjust relation for direction
					if ((rel & m->LT) == 0)
					{
						rel |= m->LT;
					}
					else
					{
						rel &= ~m->LT;
					}
				}
				if (tooLow(key))
				{
					return ((rel & m->LT) != 0)? NULL : lowestEntry();
				}
				if (tooHigh(key))
				{
					return ((rel & m->LT) != 0)? highestEntry() : NULL;
				}
				for (;;)
				{
					Node<K1, V1> *n = m->findNear(key, rel);
					if (n == NULL || !inBounds(n->key))
					{
						return NULL;
					}
					K1 k = n->key;
					V v = n->getValidValue();
					if (v != NULL)
					{
						return new typename AbstractMap<K1,V1>::template SimpleImmutableEntry<K1, V1>(k, v);
					}
				}
			}

			// Almost the same as getNearEntry, except for keys
			K1 getNearKey(K1 key, int rel)
			{
				if (isDescending)
				{ // adjust relation for direction
					if ((rel & m->LT) == 0)
					{
						rel |= m->LT;
					}
					else
					{
						rel &= ~m->LT;
					}
				}
				if (tooLow(key))
				{
					if ((rel & m->LT) == 0)
					{
						ConcurrentSkipListMap::Node<K1, V1> *n = loNode();
						if (isBeforeEnd(n))
						{
							return n->key;
						}
					}
					return NULL;
				}
				if (tooHigh(key))
				{
					if ((rel & m->LT) != 0)
					{
						ConcurrentSkipListMap::Node<K1, V1> *n = hiNode();
						if (n != NULL)
						{
							K1 last = n->key;
							if (inBounds(last))
							{
								return last;
							}
						}
					}
					return NULL;
				}
				for (;;)
				{
					Node<K1, V1> *n = m->findNear(key, rel);
					if (n == NULL || !inBounds(n->key))
					{
						return NULL;
					}
					K k = n->key;
					V v = n->getValidValue();
					if (v != NULL)
					{
						return k;
					}
				}
			}

			/* ----------------  skiplist.Map API methods -------------- */

		public:
			virtual bool containsKey(K key)
			{
				if (key == NULL)
				{
					throw NullPointerException();
				}
				K k = static_cast<K>(key);
				return inBounds(k) && m->containsKey(k);
			}

			virtual V get(K key)
			{
				if (key == NULL)
				{
					throw NullPointerException();
				}
				K k = static_cast<K>(key);
				return ((!inBounds(k)) ? NULL : m->get(k));
			}

			virtual V put(K key, V value)
			{
				checkKeyBounds(key);
				return m->put(key, value);
			}

			virtual V remove(void *key)
			{
				K k = static_cast<K>(key);
				return (!inBounds(k))? NULL : m->remove(k);
			}

			virtual int size()
			{
				long long count = 0;
				for (ConcurrentSkipListMap::Node<K, V> *n = loNode(); isBeforeEnd(n); n = n->atomicNext.get())
				{
					if (n->getValidValue() != NULL)
					{
						++count;
					}
				}
				return static_cast<int>(count);
			}

			virtual bool isEmpty()
			{
				return !isBeforeEnd(loNode());
			}

			virtual bool containsValue(void *value)
			{
				if (value == NULL)
				{
					throw NullPointerException();
				}
				for (ConcurrentSkipListMap::Node<K, V> *n = loNode(); isBeforeEnd(n); n = n->atomicNext.get())
				{
					V v = n->getValidValue();
					if (v != NULL && ((V)value)->equals(v))
					{
						return true;
					}
				}
				return false;
			}

			virtual void clear()
			{
				for (ConcurrentSkipListMap::Node<K, V> *n = loNode(); isBeforeEnd(n); n = n->atomicNext.get())
				{
					if (n->getValidValue() != NULL)
					{
						m->remove(n->key);
					}
				}
			}

			/* ----------------  skiplist.ConcurrentMap API methods -------------- */

			V putIfAbsent(K key, V value)
			{
				checkKeyBounds(key);
				return m->putIfAbsent(key, value);
			}

			bool remove(void *key, void *value)
			{
				K k = static_cast<K>(key);
				return inBounds(k) && m->remove(k, value);
			}

			bool replace(K key, V oldValue, V newValue)
			{
				checkKeyBounds(key);
				return m->replace(key, oldValue, newValue);
			}

			V replace(K key, V value)
			{
				checkKeyBounds(key);
				return m->replace(key, value);
			}

			/* ----------------  skiplist.SortedMap API methods -------------- */

			Comparator<K> *comparator()
			{
				Comparator<K> *cmp = m->comparator();
				if (isDescending)
				{
					return reverse(cmp);
				}
				else
				{
					return cmp;
				}
			}

		private:
			Comparator<K> *reverse(Comparator<K> *cmp)
			{
				if (cmp == NULL)
				{
					//todo: leak
					ReverseComparator<K> *tempVar = new ReverseComparator<K>();
					return static_cast<Comparator<K>*>(tempVar);
				}
				else
				{
					return new ReverseComparator2<K>(cmp);
				}
			}

		private:
			template<typename T>
			class ReverseComparator2 : public Comparator<T>, public Serializable
			{
			private:
				static constexpr long long serialVersionUID = 4374092139857LL;

				/**
				 * The comparatorField specified in the static factory.  This will never
				 * be null, as the static factory returns a ReverseComparator
				 * instance if its argument is null.
				 *
				 * @serial
				 */
				Comparator<T> *cmp;

			public:
				ReverseComparator2(Comparator<T> *cmp)
				{
					assert(cmp != NULL);
					this->cmp = cmp;
				}

				virtual int compare(T t1, T t2)
				{
					return cmp->compare(t2, t1);
				}
			};

		private:
			template<typename E>
			class ReverseComparator : public Comparator<E>, public Serializable
			{

				// use serialVersionUID from JDK 1.2.2 for interoperability
			private:
				static constexpr long long serialVersionUID = 7207038068494060240LL;

			public:
				virtual int compare(E c1, E c2)
				{
					return c2->compareTo(c1);
				}

			private:
				void *readResolve()
				{
					return new ReverseComparator<E>();
				}
			};

			/**
			 * Utility to create submaps, where given bounds override
			 * unbounded(null) ones and/or are checked against bounded ones.
			 */
		private:
			SubMap<K, V> *newSubMap(K fromKey, bool fromInclusive, K toKey, bool toInclusive)
			{
				if (isDescending)
				{ // flip senses
					K tk = fromKey;
					fromKey = toKey;
					toKey = tk;
					bool ti = fromInclusive;
					fromInclusive = toInclusive;
					toInclusive = ti;
				}
				if (lo != NULL)
				{
					if (fromKey == NULL)
					{
						fromKey = lo;
						fromInclusive = loInclusive;
					}
					else
					{
						int c = m->compare(fromKey, lo);
						if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
						{
							throw std::invalid_argument("key out of range");
						}
					}
				}
				if (hi != NULL)
				{
					if (toKey == NULL)
					{
						toKey = hi;
						toInclusive = hiInclusive;
					}
					else
					{
						int c = m->compare(toKey, hi);
						if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
						{
							throw std::invalid_argument("key out of range");
						}
					}
				}
				return new SubMap<K, V>(m, fromKey, fromInclusive, toKey, toInclusive, isDescending);
			}

		public:
			ConcurrentNavigableMap<K, V> *subMap(K fromKey, bool fromInclusive, K toKey, bool toInclusive)
			{
				if (fromKey == NULL || toKey == NULL)
				{
					throw NullPointerException();
				}
				return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
			}

			ConcurrentNavigableMap<K, V> *headMap(K toKey, bool inclusive)
			{
				if (toKey == NULL)
				{
					throw NullPointerException();
				}
				return newSubMap(NULL, false, toKey, inclusive);
			}

			ConcurrentNavigableMap<K, V> *tailMap(K fromKey, bool inclusive)
			{
				if (fromKey == NULL)
				{
					throw NullPointerException();
				}
				return newSubMap(fromKey, inclusive, NULL, false);
			}

			ConcurrentNavigableMap<K, V> *subMap(K fromKey, K toKey)
			{
				return subMap(fromKey, true, toKey, false);
			}

			ConcurrentNavigableMap<K, V> *headMap(K toKey)
			{
				return headMap(toKey, false);
			}

			ConcurrentNavigableMap<K, V> *tailMap(K fromKey)
			{
				return tailMap(fromKey, true);
			}

			ConcurrentNavigableMap<K, V> *descendingMap()
			{
				return new SubMap<K, V>(m, lo, loInclusive, hi, hiInclusive, !isDescending);
			}

			/* ----------------  Relational methods -------------- */

			typename Map<K,V>:: template Entry<K,V> *ceilingEntry(K key)
			{
				return getNearEntry(key, (m->GT | m->EQ));
			}

			K ceilingKey(K key)
			{
				return getNearKey(key, (m->GT | m->EQ));
			}

			typename Map<K,V>:: template Entry<K,V> *lowerEntry(K key)
			{
				return getNearEntry(key, (m->LT));
			}

			K lowerKey(K key)
			{
				return getNearKey(key, (m->LT));
			}

			typename Map<K,V>:: template Entry<K,V> *floorEntry(K key)
			{
				return getNearEntry(key, (m->LT | m->EQ));
			}

			K floorKey(K key)
			{
				return getNearKey(key, (m->LT | m->EQ));
			}

			typename Map<K,V>:: template Entry<K,V> *higherEntry(K key)
			{
				return getNearEntry(key, (m->GT));
			}

			K higherKey(K key)
			{
				return getNearKey(key, (m->GT));
			}

			K firstKey()
			{
				return isDescending? highestKey() : lowestKey();
			}

			K lastKey()
			{
				return isDescending? lowestKey() : highestKey();
			}

			typename Map<K,V>:: template Entry<K,V> *firstEntry()
			{
				return isDescending? highestEntry() : lowestEntry();
			}

			typename Map<K,V>:: template Entry<K,V> *lastEntry()
			{
				return isDescending? lowestEntry() : highestEntry();
			}

			typename Map<K,V>:: template Entry<K,V> *pollFirstEntry()
			{
				return isDescending? removeHighest() : removeLowest();
			}

			typename Map<K,V>:: template Entry<K,V> *pollLastEntry()
			{
				return isDescending? removeLowest() : removeHighest();
			}

			/* ---------------- Submap Views -------------- */

			virtual NavigableSet<K> *keySet()
			{
				KeySet<K> *ks = keySetView;
				return (ks != NULL) ? ks : (keySetView = new KeySet<K>(this));
			}

			NavigableSet<K> *navigableKeySet()
			{
				KeySet<K> *ks = keySetView;
				return (ks != NULL) ? ks : (keySetView = new KeySet<K>(this));
			}

			virtual Collection<V> *values()
			{
				Collection<V> *vs = valuesView;
				return (vs != NULL) ? vs : (valuesView = new Values<V>(this));
			}

			virtual Set<typename Map<K,V>::template Entry<K, V>*> *entrySet()
			{
				Set<typename Map<K,V>::template Entry<K, V>*> *es = entrySetView;
				return (es != NULL) ? es : (entrySetView = new EntrySet<K, V>(this));
			}

			NavigableSet<K> *descendingKeySet()
			{
				return descendingMap()->navigableKeySet();
			}

			Iterator<K> *keyIterator()
			{
				return new SubMapKeyIterator(this);
			}

			Iterator<V> *valueIterator()
			{
				return new SubMapValueIterator(this);
			}

			Iterator<typename Map<K,V>:: template Entry<K,V>*> *entryIterator()
			{
				return new SubMapEntryIterator(this);
			}

			/**
			 * Variant of main Iter class to traverse through submaps.
			 */
		public:
			template<typename T>
			class SubMapIter : public Iterator<T>
			{
			private:
				ConcurrentSkipListMap::SubMap<K, V> *outerInstance;

				/** the last node returned by next() */
			public:
				Node<K, V> *lastReturned;
				/** the next node to return from next(); */
				Node<K, V> *next;
				/** Cache of next value field to maintain weak consistency */
				V nextValue;

				SubMapIter(ConcurrentSkipListMap::SubMap<K, V> *outerInstance) : outerInstance(outerInstance)
				{
					for (;;)
					{
						next = outerInstance->isDescending ? outerInstance->hiNode() : outerInstance->loNode();
						if (next == NULL)
						{
							break;
						}
						void *x = next->atomicValue.get();
						if (x != NULL && x != next)
						{
							if (!outerInstance->inBounds(next->key))
							{
								next = NULL;
							}
							else
							{
								nextValue = static_cast<V>(x);
							}
							break;
						}
					}
				}

				virtual bool hasNext()
				{
					return next != NULL;
				}

				virtual void advance()
				{
					if (next == NULL)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
					lastReturned = next;
					if (outerInstance->isDescending)
					{
						descend();
					}
					else
					{
						ascend();
					}
				}

			private:
				void ascend()
				{
					for (;;)
					{
						next = next->atomicNext.get();
						if (next == NULL)
						{
							break;
						}
						void *x = next->atomicValue.get();
						if (x != NULL && x != next)
						{
							if (outerInstance->tooHigh(next->key))
							{
								next = NULL;
							}
							else
							{
								nextValue = static_cast<V>(x);
							}
							break;
						}
					}
				}

				void descend()
				{
					for (;;)
					{
						next = outerInstance->m->findNear(lastReturned->key, LT);
						if (next == NULL)
						{
							break;
						}
						void *x = next->atomicValue.get();
						if (x != NULL && x != next)
						{
							if (outerInstance->tooLow(next->key))
							{
								next = NULL;
							}
							else
							{
								nextValue = static_cast<V>(x);
							}
							break;
						}
					}
				}

			public:
				virtual void remove()
				{
					Node<K, V> *l = lastReturned;
					if (l == NULL)
					{
						throw IllegalStateException();
					}
					outerInstance->m->remove(l->key);
					lastReturned = NULL;
				}

			};

		public:
			class SubMapValueIterator : public SubMapIter<V>
			{
			private:
				ConcurrentSkipListMap::SubMap<K, V> *outerInstance;

			public:
				SubMapValueIterator(ConcurrentSkipListMap::SubMap<K, V> *outerInstance) : 
					SubMapIter<V>(outerInstance), outerInstance(outerInstance)
				{
				}

				V nextEntry(V entry) {
					return NULL;
				}

				V nextEntry()
				{
					V v = SubMapIter<V>::nextValue;
					SubMapIter<V>::advance();
					return v;
				}
			};

		public:
			class SubMapKeyIterator : public SubMapIter<K>
			{
			private:
				ConcurrentSkipListMap::SubMap<K, V> *outerInstance;

			public:
				SubMapKeyIterator(ConcurrentSkipListMap::SubMap<K, V> *outerInstance) : 
					SubMapIter<K>(outerInstance), outerInstance(outerInstance)
				{
				}

				K nextEntry(K entry) {
					return NULL;
				}

				K nextEntry()
				{
					Node<K, V> *n = SubMapIter<K>::next;
					SubMapIter<K>::advance();
					return n->key;
				}
			};

		public:
			class SubMapEntryIterator : public SubMapIter<typename Map<K,V>:: template Entry<K,V>*>
			{
			private:
				ConcurrentSkipListMap::SubMap<K, V> *outerInstance;

			public:
				SubMapEntryIterator(ConcurrentSkipListMap<K,V>::SubMap<K, V> *outerInstance) : 
					SubMapIter<typename Map<K,V>:: template Entry<K,V>*>(outerInstance), outerInstance(outerInstance)
				{
				}

				virtual typename Map<K, V>:: template Entry<K, V> * nextEntry(typename Map<K, V>:: template Entry<K, V> * entry) {
					Node<K, V> *n = SubMapIter<typename Map<K, V>:: template Entry<K, V>*>::next;
					V v = SubMapIter<typename Map<K, V>:: template Entry<K, V>*>::nextValue;
					SubMapIter<typename Map<K, V>:: template Entry<K, V>*>::advance();
					((typename AbstractMap<K, V>::template SimpleImmutableEntry<K, V>*)entry)->setKey(n->key);
					entry->setValue(v);
					return entry;
				}
				typename Map<K,V>:: template Entry<K,V> *nextEntry()
				{
					Node<K, V> *n = SubMapIter<typename Map<K,V>:: template Entry<K,V>*>::next;
					V v = SubMapIter<typename Map<K,V>:: template Entry<K,V>*>::nextValue;
					SubMapIter<typename Map<K,V>:: template Entry<K,V>*>::advance();
					return new typename AbstractMap<K,V>::template SimpleImmutableEntry<K, V>(n->key, v);
				}
			};
		};

		/**
		* The topmost head index of the skiplist.
		*/
		//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
		//ORIGINAL LINE: private transient volatile AtomicJava<HeadIndex<K,V>> head;

		AtomicJava<HeadIndex<K, V>*> atomicHead;

		/**
		* The comparatorField used to maintain order in this map, or null
		* if using natural ordering.
		* @serial
		*/
		Comparator<K> *const comparatorField;

		/**
		* Seed for simple random number generator.  Not volatile since it
		* doesn't matter too much if different threads don't see updates.
		*/
		int randomSeed = 0;


		/** Lazily initialized key set */
		//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		ConcurrentSkipListMap::KeySet<K> *keySet_Conflict;
		/** Lazily initialized entry set */
		//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		ConcurrentSkipListMap::EntrySet<K, V> *entrySet_Conflict;
		/** Lazily initialized values collection */
		//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		Values<V> *values_Conflict;
		/** Lazily initialized descending key set */
		//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		ConcurrentNavigableMap<K, V> *descendingMap_Conflict;


	};

}
