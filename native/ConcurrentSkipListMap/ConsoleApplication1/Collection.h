#pragma once

#include "Iterable.h"

//JAVA TO C++ CONVERTER NOTE: Forward class declarations:
namespace skiplist { template<typename E>class Iterator; }

namespace skiplist
{

	/*
	 * Copyright (c) 1997, 2006, Oracle and/or its affiliates. All rights reserved.
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

	/**
	 * The root interface in the <i>collection hierarchy</i>.  A collection
	 * represents a group of objects, known as its <i>elements</i>.  Some
	 * collections allow duplicate elements and others do not.  Some are ordered
	 * and others unordered.  The JDK does not provide any <i>direct</i>
	 * implementations of this interface: it provides implementations of more
	 * specific subinterfaces like <tt>skiplist.Set</tt> and <tt>List</tt>.  This interface
	 * is typically used to pass collections around and manipulate them where
	 * maximum generality is desired.
	 *
	 * <p><i>Bags</i> or <i>multisets</i> (unordered collections that may contain
	 * duplicate elements) should implement this interface directly.
	 *
	 * <p>All general-purpose <tt>skiplist.Collection</tt> implementation classes (which
	 * typically implement <tt>skiplist.Collection</tt> indirectly through one of its
	 * subinterfaces) should provide two "standard" constructors: a void (no
	 * arguments) constructor, which creates an empty collection, and a
	 * constructor with a single argument of type <tt>skiplist.Collection</tt>, which
	 * creates a new collection with the same elements as its argument.  In
	 * effect, the latter constructor allows the user to copy any collection,
	 * producing an equivalent collection of the desired implementation type.
	 * There is no way to enforce this convention (as interfaces cannot contain
	 * constructors) but all of the general-purpose <tt>skiplist.Collection</tt>
	 * implementations in the Java platform libraries comply.
	 *
	 * <p>The "destructive" methods contained in this interface, that is, the
	 * methods that modify the collection on which they operate, are specified to
	 * throw <tt>UnsupportedOperationException</tt> if this collection does not
	 * support the operation.  If this is the case, these methods may, but are not
	 * required to, throw an <tt>UnsupportedOperationException</tt> if the
	 * invocation would have no effect on the collection.  For example, invoking
	 * the {@link #addAll(Collection)} method on an unmodifiable collection may,
	 * but is not required to, throw the exception if the collection to be added
	 * is empty.
	 *
	 * <p>Some collection implementations have restrictions on the elements that
	 * they may contain.  For example, some implementations prohibit null elements,
	 * and some have restrictions on the types of their elements.  Attempting to
	 * add an ineligible element throws an unchecked exception, typically
	 * <tt>NullPointerException</tt> or <tt>ClassCastException</tt>.  Attempting
	 * to query the presence of an ineligible element may throw an exception,
	 * or it may simply return false; some implementations will exhibit the former
	 * behavior and some will exhibit the latter.  More generally, attempting an
	 * operation on an ineligible element whose completion would not result in
	 * the insertion of an ineligible element into the collection may throw an
	 * exception or it may succeed, at the option of the implementation.
	 * Such exceptions are marked as "optional" in the specification for this
	 * interface.
	 *
	 * <p>It is up to each collection to determine its own synchronization
	 * policy.  In the absence of a stronger guarantee by the
	 * implementation, undefined behavior may result from the invocation
	 * of any method on a collection that is being mutated by another
	 * thread; this includes direct invocations, passing the collection to
	 * a method that might perform invocations, and using an existing
	 * beginIterator to examine the collection.
	 *
	 * <p>Many methods in Collections Framework interfaces are defined in
	 * terms of the {@link Object#equals(Object) equals} method.  For example,
	 * the specification for the {@link #contains(Object) contains(Object o)}
	 * method says: "returns <tt>true</tt> if and only if this collection
	 * contains at least one element <tt>e</tt> such that
	 * <tt>(o==null ? e==null : o.equals(e))</tt>."  This specification should
	 * <i>not</i> be construed to imply that invoking <tt>skiplist.Collection.contains</tt>
	 * with a non-null argument <tt>o</tt> will cause <tt>o.equals(e)</tt> to be
	 * invoked for any element <tt>e</tt>.  Implementations are free to implement
	 * optimizations whereby the <tt>equals</tt> invocation is avoided, for
	 * example, by first comparing the hash codes of the two elements.  (The
	 * {@link Object#hashCode()} specification guarantees that two objects with
	 * unequal hash codes cannot be equal.)  More generally, implementations of
	 * the various Collections Framework interfaces are free to take advantage of
	 * the specified behavior of underlying {@link Object} methods wherever the
	 * implementor deems it appropriate.
	 *
	 * <p>This interface is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @see     Set
	 * @see     List
	 * @see     Map
	 * @see     SortedSet
	 * @see     SortedMap
	 * @see     HashSet
	 * @see     TreeSet
	 * @see     ArrayList
	 * @see     LinkedList
	 * @see     Vector
	 * @see     Collections
	 * @see     Arrays
	 * @see     AbstractCollection
	 * @since 1.2
	 */

	template<typename E>
	class Collection : public Iterable<E>
	{
		// Query Operations

		/**
		 * Returns the number of elements in this collection.  If this collection
		 * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
		 * <tt>Integer.MAX_VALUE</tt>.
		 *
		 * @return the number of elements in this collection
		 */
	public:
		virtual int size() {
			return 0;
		}

		/**
		 * Returns <tt>true</tt> if this collection contains no elements.
		 *
		 * @return <tt>true</tt> if this collection contains no elements
		 */
		virtual bool isEmpty() {
			return false;
		}

		/**
		 * Returns <tt>true</tt> if this collection contains the specified element.
		 * More formally, returns <tt>true</tt> if and only if this collection
		 * contains at least one element <tt>e</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
		 *
		 * @param o element whose presence in this collection is to be tested
		 * @return <tt>true</tt> if this collection contains the specified
		 *         element
		 * @throws ClassCastException if the type of the specified element
		 *         is incompatible with this collection (optional)
		 * @throws NullPointerException if the specified element is null and this
		 *         collection does not permit null elements (optional)
		 */
		virtual bool contains(void *o) {
			return false;
		}

		/**
		 * Returns an beginIterator over the elements in this collection.  There are no
		 * guarantees concerning the order in which the elements are returned
		 * (unless this collection is an instance of some class that provides a
		 * guarantee).
		 *
		 * @return an <tt>skiplist.Iterator</tt> over the elements in this collection
		 */
		virtual Iterator<E> *beginIterator() {
			return NULL;
		}

		// Modification Operations

		/**
		 * Ensures that this collection contains the specified element (optional
		 * operation).  Returns <tt>true</tt> if this collection changed as a
		 * result of the call.  (Returns <tt>false</tt> if this collection does
		 * not permit duplicates and already contains the specified element.)<p>
		 *
		 * Collections that support this operation may place limitations on what
		 * elements may be added to this collection.  In particular, some
		 * collections will refuse to add <tt>null</tt> elements, and others will
		 * impose restrictions on the type of elements that may be added.
		 * skiplist.Collection classes should clearly specify in their documentation any
		 * restrictions on what elements may be added.<p>
		 *
		 * If a collection refuses to add a particular element for any reason
		 * other than that it already contains the element, it <i>must</i> throw
		 * an exception (rather than returning <tt>false</tt>).  This preserves
		 * the invariant that a collection always contains the specified element
		 * after this call returns.
		 *
		 * @param e element whose presence in this collection is to be ensured
		 * @return <tt>true</tt> if this collection changed as a result of the
		 *         call
		 * @throws UnsupportedOperationException if the <tt>add</tt> operation
		 *         is not supported by this collection
		 * @throws ClassCastException if the class of the specified element
		 *         prevents it from being added to this collection
		 * @throws NullPointerException if the specified element is null and this
		 *         collection does not permit null elements
		 * @throws IllegalArgumentException if some property of the element
		 *         prevents it from being added to this collection
		 * @throws IllegalStateException if the element cannot be added at this
		 *         time due to insertion restrictions
		 */
		virtual bool add(E e) {
			return false;
		}

		/**
		 * Removes a single instance of the specified element from this
		 * collection, if it is present (optional operation).  More formally,
		 * removes an element <tt>e</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>, if
		 * this collection contains one or more such elements.  Returns
		 * <tt>true</tt> if this collection contained the specified element (or
		 * equivalently, if this collection changed as a result of the call).
		 *
		 * @param o element to be removed from this collection, if present
		 * @return <tt>true</tt> if an element was removed as a result of this call
		 * @throws ClassCastException if the type of the specified element
		 *         is incompatible with this collection (optional)
		 * @throws NullPointerException if the specified element is null and this
		 *         collection does not permit null elements (optional)
		 * @throws UnsupportedOperationException if the <tt>remove</tt> operation
		 *         is not supported by this collection
		 */
		virtual bool remove(void *o) {
			return false;
		}


		// Bulk Operations

		/**
		 * Returns <tt>true</tt> if this collection contains all of the elements
		 * in the specified collection.
		 *
		 * @param  c collection to be checked for containment in this collection
		 * @return <tt>true</tt> if this collection contains all of the elements
		 *         in the specified collection
		 * @throws ClassCastException if the types of one or more elements
		 *         in the specified collection are incompatible with this
		 *         collection (optional)
		 * @throws NullPointerException if the specified collection contains one
		 *         or more null elements and this collection does not permit null
		 *         elements (optional), or if the specified collection is null
		 * @see    #contains(Object)
		 */
		virtual bool containsAll(Collection<E> *c) {
			return false;
		}

		/**
		 * Adds all of the elements in the specified collection to this collection
		 * (optional operation).  The behavior of this operation is undefined if
		 * the specified collection is modified while the operation is in progress.
		 * (This implies that the behavior of this call is undefined if the
		 * specified collection is this collection, and this collection is
		 * nonempty.)
		 *
		 * @param c collection containing elements to be added to this collection
		 * @return <tt>true</tt> if this collection changed as a result of the call
		 * @throws UnsupportedOperationException if the <tt>addAll</tt> operation
		 *         is not supported by this collection
		 * @throws ClassCastException if the class of an element of the specified
		 *         collection prevents it from being added to this collection
		 * @throws NullPointerException if the specified collection contains a
		 *         null element and this collection does not permit null elements,
		 *         or if the specified collection is null
		 * @throws IllegalArgumentException if some property of an element of the
		 *         specified collection prevents it from being added to this
		 *         collection
		 * @throws IllegalStateException if not all the elements can be added at
		 *         this time due to insertion restrictions
		 * @see #add(Object)
		 */
		virtual bool addAll(Collection<E> *c) {
			return false;
		}

		/**
		 * Removes all of this collection's elements that are also contained in the
		 * specified collection (optional operation).  After this call returns,
		 * this collection will contain no elements in common with the specified
		 * collection.
		 *
		 * @param c collection containing elements to be removed from this collection
		 * @return <tt>true</tt> if this collection changed as a result of the
		 *         call
		 * @throws UnsupportedOperationException if the <tt>removeAll</tt> method
		 *         is not supported by this collection
		 * @throws ClassCastException if the types of one or more elements
		 *         in this collection are incompatible with the specified
		 *         collection (optional)
		 * @throws NullPointerException if this collection contains one or more
		 *         null elements and the specified collection does not support
		 *         null elements (optional), or if the specified collection is null
		 * @see #remove(Object)
		 * @see #contains(Object)
		 */
		virtual bool removeAll(Collection<E> *c) {
			return false;
		}

		/**
		 * Retains only the elements in this collection that are contained in the
		 * specified collection (optional operation).  In other words, removes from
		 * this collection all of its elements that are not contained in the
		 * specified collection.
		 *
		 * @param c collection containing elements to be retained in this collection
		 * @return <tt>true</tt> if this collection changed as a result of the call
		 * @throws UnsupportedOperationException if the <tt>retainAll</tt> operation
		 *         is not supported by this collection
		 * @throws ClassCastException if the types of one or more elements
		 *         in this collection are incompatible with the specified
		 *         collection (optional)
		 * @throws NullPointerException if this collection contains one or more
		 *         null elements and the specified collection does not permit null
		 *         elements (optional), or if the specified collection is null
		 * @see #remove(Object)
		 * @see #contains(Object)
		 */
		virtual bool retainAll(Collection<E> *c) {
			return false;
		}

		/**
		 * Removes all of the elements from this collection (optional operation).
		 * The collection will be empty after this method returns.
		 *
		 * @throws UnsupportedOperationException if the <tt>clear</tt> operation
		 *         is not supported by this collection
		 */
		virtual void clear() {

		}


		// Comparison and hashing

		/**
		 * Compares the specified object with this collection for equality. <p>
		 *
		 * While the <tt>skiplist.Collection</tt> interface adds no stipulations to the
		 * general contract for the <tt>Object.equals</tt>, programmers who
		 * implement the <tt>skiplist.Collection</tt> interface "directly" (in other words,
		 * create a class that is a <tt>skiplist.Collection</tt> but is not a <tt>skiplist.Set</tt>
		 * or a <tt>List</tt>) must exercise care if they choose to override the
		 * <tt>Object.equals</tt>.  It is not necessary to do so, and the simplest
		 * course of action is to rely on <tt>Object</tt>'s implementation, but
		 * the implementor may wish to implement a "value comparison" in place of
		 * the default "reference comparison."  (The <tt>List</tt> and
		 * <tt>skiplist.Set</tt> interfaces mandate such value comparisons.)<p>
		 *
		 * The general contract for the <tt>Object.equals</tt> method states that
		 * equals must be symmetric (in other words, <tt>a.equals(b)</tt> if and
		 * only if <tt>b.equals(a)</tt>).  The contracts for <tt>List.equals</tt>
		 * and <tt>skiplist.Set.equals</tt> state that lists are only equal to other lists,
		 * and sets to other sets.  Thus, a custom <tt>equals</tt> method for a
		 * collection class that implements neither the <tt>List</tt> nor
		 * <tt>skiplist.Set</tt> interface must return <tt>false</tt> when this collection
		 * is compared to any list or set.  (By the same logic, it is not possible
		 * to write a class that correctly implements both the <tt>skiplist.Set</tt> and
		 * <tt>List</tt> interfaces.)
		 *
		 * @param o object to be compared for equality with this collection
		 * @return <tt>true</tt> if the specified object is equal to this
		 * collection
		 *
		 * @see Object#equals(Object)
		 * @see Set#equals(Object)
		 * @see List#equals(Object)
		 */
		virtual bool equals(void *o) {
			return false;
		}

		/**
		 * Returns the hash code value for this collection.  While the
		 * <tt>skiplist.Collection</tt> interface adds no stipulations to the general
		 * contract for the <tt>Object.hashCode</tt> method, programmers should
		 * take note that any class that overrides the <tt>Object.equals</tt>
		 * method must also override the <tt>Object.hashCode</tt> method in order
		 * to satisfy the general contract for the <tt>Object.hashCode</tt>method.
		 * In particular, <tt>c1.equals(c2)</tt> implies that
		 * <tt>c1.hashCode()==c2.hashCode()</tt>.
		 *
		 * @return the hash code value for this collection
		 *
		 * @see Object#hashCode()
		 * @see Object#equals(Object)
		 */
		virtual int hashCode() {
			return 0;
		}
	};

}
