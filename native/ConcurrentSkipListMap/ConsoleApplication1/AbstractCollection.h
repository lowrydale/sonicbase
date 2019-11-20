#pragma once

#include "Collection.h"
#include "Iterator.h"
#include <string>
#include "exceptionhelper.h"
#include "stringbuilder.h"

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
	 * This class provides a skeletal implementation of the <tt>skiplist.Collection</tt>
	 * interface, to minimize the effort required to implement this interface. <p>
	 *
	 * To implement an unmodifiable collection, the programmer needs only to
	 * extend this class and provide implementations for the <tt>beginIterator</tt> and
	 * <tt>size</tt> methods.  (The beginIterator returned by the <tt>beginIterator</tt>
	 * method must implement <tt>hasNext</tt> and <tt>next</tt>.)<p>
	 *
	 * To implement a modifiable collection, the programmer must additionally
	 * override this class's <tt>add</tt> method (which otherwise throws an
	 * <tt>UnsupportedOperationException</tt>), and the beginIterator returned by the
	 * <tt>beginIterator</tt> method must additionally implement its <tt>remove</tt>
	 * method.<p>
	 *
	 * The programmer should generally provide a void (no argument) and
	 * <tt>skiplist.Collection</tt> constructor, as per the recommendation in the
	 * <tt>skiplist.Collection</tt> interface specification.<p>
	 *
	 * The documentation for each non-abstract method in this class describes its
	 * implementation in detail.  Each of these methods may be overridden if
	 * the collection being implemented admits a more efficient implementation.<p>
	 *
	 * This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @see Collection
	 * @since 1.2
	 */

	template<typename E>
	class AbstractCollection : public Collection<E>
	{
		/**
		 * Sole constructor.  (For invocation by subclass constructors, typically
		 * implicit.)
		 */
	protected:
		AbstractCollection()
		{
		}

		// Query Operations

		/**
		 * Returns an beginIterator over the elements contained in this collection.
		 *
		 * @return an beginIterator over the elements contained in this collection
		 */
	public:
		virtual Iterator<E> *beginIterator()
		{
			return NULL;
		}

		virtual int size()
		{
			return 0;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns <tt>size() == 0</tt>.
		 */
		virtual bool isEmpty()
		{
			return size() == 0;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over the elements in the collection,
		 * checking each element in turn for equality with the specified element.
		 *
		 * @throws ClassCastException   {@inheritDoc}
		 * @throws NullPointerException {@inheritDoc}
		 */
		virtual bool contains(void *o)
		{
			Iterator<E> *e = beginIterator();
			if (o == NULL)
			{
				while (e->hasNext())
				{
					if (e->nextEntry() == NULL)
					{
						return true;
					}
					e++;
				}
			}
			else
			{
				while (e->hasNext())
				{
					if (((E)o)->equals(e->nextEntry()))
					{
						return true;
					}
					e++;
				}
			}
			return false;
		}

		// Modification Operations

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation always throws an
		 * <tt>UnsupportedOperationException</tt>.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 * @throws IllegalStateException         {@inheritDoc}
		 */
		virtual bool add(E e)
		{
			throw UnsupportedOperationException();
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over the collection looking for the
		 * specified element.  If it finds the element, it removes the element
		 * from the collection using the beginIterator's remove method.
		 *
		 * <p>Note that this implementation throws an
		 * <tt>UnsupportedOperationException</tt> if the beginIterator returned by this
		 * collection's beginIterator method does not implement the <tt>remove</tt>
		 * method and this collection contains the specified object.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 */
		virtual bool remove(void *o)
		{
			Iterator<E> *e = beginIterator();
			if (o == NULL)
			{
				while (e->hasNext())
				{
					if (e->nextEntry() == NULL)
					{
						e->remove();
						return true;
					}
					e++;
				}
			}
			else
			{
				while (e->hasNext())
				{
					if (((E)o)->equals(e->nextEntry()))
					{
						e->remove();
						return true;
					}
					e++;
				}
			}
			return false;
		}


		// Bulk Operations

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over the specified collection,
		 * checking each element returned by the beginIterator in turn to see
		 * if it's contained in this collection.  If all elements are so
		 * contained <tt>true</tt> is returned, otherwise <tt>false</tt>.
		 *
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @see #contains(Object)
		 */
		virtual bool containsAll(Collection<E> *c)
		{
			Iterator<E> *e = c->beginIterator();
			while (e->hasNext())
			{
				if (!contains(e->nextEntry()))
				{
					return false;
				}
				e++;
			}
			return true;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over the specified collection, and adds
		 * each object returned by the beginIterator to this collection, in turn.
		 *
		 * <p>Note that this implementation will throw an
		 * <tt>UnsupportedOperationException</tt> unless <tt>add</tt> is
		 * overridden (assuming the specified collection is non-empty).
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 * @throws IllegalStateException         {@inheritDoc}
		 *
		 * @see #add(Object)
		 */
		virtual bool addAll(Collection<E> *c)
		{
			bool modified = false;
			Iterator<E> *e = c->beginIterator();
			while (e->hasNext())
			{
				if (add(e->nextEntry()))
				{
					modified = true;
				}
				e++;
			}
			return modified;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over this collection, checking each
		 * element returned by the beginIterator in turn to see if it's contained
		 * in the specified collection.  If it's so contained, it's removed from
		 * this collection with the beginIterator's <tt>remove</tt> method.
		 *
		 * <p>Note that this implementation will throw an
		 * <tt>UnsupportedOperationException</tt> if the beginIterator returned by the
		 * <tt>beginIterator</tt> method does not implement the <tt>remove</tt> method
		 * and this collection contains one or more elements in common with the
		 * specified collection.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 *
		 * @see #remove(Object)
		 * @see #contains(Object)
		 */
		virtual bool removeAll(Collection<E> *c)
		{
			bool modified = false;
			Iterator<E> *e = AbstractCollection::beginIterator();
			while (e->hasNext())
			{
				if (c->contains(e->nextEntry()))
				{
					e->remove();
					modified = true;
				}
				e++;
			}
			return modified;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over this collection, checking each
		 * element returned by the beginIterator in turn to see if it's contained
		 * in the specified collection.  If it's not so contained, it's removed
		 * from this collection with the beginIterator's <tt>remove</tt> method.
		 *
		 * <p>Note that this implementation will throw an
		 * <tt>UnsupportedOperationException</tt> if the beginIterator returned by the
		 * <tt>beginIterator</tt> method does not implement the <tt>remove</tt> method
		 * and this collection contains one or more elements not present in the
		 * specified collection.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 *
		 * @see #remove(Object)
		 * @see #contains(Object)
		 */
		virtual bool retainAll(Collection<E> *c)
		{
			bool modified = false;
			Iterator<E> *e = AbstractCollection::beginIterator();
			while (e->hasNext())
			{
				if (!c->contains(e->nextEntry()))
				{
					e->remove();
					modified = true;
				}
				e++;
			}
			return modified;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over this collection, removing each
		 * element using the <tt>skiplist.Iterator.remove</tt> operation.  Most
		 * implementations will probably choose to override this method for
		 * efficiency.
		 *
		 * <p>Note that this implementation will throw an
		 * <tt>UnsupportedOperationException</tt> if the beginIterator returned by this
		 * collection's <tt>beginIterator</tt> method does not implement the
		 * <tt>remove</tt> method and this collection is non-empty.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 */
		virtual void clear()
		{
			Iterator<E> *e = AbstractCollection::beginIterator();
			while (e->hasNext())
			{
				e->nextEntry();
				e->remove();
				e++;
			}
		}


	
	};

}
