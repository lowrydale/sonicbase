#pragma once

using namespace skiplist;
#include "AbstractCollection.h"
#include "Set.h"
#include "Collection.h"
#include "Iterator.h"
#include "exceptionhelper.h"

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
	 * This class provides a skeletal implementation of the <tt>skiplist.Set</tt>
	 * interface to minimize the effort required to implement this
	 * interface. <p>
	 *
	 * The process of implementing a set by extending this class is identical
	 * to that of implementing a skiplist.Collection by extending skiplist.AbstractCollection,
	 * except that all of the methods and constructors in subclasses of this
	 * class must obey the additional constraints imposed by the <tt>skiplist.Set</tt>
	 * interface (for instance, the add method must not permit addition of
	 * multiple instances of an object to a set).<p>
	 *
	 * Note that this class does not override any of the implementations from
	 * the <tt>skiplist.AbstractCollection</tt> class.  It merely adds implementations
	 * for <tt>equals</tt> and <tt>hashCode</tt>.<p>
	 *
	 * This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @param <E> the type of elements maintained by this set
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @see Collection
	 * @see AbstractCollection
	 * @see Set
	 * @since 1.2
	 */

	template<typename E>
	class AbstractSet : public AbstractCollection<E>, public Set<E>
	{
		/**
		 * Sole constructor.  (For invocation by subclass constructors, typically
		 * implicit.)
		 */
	protected:
		AbstractSet()
		{
		}

		// Comparison and hashing

		/**
		 * Compares the specified object with this set for equality.  Returns
		 * <tt>true</tt> if the given object is also a set, the two sets have
		 * the same size, and every member of the given set is contained in
		 * this set.  This ensures that the <tt>equals</tt> method works
		 * properly across different implementations of the <tt>skiplist.Set</tt>
		 * interface.<p>
		 *
		 * This implementation first checks if the specified object is this
		 * set; if so it returns <tt>true</tt>.  Then, it checks if the
		 * specified object is a set whose size is identical to the size of
		 * this set; if not, it returns false.  If so, it returns
		 * <tt>containsAll((skiplist.Collection) o)</tt>.
		 *
		 * @param o object to be compared for equality with this set
		 * @return <tt>true</tt> if the specified object is equal to this set
		 */
	public:
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
			if (c->size() != AbstractCollection<E>::size())
			{
				return false;
			}
			try
			{
				return AbstractCollection<E>::containsAll(c);
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

		
		/**
		 * Removes from this set all of its elements that are contained in the
		 * specified collection (optional operation).  If the specified
		 * collection is also a set, this operation effectively modifies this
		 * set so that its value is the <i>asymmetric set difference</i> of
		 * the two sets.
		 *
		 * <p>This implementation determines which is the smaller of this set
		 * and the specified collection, by invoking the <tt>size</tt>
		 * method on each.  If this set has fewer elements, then the
		 * implementation iterates over this set, checking each element
		 * returned by the beginIterator in turn to see if it is contained in
		 * the specified collection.  If it is so contained, it is removed
		 * from this set with the beginIterator's <tt>remove</tt> method.  If
		 * the specified collection has fewer elements, then the
		 * implementation iterates over the specified collection, removing
		 * from this set each element returned by the beginIterator, using this
		 * set's <tt>remove</tt> method.
		 *
		 * <p>Note that this implementation will throw an
		 * <tt>UnsupportedOperationException</tt> if the beginIterator returned by the
		 * <tt>beginIterator</tt> method does not implement the <tt>remove</tt> method.
		 *
		 * @param  c collection containing elements to be removed from this set
		 * @return <tt>true</tt> if this set changed as a result of the call
		 * @throws UnsupportedOperationException if the <tt>removeAll</tt> operation
		 *         is not supported by this set
		 * @throws ClassCastException if the class of an element of this set
		 *         is incompatible with the specified collection (optional)
		 * @throws NullPointerException if this set contains a null element and the
		 *         specified collection does not permit null elements (optional),
		 *         or if the specified collection is null
		 * @see #remove(Object)
		 * @see #contains(Object)
		 */
		virtual bool removeAll(Collection<E> *c)
		{
			bool modified = false;

			if (AbstractCollection<E>::size() > c->size())
			{
				for (Iterator<E> *i = c->beginIterator(); i->hasNext();)
				{
					modified |= AbstractCollection<E>::remove((void*)i->nextEntry());
				}
			}
			else
			{
				for (Iterator<E> *i = AbstractCollection<E>::beginIterator(); i->hasNext();)
				{
					if (c->contains(i->nextEntry()))
					{
						i->remove();
						modified = true;
					}
				}
			}
			return modified;
		}

	};

}
