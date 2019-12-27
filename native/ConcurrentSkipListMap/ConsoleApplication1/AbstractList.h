#pragma once

#include "AbstractCollection.h"
#include "List.h"
#include "ListIterator.h"
#include "Collection.h"
#include "Iterator.h"
#include <string>
#include <stdexcept>
#include "exceptionhelper.h"

/*
 * Copyright (c) 1997, 2007, Oracle and/or its affiliates. All rights reserved.
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

namespace skiplist
{
	template<typename E> class RandomAccessSubList;

	/**
	 * This class provides a skeletal implementation of the {@link List}
	 * interface to minimize the effort required to implement this interface
	 * backed by a "random access" data store (such as an array).  For sequential
	 * access data (such as a linked list), {@link AbstractSequentialList} should
	 * be used in preference to this class.
	 *
	 * <p>To implement an unmodifiable list, the programmer needs only to extend
	 * this class and provide implementations for the {@link #get(int)} and
	 * {@link List#size() size()} methods.
	 *
	 * <p>To implement a modifiable list, the programmer must additionally
	 * override the {@link #set(int, Object) set(int, E)} method (which otherwise
	 * throws an {@code UnsupportedOperationException}).  If the list is
	 * variable-size the programmer must additionally override the
	 * {@link #add(int, Object) add(int, E)} and {@link #remove(int)} methods.
	 *
	 * <p>The programmer should generally provide a void (no argument) and collection
	 * constructor, as per the recommendation in the {@link Collection} interface
	 * specification.
	 *
	 * <p>Unlike the other abstract collection implementations, the programmer does
	 * <i>not</i> have to provide an beginIterator implementation; the beginIterator and
	 * list beginIterator are implemented by this class, on top of the "random access"
	 * methods:
	 * {@link #get(int)},
	 * {@link #set(int, Object) set(int, E)},
	 * {@link #add(int, Object) add(int, E)} and
	 * {@link #remove(int)}.
	 *
	 * <p>The documentation for each non-abstract method in this class describes its
	 * implementation in detail.  Each of these methods may be overridden if the
	 * collection being implemented admits a more efficient implementation.
	 *
	 * <p>This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @since 1.2
	 */

	template<typename E>
	class AbstractList : public AbstractCollection<E>, public List<E>
	{
		/**
		 * Sole constructor.  (For invocation by subclass constructors, typically
		 * implicit.)
		 */
	protected:
		AbstractList()
		{
		}

		/**
		 * Appends the specified element to the end of this list (optional
		 * operation).
		 *
		 * <p>Lists that support this operation may place limitations on what
		 * elements may be added to this list.  In particular, some
		 * lists will refuse to add null elements, and others will impose
		 * restrictions on the type of elements that may be added.  List
		 * classes should clearly specify in their documentation any restrictions
		 * on what elements may be added.
		 *
		 * <p>This implementation calls {@code add(size(), e)}.
		 *
		 * <p>Note that this implementation throws an
		 * {@code UnsupportedOperationException} unless
		 * {@link #add(int, Object) add(int, E)} is overridden.
		 *
		 * @param e element to be appended to this list
		 * @return {@code true} (as specified by {@link Collection#add})
		 * @throws UnsupportedOperationException if the {@code add} operation
		 *         is not supported by this list
		 * @throws ClassCastException if the class of the specified element
		 *         prevents it from being added to this list
		 * @throws NullPointerException if the specified element is null and this
		 *         list does not permit null elements
		 * @throws IllegalArgumentException if some property of this element
		 *         prevents it from being added to this list
		 */
	public:
		virtual bool add(E e)
		{
			add(AbstractCollection<E>::size(), e);
			return true;
		}

		/**
		 * {@inheritDoc}
		 *
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual E get(int index) = 0;

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation always throws an
		 * {@code UnsupportedOperationException}.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 * @throws IndexOutOfBoundsException     {@inheritDoc}
		 */
		virtual E set(int index, E element)
		{
			throw UnsupportedOperationException();
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation always throws an
		 * {@code UnsupportedOperationException}.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 * @throws IndexOutOfBoundsException     {@inheritDoc}
		 */
		virtual void add(int index, E element)
		{
			throw UnsupportedOperationException();
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation always throws an
		 * {@code UnsupportedOperationException}.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws IndexOutOfBoundsException     {@inheritDoc}
		 */
		virtual E remove(int index)
		{
			throw UnsupportedOperationException();
		}


		// Search Operations

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation first gets a list beginIterator (with
		 * {@code listIterator()}).  Then, it iterates over the list until the
		 * specified element is found or the end of the list is reached.
		 *
		 * @throws ClassCastException   {@inheritDoc}
		 * @throws NullPointerException {@inheritDoc}
		 */
		virtual int indexOf(E o)
		{
			ListIterator<E> *e = listIterator();
			if (o == NULL)
			{
				while (e->hasNext())
				{
					if (e->nextEntry() == NULL)
					{
						return e->previousIndex();
					}
					e++;
				}
			}
			else
			{
				while (e->hasNext())
				{
					if (o->equals(e->nextEntry()))
					{
						return e->previousIndex();
					}
					e++;
				}
			}
			return -1;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation first gets a list beginIterator that points to the end
		 * of the list (with {@code listIterator(size())}).  Then, it iterates
		 * backwards over the list until the specified element is found, or the
		 * beginning of the list is reached.
		 *
		 * @throws ClassCastException   {@inheritDoc}
		 * @throws NullPointerException {@inheritDoc}
		 */
		virtual int lastIndexOf(E o)
		{
			ListIterator<E> *e = listIterator(AbstractCollection<E>::size());
			if (o == NULL)
			{
				while (e->hasPrevious())
				{
					if (e->previous() == NULL)
					{
						return e->nextIndex();
					}
				}
			}
			else
			{
				while (e->hasPrevious())
				{
					if (o->equals(e->previous()))
					{
						return e->nextIndex();
					}
				}
			}
			return -1;
		}


		// Bulk Operations

		/**
		 * Removes all of the elements from this list (optional operation).
		 * The list will be empty after this call returns.
		 *
		 * <p>This implementation calls {@code removeRange(0, size())}.
		 *
		 * <p>Note that this implementation throws an
		 * {@code UnsupportedOperationException} unless {@code remove(int
		 * index)} or {@code removeRange(int fromIndex, int toIndex)} is
		 * overridden.
		 *
		 * @throws UnsupportedOperationException if the {@code clear} operation
		 *         is not supported by this list
		 */
		virtual void clear()
		{
			removeRange(0, AbstractCollection<E>::size());
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation gets an beginIterator over the specified collection
		 * and iterates over it, inserting the elements obtained from the
		 * beginIterator into this list at the appropriate position, one at a time,
		 * using {@code add(int, E)}.
		 * Many implementations will override this method for efficiency.
		 *
		 * <p>Note that this implementation throws an
		 * {@code UnsupportedOperationException} unless
		 * {@link #add(int, Object) add(int, E)} is overridden.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 * @throws IndexOutOfBoundsException     {@inheritDoc}
		 */
		template<typename T1>
//JAVA TO C++ CONVERTER TODO TASK: There is no C++ template equivalent to this generic constraint:
//ORIGINAL LINE: public boolean addAll(int index, Collection<? extends E> c)
		bool addAll(int index, Collection<T1> *c)
		{
			rangeCheckForAdd(index);
			bool modified = false;
//JAVA TO C++ CONVERTER TODO TASK: Java wildcard generics are not converted to C++:
//ORIGINAL LINE: Iterator<? extends E> e = c.beginIterator();
			Iterator<E> *e = c->beginIterator();
			while (e->hasNext())
			{
				add(index++, e->nextEntry());
				modified = true;
				e++;
			}
			return modified;
		}


		// Iterators

		/**
		 * Returns an beginIterator over the elements in this list in proper sequence.
		 *
		 * <p>This implementation returns a straightforward implementation of the
		 * beginIterator interface, relying on the backing list's {@code size()},
		 * {@code get(int)}, and {@code remove(int)} methods.
		 *
		 * <p>Note that the beginIterator returned by this method will throw an
		 * {@link UnsupportedOperationException} in response to its
		 * {@code remove} method unless the list's {@code remove(int)} method is
		 * overridden.
		 *
		 * <p>This implementation can be made to throw runtime exceptions in the
		 * face of concurrent modification, as described in the specification
		 * for the (protected) {@link #modCount} field.
		 *
		 * @return an beginIterator over the elements in this list in proper sequence
		 */
		virtual Iterator<E> *beginIterator()
		{
			return new Itr(this);
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns {@code listIterator(0)}.
		 *
		 * @see #listIterator(int)
		 */
		virtual ListIterator<E> *listIterator()
		{
			return listIterator(0);
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns a straightforward implementation of the
		 * {@code ListIterator} interface that extends the implementation of the
		 * {@code Iterator} interface returned by the {@code beginIterator()} method.
		 * The {@code ListIterator} implementation relies on the backing list's
		 * {@code get(int)}, {@code set(int, E)}, {@code add(int, E)}
		 * and {@code remove(int)} methods.
		 *
		 * <p>Note that the list beginIterator returned by this implementation will
		 * throw an {@link UnsupportedOperationException} in response to its
		 * {@code remove}, {@code set} and {@code add} methods unless the
		 * list's {@code remove(int)}, {@code set(int, E)}, and
		 * {@code add(int, E)} methods are overridden.
		 *
		 * <p>This implementation can be made to throw runtime exceptions in the
		 * face of concurrent modification, as described in the specification for
		 * the (protected) {@link #modCount} field.
		 *
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual ListIterator<E> *listIterator(int const index)
		{
			rangeCheckForAdd(index);

			return new ListItr(this, index);
		}

	private:
		class Itr : public Iterator<E>
		{
		private:
			AbstractList<E> *outerInstance;

		public:
			Itr(AbstractList<E> *outerInstance) : outerInstance(outerInstance)
			{
			}

			/**
			 * Index of element to be returned by subsequent call to next.
			 */
			int cursor = 0;

			/**
			 * Index of element returned by most recent call to next or
			 * previous.  Reset to -1 if this element is deleted by a call
			 * to remove.
			 */
			int lastRet = -1;

			/**
			 * The modCount value that the beginIterator believes that the backing
			 * List should have.  If this expectation is violated, the beginIterator
			 * has detected concurrent modification.
			 */
			int expectedModCount = outerInstance->modCount;

			virtual bool hasNext()
			{
				return cursor != ((AbstractCollection<E>*)outerInstance)->size();
			}

			virtual E nextEntry()
			{
				checkForComodification();
				try
				{
					int i = cursor;
					E next = outerInstance->get(i);
					lastRet = i;
					cursor = i + 1;
					return next;
				}
				catch (const std::out_of_range &e)
				{
					checkForComodification();
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			virtual void remove()
			{
				if (lastRet < 0)
				{
					throw IllegalStateException();
				}
				checkForComodification();

				try
				{
					outerInstance->remove(lastRet);
					if (lastRet < cursor)
					{
						cursor--;
					}
					lastRet = -1;
					expectedModCount = outerInstance->modCount;
				}
				catch (const std::out_of_range &e)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			virtual void checkForComodification()
			{
				if (outerInstance->modCount != expectedModCount)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}
		};

	private:
		class ListItr : public Itr, public ListIterator<E>
		{
		private:
			AbstractList<E> *outerInstance;

		public:
			ListItr(AbstractList<E> *outerInstance, int index) : Itr(outerInstance), outerInstance(outerInstance)
			{
				Itr::cursor = index;
			}

			virtual bool hasPrevious()
			{
				return Itr::cursor != 0;
			}

			virtual E previous()
			{
				Itr::checkForComodification();
				try
				{
					int i = Itr::cursor - 1;
					E previous = outerInstance->get(i);
					Itr::lastRet = Itr::cursor = i;
					return previous;
				}
				catch (const std::out_of_range &e)
				{
					Itr::checkForComodification();
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			virtual int nextIndex()
			{
				return Itr::cursor;
			}

			virtual int previousIndex()
			{
				return Itr::cursor - 1;
			}

			virtual void set(E e)
			{
				if (Itr::lastRet < 0)
				{
					throw IllegalStateException();
				}
				Itr::checkForComodification();

				try
				{
					outerInstance->set(Itr::lastRet, e);
					Itr::expectedModCount = outerInstance->modCount;
				}
				catch (const std::out_of_range &ex)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			virtual void add(E e)
			{
				Itr::checkForComodification();

				try
				{
					int i = Itr::cursor;
					outerInstance->add(i, e);
					Itr::lastRet = -1;
					Itr::cursor = i + 1;
					Itr::expectedModCount = outerInstance->modCount;
				}
				catch (const std::out_of_range &ex)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}
		};

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns a list that subclasses
		 * {@code AbstractList}.  The subclass stores, in private fields, the
		 * offset of the subList within the backing list, the size of the subList
		 * (which can change over its lifetime), and the expected
		 * {@code modCount} value of the backing list.  There are two variants
		 * of the subclass, one of which implements {@code RandomAccess}.
		 * If this list implements {@code RandomAccess} the returned list will
		 * be an instance of the subclass that implements {@code RandomAccess}.
		 *
		 * <p>The subclass's {@code set(int, E)}, {@code get(int)},
		 * {@code add(int, E)}, {@code remove(int)}, {@code addAll(int,
		 * Collection)} and {@code removeRange(int, int)} methods all
		 * delegate to the corresponding methods on the backing abstract list,
		 * after bounds-checking the index and adjusting for the offset.  The
		 * {@code addAll(Collection c)} method merely returns {@code addAll(size,
		 * c)}.
		 *
		 * <p>The {@code listIterator(int)} method returns a "wrapper object"
		 * over a list beginIterator on the backing list, which is created with the
		 * corresponding method on the backing list.  The {@code beginIterator} method
		 * merely returns {@code listIterator()}, and the {@code size} method
		 * merely returns the subclass's {@code size} field.
		 *
		 * <p>All methods first check to see if the actual {@code modCount} of
		 * the backing list is equal to its expected value, and throw a
		 * {@code ConcurrentModificationException} if it is not.
		 *
		 * @throws IndexOutOfBoundsException if an endpoint index value is out of range
		 *         {@code (fromIndex < 0 || toIndex > size)}
		 * @throws IllegalArgumentException if the endpoint indices are out of order
		 *         {@code (fromIndex > toIndex)}
		 */
	public:
		virtual List<E> *subList(int fromIndex, int toIndex)
		{
			RandomAccessSubList<E> tempVar(this, fromIndex, toIndex);
			return (&tempVar);
		}

		// Comparison and hashing

		/**
		 * Compares the specified object with this list for equality.  Returns
		 * {@code true} if and only if the specified object is also a list, both
		 * lists have the same size, and all corresponding pairs of elements in
		 * the two lists are <i>equal</i>.  (Two elements {@code e1} and
		 * {@code e2} are <i>equal</i> if {@code (e1==null ? e2==null :
		 * e1.equals(e2))}.)  In other words, two lists are defined to be
		 * equal if they contain the same elements in the same order.<p>
		 *
		 * This implementation first checks if the specified object is this
		 * list. If so, it returns {@code true}; if not, it checks if the
		 * specified object is a list. If not, it returns {@code false}; if so,
		 * it iterates over both lists, comparing corresponding pairs of elements.
		 * If any comparison returns {@code false}, this method returns
		 * {@code false}.  If either beginIterator runs out of elements before the
		 * other it returns {@code false} (as the lists are of unequal length);
		 * otherwise it returns {@code true} when the iterations complete.
		 *
		 * @param o the object to be compared for equality with this list
		 * @return {@code true} if the specified object is equal to this list
		 */
		virtual bool equals(void *o)
		{
			if (o == this)
			{
				return true;
			}
			if (!(static_cast<List<E>*>(o) != NULL))
			{
				return false;
			}

			ListIterator<E> *e1 = listIterator();
			ListIterator<E> *e2 = (ListIterator<E>*)(static_cast<List<E>*>(o))->beginIterator();
			while (e1->hasNext() && e2->hasNext())
			{
				E o1 = e1->nextEntry();
				auto o2 = e2->nextEntry();
				if (!(o1 == NULL ? o2 == NULL : o1->equals(o2)))
				{
					return false;
				}
			}
			return !(e1->hasNext() || e2->hasNext());
		}

		/**
		 * Returns the hash code value for this list.
		 *
		 * <p>This implementation uses exactly the code that is used to define the
		 * list hash function in the documentation for the {@link List#hashCode}
		 * method.
		 *
		 * @return the hash code value for this list
		 */
		virtual int hashCode()
		{
			int hashCode = 1;
			for (Iterator<E> *i = this->beginIterator(); i->hasNext();)
			{
				E e = i->nextEntry();
				hashCode = 31 * hashCode + (e == NULL ? 0 : e->hashCode());
			}
			return hashCode;
		}

		/**
		 * Removes from this list all of the elements whose index is between
		 * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.
		 * Shifts any succeeding elements to the left (reduces their index).
		 * This call shortens the list by {@code (toIndex - fromIndex)} elements.
		 * (If {@code toIndex==fromIndex}, this operation has no effect.)
		 *
		 * <p>This method is called by the {@code clear} operation on this list
		 * and its subLists.  Overriding this method to take advantage of
		 * the internals of the list implementation can <i>substantially</i>
		 * improve the performance of the {@code clear} operation on this list
		 * and its subLists.
		 *
		 * <p>This implementation gets a list beginIterator positioned before
		 * {@code fromIndex}, and repeatedly calls {@code ListIterator.next}
		 * followed by {@code ListIterator.remove} until the entire range has
		 * been removed.  <b>Note: if {@code ListIterator.remove} requires linear
		 * time, this implementation requires quadratic time.</b>
		 *
		 * @param fromIndex index of first element to be removed
		 * @param toIndex index after last element to be removed
		 */
	public:
		virtual void removeRange(int fromIndex, int toIndex)
		{
			ListIterator<E> *it = listIterator(fromIndex);
			for (int i = 0, n = toIndex - fromIndex; i < n; i++)
			{
				it->nextEntry();
				it->remove();
			}
		}

		/**
		 * The number of times this list has been <i>structurally modified</i>.
		 * Structural modifications are those that change the size of the
		 * list, or otherwise perturb it in such a fashion that iterations in
		 * progress may yield incorrect results.
		 *
		 * <p>This field is used by the beginIterator and list beginIterator implementation
		 * returned by the {@code beginIterator} and {@code listIterator} methods.
		 * If the value of this field changes unexpectedly, the beginIterator (or list
		 * beginIterator) will throw a {@code ConcurrentModificationException} in
		 * response to the {@code next}, {@code remove}, {@code previous},
		 * {@code set} or {@code add} operations.  This provides
		 * <i>fail-fast</i> behavior, rather than non-deterministic behavior in
		 * the face of concurrent modification during iteration.
		 *
		 * <p><b>Use of this field by subclasses is optional.</b> If a subclass
		 * wishes to provide fail-fast iterators (and list iterators), then it
		 * merely has to increment this field in its {@code add(int, E)} and
		 * {@code remove(int)} methods (and any other methods that it overrides
		 * that result in structural modifications to the list).  A single call to
		 * {@code add(int, E)} or {@code remove(int)} must add no more than
		 * one to this field, or the iterators (and list iterators) will throw
		 * bogus {@code ConcurrentModificationExceptions}.  If an implementation
		 * does not wish to provide fail-fast iterators, this field may be
		 * ignored.
		 */
	public:
		int modCount = 0;

	private:
		void rangeCheckForAdd(int index)
		{
			if (index < 0 || index > AbstractCollection<E>::size())
			{
				throw std::out_of_range("error");
			}
		}

		std::wstring outOfBoundsMsg(int index)
		{
			return L"Index: " + std::to_wstring(index) + L", Size: " + std::to_wstring(AbstractCollection<E>::size());
		}
	};

	template<typename E>
	class SubList : public AbstractList<E>
	{
	private:
		AbstractList<E> *const l;
		const int offset;
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		int size_Conflict = 0;

	public:
		SubList(AbstractList<E> *list, int fromIndex, int toIndex) : l(list), offset(fromIndex)
		{
			if (fromIndex < 0)
			{
				throw std::out_of_range("error");
			}
			if (toIndex > ((AbstractCollection<E>*)list)->size())
			{
				throw std::out_of_range("error");
			}
			if (fromIndex > toIndex)
			{
				throw std::invalid_argument("error");
			}
			size_Conflict = toIndex - fromIndex;
			this->modCount = l->modCount;
		}

		virtual E set(int index, E element)
		{
			rangeCheck(index);
			checkForComodification();
			return l->set(index + offset, element);
		}

		virtual E get(int index)
		{
			rangeCheck(index);
			checkForComodification();
			return l->get(index + offset);
		}

		virtual int size()
		{
			checkForComodification();
			return size_Conflict;
		}

		virtual void add(int index, E element)
		{
			rangeCheckForAdd(index);
			checkForComodification();
			l->add(index + offset, element);
			this->modCount = l->modCount;
			size_Conflict++;
		}

		virtual E remove(int index)
		{
			rangeCheck(index);
			checkForComodification();
			E result = l->remove(index + offset);
			this->modCount = l->modCount;
			size_Conflict--;
			return result;
		}

	protected:
		virtual void removeRange(int fromIndex, int toIndex)
		{
			checkForComodification();
			l->removeRange(fromIndex + offset, toIndex + offset);
			this->modCount = l->modCount;
			size_Conflict -= (toIndex - fromIndex);
		}

	public:
		virtual bool addAll(Collection<E> *c)
		{
			return addAll(size_Conflict, c);
		}

		virtual bool addAll(int index, Collection<E> *c)
		{
			rangeCheckForAdd(index);
			int cSize = c->size();
			if (cSize == 0)
			{
				return false;
			}

			checkForComodification();
			l->addAll(offset + index, c);
			this->modCount = l->modCount;
			size_Conflict += cSize;
			return true;
		}

		virtual Iterator<E> *beginIterator()
		{
			return listIterator(0);
		}

		virtual ListIterator<E> *listIterator(int const index)
		{
			checkForComodification();
			rangeCheckForAdd(index);

			return new ListIteratorAnonymousInnerClass(this, index);
		}

	private:
		class ListIteratorAnonymousInnerClass : public ListIterator<E>
		{
		private:
			SubList<E> *outerInstance;

			int index = 0;

		public:
			ListIteratorAnonymousInnerClass(SubList<E> *outerInstance, int index)
			{
				this->outerInstance = outerInstance;
				this->index = index;
				i = outerInstance->l->listIterator(index + outerInstance->offset);
			}

		private:
			ListIterator<E> *i;

		public:
			bool hasNext()
			{
				return nextIndex() < outerInstance->size();
			}

			E nextEntry()
			{
				if (hasNext())
				{
					return i->nextEntry();
				}
				else
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			bool hasPrevious()
			{
				return previousIndex() >= 0;
			}

			E previous()
			{
				if (hasPrevious())
				{
					return i->previous();
				}
				else
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}

			int nextIndex()
			{
				return i->nextIndex() - outerInstance->offset;
			}

			int previousIndex()
			{
				return i->previousIndex() - outerInstance->offset;
			}

			void remove()
			{
				i->remove();
				outerInstance->modCount = outerInstance->l->modCount;
				outerInstance->size_Conflict--;
			}

			void set(E e)
			{
				i->set(e);
			}

			void add(E e)
			{
				i->add(e);
				outerInstance->modCount = outerInstance->l->modCount;
				outerInstance->size_Conflict++;
			}
		};

	public:
		virtual List<E> *subList(int fromIndex, int toIndex)
		{
			return new SubList<E>(this, fromIndex, toIndex);
		}

	private:
		void rangeCheck(int index)
		{
			if (index < 0 || index >= size_Conflict)
			{
				throw std::out_of_range("error");
			}
		}

		void rangeCheckForAdd(int index)
		{
			if (index < 0 || index > size_Conflict)
			{
				throw std::out_of_range("error");
			}
		}

		std::wstring outOfBoundsMsg(int index)
		{
			return L"Index: " + std::to_wstring(index) + L", Size: " + std::to_wstring(size_Conflict);
		}

		void checkForComodification()
		{
			if (this->modCount != l->modCount)
			{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
				throw std::runtime_error("error");
			}
		}
	};

	template<typename E>
	class RandomAccessSubList : public SubList<E>
	{
	public:
		RandomAccessSubList(AbstractList<E> *list, int fromIndex, int toIndex) : SubList<E>(list, fromIndex, toIndex)
		{
		}

		virtual List<E> *subList(int fromIndex, int toIndex)
		{
			return new RandomAccessSubList<E>(this, fromIndex, toIndex);
		}
	};

}
