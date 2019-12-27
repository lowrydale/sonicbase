#pragma once

#include "AbstractList.h"
#include "List.h"
#include "Collection.h"
#include "ListIterator.h"
#include "Iterator.h"
#include <string>
#include <stdexcept>
#include "exceptionhelper.h"
#include <atomic>

namespace skiplist
{

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

	/**
	 * Resizable-array implementation of the <tt>List</tt> interface.  Implements
	 * all optional list operations, and permits all elements, including
	 * <tt>null</tt>.  In addition to implementing the <tt>List</tt> interface,
	 * this class provides methods to manipulate the size of the array that is
	 * used internally to store the list.  (This class is roughly equivalent to
	 * <tt>Vector</tt>, except that it is unsynchronized.)
	 *
	 * <p>The <tt>size</tt>, <tt>isEmpty</tt>, <tt>get</tt>, <tt>set</tt>,
	 * <tt>beginIterator</tt>, and <tt>listIterator</tt> operations run in constant
	 * time.  The <tt>add</tt> operation runs in <i>amortized constant time</i>,
	 * that is, adding n elements requires O(n) time.  All of the other operations
	 * run in linear time (roughly speaking).  The constant factor is low compared
	 * to that for the <tt>LinkedList</tt> implementation.
	 *
	 * <p>Each <tt>skiplist.ArrayList</tt> instance has a <i>capacity</i>.  The capacity is
	 * the size of the array used to store the elements in the list.  It is always
	 * at least as large as the list size.  As elements are added to an skiplist.ArrayList,
	 * its capacity grows automatically.  The details of the growth policy are not
	 * specified beyond the fact that adding an element has constant amortized
	 * time cost.
	 *
	 * <p>An application can increase the capacity of an <tt>skiplist.ArrayList</tt> instance
	 * before adding a large number of elements using the <tt>ensureCapacity</tt>
	 * operation.  This may reduce the amount of incremental reallocation.
	 *
	 * <p><strong>Note that this implementation is not synchronized.</strong>
	 * If multiple threads access an <tt>skiplist.ArrayList</tt> instance concurrently,
	 * and at least one of the threads modifies the list structurally, it
	 * <i>must</i> be synchronized externally.  (A structural modification is
	 * any operation that adds or deletes one or more elements, or explicitly
	 * resizes the backing array; merely setting the value of an element is not
	 * a structural modification.)  This is typically accomplished by
	 * synchronizing on some object that naturally encapsulates the list.
	 *
	 * If no such object exists, the list should be "wrapped" using the
	 * {@link Collections#synchronizedList Collections.synchronizedList}
	 * method.  This is best done at creation time, to prevent accidental
	 * unsynchronized access to the list:<pre>
	 *   List list = Collections.synchronizedList(new skiplist.ArrayList(...));</pre>
	 *
	 * <p><a name="fail-fast"/>
	 * The iterators returned by this class's {@link #beginIterator() beginIterator} and
	 * {@link #listIterator(int) listIterator} methods are <em>fail-fast</em>:
	 * if the list is structurally modified at any time after the beginIterator is
	 * created, in any way except through the beginIterator's own
	 * {@link ListIterator#remove() remove} or
	 * {@link ListIterator#add(Object) add} methods, the beginIterator will throw a
	 * {@link ConcurrentModificationException}.  Thus, in the face of
	 * concurrent modification, the beginIterator fails quickly and cleanly, rather
	 * than risking arbitrary, non-deterministic behavior at an undetermined
	 * time in the future.
	 *
	 * <p>Note that the fail-fast behavior of an beginIterator cannot be guaranteed
	 * as it is, generally speaking, impossible to make any hard guarantees in the
	 * presence of unsynchronized concurrent modification.  Fail-fast iterators
	 * throw {@code ConcurrentModificationException} on a best-effort basis.
	 * Therefore, it would be wrong to write a program that depended on this
	 * exception for its correctness:  <i>the fail-fast behavior of iterators
	 * should be used only to detect bugs.</i>
	 *
	 * <p>This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @see     Collection
	 * @see     List
	 * @see     LinkedList
	 * @see     Vector
	 * @since   1.2
	 */

	template<typename E>
	class ArrayList : public AbstractList<E>, public List<E>
	{
	private:
		static constexpr long long serialVersionUID = 8683452581122892189LL;

		/**
		 * The array buffer into which the elements of the skiplist.ArrayList are stored.
		 * The capacity of the skiplist.ArrayList is the length of this array buffer.
		 */
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		E *elementData_Conflict;
		int elementDataLen;

		/**
		 * The size of the skiplist.ArrayList (the number of elements it contains).
		 *
		 * @serial
		 */
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		int size_Conflict = 0;

		/**
		 * Constructs an empty list with the specified initial capacity.
		 *
		 * @param   initialCapacity   the initial capacity of the list
		 * @exception IllegalArgumentException if the specified initial capacity
		 *            is negative
		 */
	public:
		ArrayList(int initialCapacity) : AbstractList<E>()
		{
			if (initialCapacity < 0)
			{
				throw std::invalid_argument("error");
			}
			this->elementData_Conflict = new E[initialCapacity];
			this->elementDataLen = initialCapacity;
		}

		/**
		 * Constructs an empty list with an initial capacity of ten.
		 */
		ArrayList() : ArrayList(10)
		{
		}

		/**
		 * Increases the capacity of this <tt>skiplist.ArrayList</tt> instance, if
		 * necessary, to ensure that it can hold at least the number of elements
		 * specified by the minimum capacity argument.
		 *
		 * @param   minCapacity   the desired minimum capacity
		 */
		virtual void ensureCapacity(int minCapacity)
		{
			AbstractList<E>::modCount++;
			int oldCapacity = elementDataLen;
			if (minCapacity > oldCapacity)
			{
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Object oldData[] = elementData;
				E *oldData = elementData_Conflict;
				int newCapacity = (oldCapacity * 3) / 2 + 1;
				if (newCapacity < minCapacity)
				{
					newCapacity = minCapacity;
				}
				// minCapacity is usually close to size, so this is a win:
				E *tmp = new E[newCapacity];
				memcpy(elementData_Conflict, tmp, newCapacity * sizeof(E));
				elementData_Conflict = tmp;
				elementDataLen = newCapacity;
			}
		}

		/**
		 * Returns the number of elements in this list.
		 *
		 * @return the number of elements in this list
		 */
		virtual int size()
		{
			return size_Conflict;
		}

		/**
		 * Returns <tt>true</tt> if this list contains no elements.
		 *
		 * @return <tt>true</tt> if this list contains no elements
		 */
		virtual bool isEmpty()
		{
			return size_Conflict == 0;
		}

		/**
		 * Returns <tt>true</tt> if this list contains the specified element.
		 * More formally, returns <tt>true</tt> if and only if this list contains
		 * at least one element <tt>e</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
		 *
		 * @param o element whose presence in this list is to be tested
		 * @return <tt>true</tt> if this list contains the specified element
		 */
		virtual bool contains(E o)
		{
			return indexOf(o) >= 0;
		}

		/**
		 * Returns the index of the first occurrence of the specified element
		 * in this list, or -1 if this list does not contain the element.
		 * More formally, returns the lowest index <tt>i</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
		 * or -1 if there is no such index.
		 */
		virtual int indexOf(E o)
		{
			if (o == NULL)
			{
				for (int i = 0; i < size_Conflict; i++)
				{
					if (elementData_Conflict[i] == NULL)
					{
						return i;
					}
				}
			}
			else
			{
				for (int i = 0; i < size_Conflict; i++)
				{
					if (o->equals(elementData_Conflict[i]))
					{
						return i;
					}
				}
			}
			return -1;
		}

		/**
		 * Returns the index of the last occurrence of the specified element
		 * in this list, or -1 if this list does not contain the element.
		 * More formally, returns the highest index <tt>i</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
		 * or -1 if there is no such index.
		 */
		virtual int lastIndexOf(E o)
		{
			if (o == NULL)
			{
				for (int i = size_Conflict - 1; i >= 0; i--)
				{
					if (elementData_Conflict[i] == NULL)
					{
						return i;
					}
				}
			}
			else
			{
				for (int i = size_Conflict - 1; i >= 0; i--)
				{
					if (o->equals(elementData_Conflict[i]))
					{
						return i;
					}
				}
			}
			return -1;
		}

		// Positional Access Operations

//JAVA TO C++ CONVERTER TODO TASK: Most Java annotations will not have direct C++ equivalents:
//ORIGINAL LINE: @SuppressWarnings("unchecked") E elementData(int index)
		virtual E getElementData(int index)
		{
			return static_cast<E>(elementData_Conflict[index]);
		}

		/**
		 * Returns the element at the specified position in this list.
		 *
		 * @param  index index of the element to return
		 * @return the element at the specified position in this list
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual E get(int index)
		{
			rangeCheck(index);

			return getElementData(index);
		}

		/**
		 * Replaces the element at the specified position in this list with
		 * the specified element.
		 *
		 * @param index index of the element to replace
		 * @param element element to be stored at the specified position
		 * @return the element previously at the specified position
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual E set(int index, E element)
		{
			rangeCheck(index);

			E oldValue = getElementData(index);
			elementData_Conflict[index] = element;
			return oldValue;
		}

		/**
		 * Appends the specified element to the end of this list.
		 *
		 * @param e element to be appended to this list
		 * @return <tt>true</tt> (as specified by {@link Collection#add})
		 */
		virtual bool add(E e)
		{
			ensureCapacity(size_Conflict + 1); // Increments modCount!!
			elementData_Conflict[size_Conflict++] = e;
			return true;
		}

		/**
		 * Inserts the specified element at the specified position in this
		 * list. Shifts the element currently at that position (if any) and
		 * any subsequent elements to the right (adds one to their indices).
		 *
		 * @param index index at which the specified element is to be inserted
		 * @param element element to be inserted
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual void add(int index, E element)
		{
			rangeCheckForAdd(index);

			ensureCapacity(size_Conflict + 1); // Increments modCount!!
			memcpy(elementData_Conflict + index, elementData_Conflict + index + 1, size_Conflict - index);
			elementData_Conflict[index] = element;
			size_Conflict++;
		}

		/**
		 * Removes the element at the specified position in this list.
		 * Shifts any subsequent elements to the left (subtracts one from their
		 * indices).
		 *
		 * @param index the index of the element to be removed
		 * @return the element that was removed from the list
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
		virtual E remove(int index)
		{
			rangeCheck(index);

			AbstractList<E>::modCount++;
			E oldValue = getElementData(index);

			int numMoved = size_Conflict - index - 1;
			if (numMoved > 0)
			{
				memcpy(elementData_Conflict + index + 1, elementData_Conflict + index, numMoved);
			}
			elementData_Conflict[--size_Conflict] = NULL; // Let gc do its work

			return oldValue;
		}

		/**
		 * Removes the first occurrence of the specified element from this list,
		 * if it is present.  If the list does not contain the element, it is
		 * unchanged.  More formally, removes the element with the lowest index
		 * <tt>i</tt> such that
		 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
		 * (if such an element exists).  Returns <tt>true</tt> if this list
		 * contained the specified element (or equivalently, if this list
		 * changed as a result of the call).
		 *
		 * @param o element to be removed from this list, if present
		 * @return <tt>true</tt> if this list contained the specified element
		 */
		virtual bool remove(E o)
		{
			if (o == NULL)
			{
				for (int index = 0; index < size_Conflict; index++)
				{
					if (elementData_Conflict[index] == NULL)
					{
						fastRemove(index);
						return true;
					}
				}
			}
			else
			{
				for (int index = 0; index < size_Conflict; index++)
				{
					if (o->equals(elementData_Conflict[index]))
					{
						fastRemove(index);
						return true;
					}
				}
			}
			return false;
		}

		/*
		 * Private remove method that skips bounds checking and does not
		 * return the value removed.
		 */
	private:
		void fastRemove(int index)
		{
			AbstractList<E>::modCount++;
			int numMoved = size_Conflict - index - 1;
			if (numMoved > 0)
			{
				memcpy(elementData_Conflict + index + 1, elementData_Conflict + index, numMoved * sizeof(E));
			}
			elementData_Conflict[--size_Conflict] = NULL; // Let gc do its work
		}

		/**
		 * Removes all of the elements from this list.  The list will
		 * be empty after this call returns.
		 */
	public:
		virtual void clear()
		{
			AbstractList<E>::modCount++;

			// Let gc do its work
			for (int i = 0; i < size_Conflict; i++)
			{
				elementData_Conflict[i] = NULL;
			}

			size_Conflict = 0;
		}

		/**
		 * Removes from this list all of the elements whose index is between
		 * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.
		 * Shifts any succeeding elements to the left (reduces their index).
		 * This call shortens the list by {@code (toIndex - fromIndex)} elements.
		 * (If {@code toIndex==fromIndex}, this operation has no effect.)
		 *
		 * @throws IndexOutOfBoundsException if {@code fromIndex} or
		 *         {@code toIndex} is out of range
		 *         ({@code fromIndex < 0 ||
		 *          fromIndex >= size() ||
		 *          toIndex > size() ||
		 *          toIndex < fromIndex})
		 */
	protected:
		virtual void removeRange(int fromIndex, int toIndex)
		{
			AbstractList<E>::modCount++;
			int numMoved = size_Conflict - toIndex;
			memcpy(elementData_Conflict + toIndex, elementData_Conflict + fromIndex, numMoved);

			// Let gc do its work
			int newSize = size_Conflict - (toIndex - fromIndex);
			while (size_Conflict != newSize)
			{
				elementData_Conflict[--size_Conflict] = NULL;
			}
		}

		/**
		 * Checks if the given index is in range.  If not, throws an appropriate
		 * runtime exception.  This method does *not* check if the index is
		 * negative: It is always used immediately prior to an array access,
		 * which throws an ArrayIndexOutOfBoundsException if index is negative.
		 */
	private:
		void rangeCheck(int index)
		{
			if (index >= size_Conflict)
			{
				throw std::out_of_range("error");
			}
		}

		/**
		 * A version of rangeCheck used by add and addAll.
		 */
		void rangeCheckForAdd(int index)
		{
			if (index > size_Conflict || index < 0)
			{
				throw std::out_of_range("error");
			}
		}

		/**
		 * Constructs an IndexOutOfBoundsException detail message.
		 * Of the many possible refactorings of the error handling code,
		 * this "outlining" performs best with both server and client VMs.
		 */
		std::wstring outOfBoundsMsg(int index)
		{
			return L"Index: " + std::to_wstring(index) + L", Size: " + std::to_wstring(size_Conflict);
		}

		/**
		 * Removes from this list all of its elements that are contained in the
		 * specified collection.
		 *
		 * @param c collection containing elements to be removed from this list
		 * @return {@code true} if this list changed as a result of the call
		 * @throws ClassCastException if the class of an element of this list
		 *         is incompatible with the specified collection (optional)
		 * @throws NullPointerException if this list contains a null element and the
		 *         specified collection does not permit null elements (optional),
		 *         or if the specified collection is null
		 * @see Collection#contains(Object)
		 */
	public:
		virtual bool removeAll(Collection<E> *c)
		{
			return batchRemove(c, false);
		}

		/**
		 * Retains only the elements in this list that are contained in the
		 * specified collection.  In other words, removes from this list all
		 * of its elements that are not contained in the specified collection.
		 *
		 * @param c collection containing elements to be retained in this list
		 * @return {@code true} if this list changed as a result of the call
		 * @throws ClassCastException if the class of an element of this list
		 *         is incompatible with the specified collection (optional)
		 * @throws NullPointerException if this list contains a null element and the
		 *         specified collection does not permit null elements (optional),
		 *         or if the specified collection is null
		 * @see Collection#contains(Object)
		 */
		virtual bool retainAll(Collection<E> *c)
		{
			return batchRemove(c, true);
		}

	private:
		bool batchRemove(Collection<E> *c, bool complement)
		{
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: final Object[] elementData = this.elementData;
			E * const elementData = this->elementData_Conflict;
			std::atomic<int> r;
			std::atomic<int> w;
			std::atomic<bool> modified;

			finallyClause f(this, &r, &w, &modified);
			for (; r.load() < size_Conflict; r.store(r.load() + 1))
			{
				if (c->contains(elementData[r.load()]) == complement)
				{
					int ww = w.load() + 1;
					w.store(ww);
					elementData[ww] = elementData[r.load()];
				}
			}
			return modified.load();
		}

		class finallyClause {
		private:
			std::atomic<int> *r;
			std::atomic<int> *w;
			std::atomic<bool> *modified;
			ArrayList *outerInstance;
		public:
			finallyClause(ArrayList *outerInstance, std::atomic<int> *r, std::atomic<int> *w, std::atomic<bool> *modified) {
				this->outerInstance = outerInstance;
				this->r = r;
				this->w = w;
				this->modified = modified;
			}
			~finallyClause() {
				// Preserve behavioral compatibility with AbstractCollection,
				// even if c.contains() throws.
				if (r->load() != outerInstance->size_Conflict)
				{
					memcpy(outerInstance->elementData_Conflict + r->load(), outerInstance->elementData_Conflict + w->load(), outerInstance->size_Conflict - r->load());
					w->store(w->load() + outerInstance->size_Conflict - r->load());
				}
				if (w->load() != outerInstance->size_Conflict)
				{
					for (int i = w->load(); i < outerInstance->size_Conflict; i++)
					{
						outerInstance->elementData_Conflict[i] = NULL;
					}
					outerInstance->modCount += outerInstance->size_Conflict - w->load();
					outerInstance->size_Conflict = w->load();
					modified->store(true);
				}
			}
		};

		 /**
		 * Returns a list beginIterator over the elements in this list (in proper
		 * sequence), starting at the specified position in the list.
		 * The specified index indicates the first element that would be
		 * returned by an initial call to {@link ListIterator#next next}.
		 * An initial call to {@link ListIterator#previous previous} would
		 * return the element with the specified index minus one.
		 *
		 * <p>The returned list beginIterator is <a href="#fail-fast"><i>fail-fast</i></a>.
		 *
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 */
	public:
		virtual ListIterator<E> *listIterator(int index)
		{
			if (index < 0 || index > size_Conflict)
			{
				throw std::out_of_range("Index out of range");
			}
			return new ListItr(this, index);
		}

		/**
		 * Returns a list beginIterator over the elements in this list (in proper
		 * sequence).
		 *
		 * <p>The returned list beginIterator is <a href="#fail-fast"><i>fail-fast</i></a>.
		 *
		 * @see #listIterator(int)
		 */
		virtual ListIterator<E> *listIterator()
		{
			return new ListItr(this, 0);
		}

		/**
		 * Returns an beginIterator over the elements in this list in proper sequence.
		 *
		 * <p>The returned beginIterator is <a href="#fail-fast"><i>fail-fast</i></a>.
		 *
		 * @return an beginIterator over the elements in this list in proper sequence
		 */
		virtual Iterator<E> *beginIterator()
		{
			return new Itr(this);
		}

		/**
		 * An optimized version of AbstractList.Itr
		 */
	private:
		class Itr : public Iterator<E>
		{
		private:
			ArrayList<E> *outerInstance;

		public:
			Itr(ArrayList<E> *outerInstance) : outerInstance(outerInstance)
			{
			}

			int cursor = 0; // index of next element to return
			int lastRet = -1; // index of last element returned; -1 if no such
			int expectedModCount = outerInstance->modCount;

			virtual bool hasNext()
			{
				return cursor != outerInstance->size();
			}

//JAVA TO C++ CONVERTER TODO TASK: Most Java annotations will not have direct C++ equivalents:
//ORIGINAL LINE: @SuppressWarnings("unchecked") public E next()
			virtual E nextEntry()
			{
				checkForComodification();
				int i = cursor;
				if (i >= outerInstance->size())
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Object[] elementData = ArrayList.this.elementData;
				E *elementData = outerInstance->elementData_Conflict;
				if (i >= outerInstance->elementDataLen)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
				cursor = i + 1;
				return static_cast<E>(elementData[lastRet = i]);
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
					cursor = lastRet;
					lastRet = -1;
					expectedModCount = outerInstance->modCount;
				}
				catch (const std::out_of_range &ex)
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

		/**
		 * An optimized version of AbstractList.ListItr
		 */
	private:
		class ListItr : public Itr, public ListIterator<E>
		{
		private:
			ArrayList<E> *outerInstance;

		public:
			ListItr(ArrayList<E> *outerInstance, int index) : Itr(outerInstance), outerInstance(outerInstance)
			{
				Itr::cursor = index;
			}

			virtual bool hasPrevious()
			{
				return Itr::cursor != 0;
			}

			virtual int nextIndex()
			{
				return Itr::cursor;
			}

			virtual int previousIndex()
			{
				return Itr::cursor - 1;
			}

//JAVA TO C++ CONVERTER TODO TASK: Most Java annotations will not have direct C++ equivalents:
//ORIGINAL LINE: @SuppressWarnings("unchecked") public E previous()
			virtual E previous()
			{
				Itr::checkForComodification();
				int i = Itr::cursor - 1;
				if (i < 0)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Object[] elementData = ArrayList.this.elementData;
				E *elementData = outerInstance->elementData_Conflict;
				if (i >= outerInstance->elementDataLen)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
				Itr::cursor = i;
				return static_cast<E>(elementData[Itr::lastRet = i]);
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
					Itr::cursor = i + 1;
					Itr::lastRet = -1;
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
		 * Returns a view of the portion of this list between the specified
		 * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.  (If
		 * {@code fromIndex} and {@code toIndex} are equal, the returned list is
		 * empty.)  The returned list is backed by this list, so non-structural
		 * changes in the returned list are reflected in this list, and vice-versa.
		 * The returned list supports all of the optional list operations.
		 *
		 * <p>This method eliminates the need for explicit range operations (of
		 * the sort that commonly exist for arrays).  Any operation that expects
		 * a list can be used as a range operation by passing a subList view
		 * instead of a whole list.  For example, the following idiom
		 * removes a range of elements from a list:
		 * <pre>
		 *      list.subList(from, to).clear();
		 * </pre>
		 * Similar idioms may be constructed for {@link #indexOf(Object)} and
		 * {@link #lastIndexOf(Object)}, and all of the algorithms in the
		 * {@link Collections} class can be applied to a subList.
		 *
		 * <p>The semantics of the list returned by this method become undefined if
		 * the backing list (i.e., this list) is <i>structurally modified</i> in
		 * any way other than via the returned list.  (Structural modifications are
		 * those that change the size of this list, or otherwise perturb it in such
		 * a fashion that iterations in progress may yield incorrect results.)
		 *
		 * @throws IndexOutOfBoundsException {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
	public:
		virtual List<E> *subList(int fromIndex, int toIndex)
		{
			subListRangeCheck(fromIndex, toIndex, size_Conflict);
			return new SubList(this, this, 0, fromIndex, toIndex);
		}

		static void subListRangeCheck(int fromIndex, int toIndex, int size)
		{
			if (fromIndex < 0)
			{
				throw std::out_of_range("error");
			}
			if (toIndex > size)
			{
				throw std::out_of_range("error");
			}
			if (fromIndex > toIndex)
			{
				throw std::invalid_argument("error");
			}
		}

	private:
		class SubList : public AbstractList<E>
		{
		private:
			ArrayList<E> *outerInstance;

			AbstractList<E> *const parent;
			const int parentOffset;
			const int offset;
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
			int size_Conflict = 0;

		public:
			SubList(ArrayList<E> *outerInstance, AbstractList<E> *parent, int offset, int fromIndex, int toIndex) : parent(parent), parentOffset(fromIndex), offset(offset + fromIndex), outerInstance(outerInstance)
			{
				this->size_Conflict = toIndex - fromIndex;
				this->modCount = outerInstance->modCount;
			}

			virtual E set(int index, E e)
			{
				rangeCheck(index);
				checkForComodification();
				E oldValue = outerInstance->getElementData(offset + index);
				outerInstance->elementData_Conflict[offset + index] = e;
				return oldValue;
			}

			virtual E get(int index)
			{
				rangeCheck(index);
				checkForComodification();
				return outerInstance->getElementData(offset + index);
			}

			virtual int size()
			{
				checkForComodification();
				return this->size_Conflict;
			}

			virtual void add(int index, E e)
			{
				rangeCheckForAdd(index);
				checkForComodification();
				parent->add(parentOffset + index, e);
				this->modCount = parent->modCount;
				this->size_Conflict++;
			}

			virtual E remove(int index)
			{
				rangeCheck(index);
				checkForComodification();
				E result = parent->remove(parentOffset + index);
				this->modCount = parent->modCount;
				this->size_Conflict--;
				return result;
			}

		protected:
			virtual void removeRange(int fromIndex, int toIndex)
			{
				checkForComodification();
				parent->removeRange(parentOffset + fromIndex, parentOffset + toIndex);
				this->modCount = parent->modCount;
				this->size_Conflict -= toIndex - fromIndex;
			}

		public:
			virtual bool addAll(Collection<E> *c)
			{
				return addAll(this->size_Conflict, c);
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
				parent->addAll(parentOffset + index, c);
				this->modCount = parent->modCount;
				this->size_Conflict += cSize;
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
				SubList *outerInstance;

				int index = 0;

			public:
				ListIteratorAnonymousInnerClass(SubList *outerInstance, int index)
				{
					this->outerInstance = outerInstance;
					this->index = index;
					cursor = index;
					lastRet = -1;
					expectedModCount = outerInstance->outerInstance->modCount;
				}

				int cursor = 0;
				int lastRet = 0;
				int expectedModCount = 0;

				bool hasNext()
				{
					return cursor != outerInstance->size();
				}

//JAVA TO C++ CONVERTER TODO TASK: Most Java annotations will not have direct C++ equivalents:
//ORIGINAL LINE: @SuppressWarnings("unchecked") public E next()
				E next()
				{
					outerInstance->checkForComodification();
					int i = cursor;
					if (i >= outerInstance->size)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Object[] elementData = outerInstance.outerInstance.elementData;
					E *elementData = outerInstance->outerInstance->elementData;
					if (outerInstance->offset + i >= elementData->length)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
					cursor = i + 1;
					return static_cast<E>(elementData[outerInstance->offset + (lastRet = i)]);
				}

				bool hasPrevious()
				{
					return cursor != 0;
				}

//JAVA TO C++ CONVERTER TODO TASK: Most Java annotations will not have direct C++ equivalents:
//ORIGINAL LINE: @SuppressWarnings("unchecked") public E previous()
				E previous()
				{
					outerInstance->checkForComodification();
					int i = cursor - 1;
					if (i < 0)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
//JAVA TO C++ CONVERTER WARNING: Java to C++ Converter has converted this array to a pointer. You will need to call 'delete[]' where appropriate:
//ORIGINAL LINE: Object[] elementData = outerInstance.outerInstance.elementData;
					E *elementData = outerInstance->outerInstance->elementData_Conflict;
					if (outerInstance->offset + i >= outerInstance->outerInstance->elementDataLen)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
					cursor = i;
					return static_cast<E>(elementData[outerInstance->offset + (lastRet = i)]);
				}

				int nextIndex()
				{
					return cursor;
				}

				int previousIndex()
				{
					return cursor - 1;
				}

				void remove()
				{
					if (lastRet < 0)
					{
						throw IllegalStateException();
					}
					outerInstance->checkForComodification();

					try
					{
						outerInstance->remove(lastRet);
						cursor = lastRet;
						lastRet = -1;
						expectedModCount = outerInstance->outerInstance->modCount;
					}
					catch (const std::out_of_range &ex)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
				}

				void set(E e)
				{
					if (lastRet < 0)
					{
						throw IllegalStateException();
					}
					outerInstance->checkForComodification();

					try
					{
						outerInstance->outerInstance->set(outerInstance->offset + lastRet, e);
					}
					catch (const std::out_of_range &ex)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
				}

				void add(E e)
				{
					outerInstance->checkForComodification();

					try
					{
						int i = cursor;
						outerInstance->add(i, e);
						cursor = i + 1;
						lastRet = -1;
						expectedModCount = outerInstance->outerInstance->modCount;
					}
					catch (const std::out_of_range &ex)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
				}

				void checkForComodification()
				{
					if (expectedModCount != outerInstance->outerInstance->modCount)
					{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
						throw std::runtime_error("error");
					}
				}
			};

		public:
			virtual List<E> *subList(int fromIndex, int toIndex)
			{
				subListRangeCheck(fromIndex, toIndex, size_Conflict);
				return new SubList(outerInstance, this, offset, fromIndex, toIndex);
			}

		private:
			void rangeCheck(int index)
			{
				if (index < 0 || index >= this->size_Conflict)
				{
					throw std::out_of_range("error");
				}
			}

			void rangeCheckForAdd(int index)
			{
				if (index < 0 || index > this->size_Conflict)
				{
					throw std::out_of_range("error");
				}
			}

			std::wstring outOfBoundsMsg(int index)
			{
				return L"Index: " + std::to_wstring(index) + L", Size: " + std::to_wstring(this->size_Conflict);
			}

			void checkForComodification()
			{
				if (outerInstance->modCount != this->modCount)
				{
//JAVA TO C++ CONVERTER TODO TASK: This exception's constructor requires an argument:
//ORIGINAL LINE: throw new RuntimeException();
					throw std::runtime_error("error");
				}
			}
		};
	};

}
