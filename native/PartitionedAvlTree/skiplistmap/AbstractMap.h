#pragma once

#include "Map.h"
#include "Iterator.h"
#include "Set.h"
#include "Collection.h"
#include "AbstractSet.h"
#include "AbstractCollection.h"
#include "Serializable.h"
#include <string>
#include "exceptionhelper.h"

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
	 * This class provides a skeletal implementation of the <tt>skiplist.Map</tt>
	 * interface, to minimize the effort required to implement this interface.
	 *
	 * <p>To implement an unmodifiable map, the programmer needs only to extend this
	 * class and provide an implementation for the <tt>entrySet</tt> method, which
	 * returns a set-view of the map's mappings.  Typically, the returned set
	 * will, in turn, be implemented atop <tt>skiplist.AbstractSet</tt>.  This set should
	 * not support the <tt>add</tt> or <tt>remove</tt> methods, and its beginIterator
	 * should not support the <tt>remove</tt> method.
	 *
	 * <p>To implement a modifiable map, the programmer must additionally override
	 * this class's <tt>put</tt> method (which otherwise throws an
	 * <tt>UnsupportedOperationException</tt>), and the beginIterator returned by
	 * <tt>entrySet().beginIterator()</tt> must additionally implement its
	 * <tt>remove</tt> method.
	 *
	 * <p>The programmer should generally provide a void (no argument) and map
	 * constructor, as per the recommendation in the <tt>skiplist.Map</tt> interface
	 * specification.
	 *
	 * <p>The documentation for each non-abstract method in this class describes its
	 * implementation in detail.  Each of these methods may be overridden if the
	 * map being implemented admits a more efficient implementation.
	 *
	 * <p>This class is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @param <K> the type of keys maintained by this map
	 * @param <V> the type of mapped values
	 *
	 * @author  Josh Bloch
	 * @author  Neal Gafter
	 * @see Map
	 * @see Collection
	 * @since 1.2
	 */

	template<typename K, typename V>
	class AbstractMap : public Map<K, V>
	{
		/**
		 * Sole constructor.  (For invocation by subclass constructors, typically
		 * implicit.)
		 */
	protected:
		AbstractMap()
		{
		}

		// Query Operations

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns <tt>entrySet().size()</tt>.
		 */
	public:
		virtual int size()
		{
			return entrySet()->size();
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
		 * <p>This implementation iterates over <tt>entrySet()</tt> searching
		 * for an entry with the specified value.  If such an entry is found,
		 * <tt>true</tt> is returned.  If the iteration terminates without
		 * finding such an entry, <tt>false</tt> is returned.  Note that this
		 * implementation requires linear time in the size of the map.
		 *
		 * @throws ClassCastException   {@inheritDoc}
		 * @throws NullPointerException {@inheritDoc}
		 */
		virtual bool containsValue(void *value)
		{
			Iterator<typename Map<K,V>::template Entry<K, V>*> *i = entrySet()->beginIterator();
			if (value == NULL)
			{
				while (i->hasNext())
				{
					typename Map<K,V>::template Entry<K, V> *e = i->nextEntry();
					if (e->getValue() == NULL)
					{
						return true;
					}
					i++;
				}
			}
			else
			{
				while (i->hasNext())
				{
					typename Map<K,V>::template Entry<K, V> *e = i->nextEntry();
					if (((V)value)->equals(e->getValue()))
					{
						return true;
					}
					i++;
				}
			}
			return false;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over <tt>entrySet()</tt> searching
		 * for an entry with the specified key.  If such an entry is found,
		 * <tt>true</tt> is returned.  If the iteration terminates without
		 * finding such an entry, <tt>false</tt> is returned.  Note that this
		 * implementation requires linear time in the size of the map; many
		 * implementations will override this method.
		 *
		 * @throws ClassCastException   {@inheritDoc}
		 * @throws NullPointerException {@inheritDoc}
		 */
		virtual bool containsKey(void *key)
		{
			Iterator<typename Map<K,V>::template Entry<K, V>*> *i = entrySet()->beginIterator();
			if (key == NULL)
			{
				while (i->hasNext())
				{
					typename Map<K,V>:: template Entry<K, V> *e = i->nextEntry();
					if (e->getKey() == NULL)
					{
						return true;
					}
					i++;
				}
			}
			else
			{
				while (i->hasNext())
				{
					typename Map<K,V>:: template Entry<K, V> *e = i->nextEntry();
					if (((K)key)->equals(e->getKey()))
					{
						return true;
					}
					i++;
				}
			}
			return false;
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over <tt>entrySet()</tt> searching
		 * for an entry with the specified key.  If such an entry is found,
		 * the entry's value is returned.  If the iteration terminates without
		 * finding such an entry, <tt>null</tt> is returned.  Note that this
		 * implementation requires linear time in the size of the map; many
		 * implementations will override this method.
		 *
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 */
		virtual V get(K key)
		{
			Iterator<typename Map<K,V>::template Entry<K, V>*> *i = entrySet()->beginIterator();
			if (key == NULL)
			{
				while (i->hasNext())
				{
					typename Map<K,V>::template Entry<K, V> *e = i->nextEntry();
					if (e->getKey() == NULL)
					{
						return e->getValue();
					}
					i++;
				}
			}
			else
			{
				while (i->hasNext())
				{
					typename Map<K,V>::template Entry<K, V> *e = i->nextEntry();
					if (((K)key)->equals(e->getKey()))
					{
						return e->getValue();
					}
					i++;
				}
			}
			return NULL;
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
		 */
		virtual V put(K key, V value)
		{
			throw UnsupportedOperationException();
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over <tt>entrySet()</tt> searching for an
		 * entry with the specified key.  If such an entry is found, its value is
		 * obtained with its <tt>getValue</tt> operation, the entry is removed
		 * from the collection (and the backing map) with the beginIterator's
		 * <tt>remove</tt> operation, and the saved value is returned.  If the
		 * iteration terminates without finding such an entry, <tt>null</tt> is
		 * returned.  Note that this implementation requires linear time in the
		 * size of the map; many implementations will override this method.
		 *
		 * <p>Note that this implementation throws an
		 * <tt>UnsupportedOperationException</tt> if the <tt>entrySet</tt>
		 * beginIterator does not support the <tt>remove</tt> method and this map
		 * contains a mapping for the specified key.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 */
		virtual V remove(void *key)
		{
			Iterator<typename Map<K,V>:: template Entry<K,V>*> *i = entrySet()->beginIterator();
			typename Map<K,V>:: template Entry<K,V> *correctEntry = NULL;
			if (key == NULL)
			{
				while (correctEntry == NULL && i->hasNext())
				{
					typename Map<K,V>:: template Entry<K,V> *e = i->nextEntry();
					if (e->getKey() == NULL)
					{
						correctEntry = e;
					}
				}
			}
			else
			{
				while (correctEntry == NULL && i->hasNext())
				{
					typename Map<K,V>:: template Entry<K,V> *e = i->nextEntry();
					if (((K)key)->equals(e->getKey()))
					{
						correctEntry = e;
					}
				}
			}

			V oldValue = NULL;
			if (correctEntry != NULL)
			{
				oldValue = correctEntry->getValue();
				i->remove();
			}
			return oldValue;
		}


		// Bulk Operations

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation iterates over the specified map's
		 * <tt>entrySet()</tt> collection, and calls this map's <tt>put</tt>
		 * operation once for each entry returned by the iteration.
		 *
		 * <p>Note that this implementation throws an
		 * <tt>UnsupportedOperationException</tt> if this map does not support
		 * the <tt>put</tt> operation and the specified map is nonempty.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 * @throws ClassCastException            {@inheritDoc}
		 * @throws NullPointerException          {@inheritDoc}
		 * @throws IllegalArgumentException      {@inheritDoc}
		 */
		virtual void putAll(Map<K, V> *m)
		{
			for (Iterator<typename Map<K,V>:: template Entry<K,V>*> *iter = m->entrySet()->beginIterator(); iter->hasNext();)
			{
				typename Map<K,V>:: template Entry<K,V> *e = iter->nextEntry();
				put(e->getKey(), e->getValue());
			}
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation calls <tt>entrySet().clear()</tt>.
		 *
		 * <p>Note that this implementation throws an
		 * <tt>UnsupportedOperationException</tt> if the <tt>entrySet</tt>
		 * does not support the <tt>clear</tt> operation.
		 *
		 * @throws UnsupportedOperationException {@inheritDoc}
		 */
		virtual void clear()
		{
			entrySet()->clear();
		}


		// Views

		/**
		 * Each of these fields are initialized to contain an instance of the
		 * appropriate view the first time this view is requested.  The views are
		 * stateless, so there's no reason to create more than one of each.
		 */
//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: transient volatile Set<K> keySet = null;
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		Set<K> *keySet_Conflict = NULL;
//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: transient volatile Collection<V> values = null;
//JAVA TO C++ CONVERTER NOTE: Fields cannot have the same name as methods:
		Collection<V> *values_Conflict = NULL;

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns a set that subclasses {@link AbstractSet}.
		 * The subclass's beginIterator method returns a "wrapper object" over this
		 * map's <tt>entrySet()</tt> beginIterator.  The <tt>size</tt> method
		 * delegates to this map's <tt>size</tt> method and the
		 * <tt>contains</tt> method delegates to this map's
		 * <tt>containsKey</tt> method.
		 *
		 * <p>The set is created the first time this method is called,
		 * and returned in response to all subsequent calls.  No synchronization
		 * is performed, so there is a slight chance that multiple calls to this
		 * method will not all return the same set.
		 */
		virtual Set<K> *keySet()
		{
			if (keySet_Conflict == NULL)
			{
				//keySet_Conflict = new AbstractSetAnonymousInnerClass(this);
			}
			return keySet_Conflict;
		}

	private:
		class AbstractSetAnonymousInnerClass : public AbstractSet<K>
		{
		private:
			AbstractMap<K, V> *outerInstance;

		public:
			AbstractSetAnonymousInnerClass(AbstractMap<K, V> *outerInstance)
			{
				this->outerInstance = outerInstance;
			}

			virtual Iterator<K> *beginIterator()
			{
				return new IteratorAnonymousInnerClass(this);
			}

		private:
			class IteratorAnonymousInnerClass : public Iterator<K>
			{
			private:
				AbstractSetAnonymousInnerClass *outerInstance;

			public:
				IteratorAnonymousInnerClass(AbstractSetAnonymousInnerClass *outerInstance)
				{
					this->outerInstance = outerInstance;
					i = outerInstance->outerInstance->entrySet().beginIterator();
				}

			private:
				Iterator<typename Map<K,V>::template Entry<K, V>*> *i;

			public:
				bool hasNext()
				{
					return i->hasNext();
				}

				K next()
				{
					return i->next().getKey();
				}

				void remove()
				{
					i->remove();
				}
			};

		public:
			virtual int size()
			{
				return outerInstance->size();
			}

			virtual bool isEmpty()
			{
				return outerInstance->isEmpty();
			}

			virtual void clear()
			{
				outerInstance->clear();
			}

			virtual bool contains(void *k)
			{
				return outerInstance->containsKey(k);
			}
		};

		/**
		 * {@inheritDoc}
		 *
		 * <p>This implementation returns a collection that subclasses {@link
		 * AbstractCollection}.  The subclass's beginIterator method returns a
		 * "wrapper object" over this map's <tt>entrySet()</tt> beginIterator.
		 * The <tt>size</tt> method delegates to this map's <tt>size</tt>
		 * method and the <tt>contains</tt> method delegates to this map's
		 * <tt>containsValue</tt> method.
		 *
		 * <p>The collection is created the first time this method is called, and
		 * returned in response to all subsequent calls.  No synchronization is
		 * performed, so there is a slight chance that multiple calls to this
		 * method will not all return the same collection.
		 */
	public:
		virtual Collection<V> *values()
		{
			if (values_Conflict == NULL)
			{
				//values_Conflict = new AbstractCollectionAnonymousInnerClass(this);
			}
			return values_Conflict;
		}

	private:
		class AbstractCollectionAnonymousInnerClass : public AbstractCollection<V>
		{
		private:
			AbstractMap<K, V> *outerInstance;

		public:
			AbstractCollectionAnonymousInnerClass(AbstractMap<K, V> *outerInstance)
			{
				this->outerInstance = outerInstance;
			}

			virtual Iterator<V> *beginIterator()
			{
				return new IteratorAnonymousInnerClass2(this);
			}

		private:
			class IteratorAnonymousInnerClass2 : public Iterator<V>
			{
			private:
				AbstractCollectionAnonymousInnerClass *outerInstance;

			public:
				IteratorAnonymousInnerClass2(AbstractCollectionAnonymousInnerClass *outerInstance)
				{
					this->outerInstance = outerInstance;
					i = outerInstance->outerInstance->entrySet().beginIterator();
				}

			private:
				Iterator<typename Map<K,V>::template Entry<K, V>*> *i;

			public:
				bool hasNext()
				{
					return i->hasNext();
				}

				V next()
				{
					return i->next().getValue();
				}

				void remove()
				{
					i->remove();
				}
			};

		public:
			virtual int size()
			{
				return outerInstance->size();
			}

			virtual bool isEmpty()
			{
				return outerInstance->isEmpty();
			}

			virtual void clear()
			{
				outerInstance->clear();
			}

			virtual bool contains(void *v)
			{
				return outerInstance->containsValue(v);
			}
		};

	public:
		virtual Set<typename Map<K,V>::template Entry<K, V>*> *entrySet()
		{
			return NULL;
		}


		// Comparison and hashing

		/**
		 * Compares the specified object with this map for equality.  Returns
		 * <tt>true</tt> if the given object is also a map and the two maps
		 * represent the same mappings.  More formally, two maps <tt>m1</tt> and
		 * <tt>m2</tt> represent the same mappings if
		 * <tt>m1.entrySet().equals(m2.entrySet())</tt>.  This ensures that the
		 * <tt>equals</tt> method works properly across different implementations
		 * of the <tt>skiplist.Map</tt> interface.
		 *
		 * <p>This implementation first checks if the specified object is this map;
		 * if so it returns <tt>true</tt>.  Then, it checks if the specified
		 * object is a map whose size is identical to the size of this map; if
		 * not, it returns <tt>false</tt>.  If so, it iterates over this map's
		 * <tt>entrySet</tt> collection, and checks that the specified map
		 * contains each mapping that this map contains.  If the specified map
		 * fails to contain such a mapping, <tt>false</tt> is returned.  If the
		 * iteration completes, <tt>true</tt> is returned.
		 *
		 * @param o object to be compared for equality with this map
		 * @return <tt>true</tt> if the specified object is equal to this map
		 */
		virtual bool equals(void *o)
		{
			if (o == this)
			{
				return true;
			}

			if (!(static_cast<Map<K,V>*>(o) != NULL))
			{
				return false;
			}
			Map<K, V> *m = static_cast<Map<K, V>*>(o);
			if (m->size() != size())
			{
				return false;
			}

			try
			{
				Iterator<typename Map<K,V>:: template Entry<K,V>*> *i = entrySet()->beginIterator();
				while (i->hasNext())
				{
					typename Map<K,V>:: template Entry<K,V> *e = i->nextEntry();
					K key = e->getKey();
					V value = e->getValue();
					if (value == NULL)
					{
						if (!(m->get(key) == NULL && m->containsKey(key)))
						{
							return false;
						}
					}
					else
					{
						if (!value->equals(m->get(key)))
						{
							return false;
						}
					}
					i++;
				}
			}
			catch (const ClassCastException &unused)
			{
				return false;
			}
			catch (const NullPointerException &unused)
			{
				return false;
			}

			return true;
		}

		/**
		 * Returns the hash code value for this map.  The hash code of a map is
		 * defined to be the sum of the hash codes of each entry in the map's
		 * <tt>entrySet()</tt> view.  This ensures that <tt>m1.equals(m2)</tt>
		 * implies that <tt>m1.hashCode()==m2.hashCode()</tt> for any two maps
		 * <tt>m1</tt> and <tt>m2</tt>, as required by the general contract of
		 * {@link Object#hashCode}.
		 *
		 * <p>This implementation iterates over <tt>entrySet()</tt>, calling
		 * {@link Entry#hashCode hashCode()} on each element (entry) in the
		 * set, and adding up the results.
		 *
		 * @return the hash code value for this map
		 * @see Entry#hashCode()
		 * @see Object#equals(Object)
		 * @see Set#equals(Object)
		 */
		virtual int hashCode()
		{
			int h = 0;
			Iterator<typename Map<K,V>:: template Entry<K,V>*> *i = entrySet()->beginIterator();
			while (i->hasNext())
			{
				h += i->nextEntry()->hashCode();
				i++;
			}
			return h;
		}

		/**
		 * Utility method for SimpleEntry and SimpleImmutableEntry.
		 * skiplist.Test for equality, checking for nulls.
		 */
	private:
		static bool eq(K o1, K o2)
		{
			return o1 == NULL ? o2 == NULL : o1->equals(o2);
		}

		static bool eqValue(V o1, V o2)
		{
			return o1 == NULL ? o2 == NULL : o1->equals(o2);
		}

		// Implementation Note: SimpleEntry and SimpleImmutableEntry
		// are distinct unrelated classes, even though they share
		// some code. Since you can't add or subtract final-ness
		// of a field in a subclass, they can't share representations,
		// and the amount of duplicated code is too small to warrant
		// exposing a common abstract class.


		/**
		 * An Entry maintaining a key and a value.  The value may be
		 * changed using the <tt>setValue</tt> method.  This class
		 * facilitates the process of building custom map
		 * implementations. For example, it may be convenient to return
		 * arrays of <tt>SimpleEntry</tt> instances in method
		 * <tt>skiplist.Map.entrySet().toArray</tt>.
		 *
		 * @since 1.6
		 */
	public:
		template<typename K1, typename V1>
		class SimpleEntry : public Map<K1,V1>::template Entry<K1, V1>, public Serializable
		{
		private:
			static constexpr long long serialVersionUID = -8499721149061103585LL;

			const K1 key;
			V1 value;

			/**
			 * Creates an entry representing a mapping from the specified
			 * key to the specified value.
			 *
			 * @param key the key represented by this entry
			 * @param value the value represented by this entry
			 */
		public:
			SimpleEntry(K1 key, V1 value) : key(key)
			{
				this->value = value;
			}

			/**
			 * Creates an entry representing the same mapping as the
			 * specified entry.
			 *
			 * @param entry the entry to copy
			 */
			SimpleEntry(typename Map<K,V>::template Entry<K1, V1> *entry) : key(entry->getKey())
			{
				this->value = entry->getValue();
			}

			/**
			 * Returns the key corresponding to this entry.
			 *
			 * @return the key corresponding to this entry
			 */
			virtual K1 getKey()
			{
				return key;
			}

			/**
			 * Returns the value corresponding to this entry.
			 *
			 * @return the value corresponding to this entry
			 */
			virtual V1 getValue()
			{
				return value;
			}

			/**
			 * Replaces the value corresponding to this entry with the specified
			 * value.
			 *
			 * @param value new value to be stored in this entry
			 * @return the old value corresponding to the entry
			 */
			virtual V1 setValue(V1 value)
			{
				V1 oldValue = this->value;
				this->value = value;
				return oldValue;
			}

			/**
			 * Compares the specified object with this entry for equality.
			 * Returns {@code true} if the given object is also a map entry and
			 * the two entries represent the same mapping.  More formally, two
			 * entries {@code e1} and {@code e2} represent the same mapping
			 * if<pre>
			 *   (e1.getKey()==null ?
			 *    e2.getKey()==null :
			 *    e1.getKey().equals(e2.getKey()))
			 *   &amp;&amp;
			 *   (e1.getValue()==null ?
			 *    e2.getValue()==null :
			 *    e1.getValue().equals(e2.getValue()))</pre>
			 * This ensures that the {@code equals} method works properly across
			 * different implementations of the {@code skiplist.Map.Entry} interface.
			 *
			 * @param o object to be compared for equality with this map entry
			 * @return {@code true} if the specified object is equal to this map
			 *         entry
			 * @see    #hashCode
			 */
			virtual bool equals(void *o)
			{
				if (!(static_cast<typename Map<K,V>::template Entry<K,V>*>(o) != NULL))
				{
					return false;
				}
				typename Map<K,V>::template Entry<K,V> *e = static_cast<typename Map<K,V>::template Entry<K,V>*>(o);
				return eq(key, e->getKey()) && eqValue(value, e->getValue());
			}

			/**
			 * Returns the hash code value for this map entry.  The hash code
			 * of a map entry {@code e} is defined to be: <pre>
			 *   (e.getKey()==null   ? 0 : e.getKey().hashCode()) ^
			 *   (e.getValue()==null ? 0 : e.getValue().hashCode())</pre>
			 * This ensures that {@code e1.equals(e2)} implies that
			 * {@code e1.hashCode()==e2.hashCode()} for any two Entries
			 * {@code e1} and {@code e2}, as required by the general
			 * contract of {@link Object#hashCode}.
			 *
			 * @return the hash code value for this map entry
			 * @see    #equals
			 */
			virtual int hashCode()
			{
				return (key == NULL ? 0 : key.hashCode()) ^ (value == NULL ? 0 : value.hashCode());
			}

			/**
			 * Returns a String representation of this map entry.  This
			 * implementation returns the string representation of this
			 * entry's key followed by the equals character ("<tt>=</tt>")
			 * followed by the string representation of this entry's value.
			 *
			 * @return a String representation of this map entry
			 */
			virtual std::wstring toString()
			{
				return key + L"=" + value;
			}

		};

		/**
		 * An Entry maintaining an immutable key and value.  This class
		 * does not support method <tt>setValue</tt>.  This class may be
		 * convenient in methods that return thread-safe snapshots of
		 * key-value mappings.
		 *
		 * @since 1.6
		 */
	public:
		template<typename K1, typename V1>
		class SimpleImmutableEntry : public Map<K1,V1>::template Entry<K1, V1>, public Serializable
		{
		private:
			static constexpr long long serialVersionUID = 7138329143949025153LL;

			K1 key;
			V1 value;

			/**
			 * Creates an entry representing a mapping from the specified
			 * key to the specified value.
			 *
			 * @param key the key represented by this entry
			 * @param value the value represented by this entry
			 */
		public:
			SimpleImmutableEntry()
			{
			}
                        
                        void setKey(K1 key) {
                            this->key = key;
                        }
     
			SimpleImmutableEntry(K1 key, V1 value) : key(key), value(value)
			{
			}

			/**
			 * Creates an entry representing the same mapping as the
			 * specified entry.
			 *
			 * @param entry the entry to copy
			 */
			SimpleImmutableEntry(typename Map<K1,V1>::template Entry<K1, V1> *entry) : key(entry->getKey()), value(entry->getValue())
			{
			}

			/**
			 * Returns the key corresponding to this entry.
			 *
			 * @return the key corresponding to this entry
			 */
			virtual K1 getKey()
			{
				return key;
			}

			/**
			 * Returns the value corresponding to this entry.
			 *
			 * @return the value corresponding to this entry
			 */
			virtual V1 getValue()
			{
				return value;
			}

			/**
			 * Replaces the value corresponding to this entry with the specified
			 * value (optional operation).  This implementation simply throws
			 * <tt>UnsupportedOperationException</tt>, as this class implements
			 * an <i>immutable</i> map entry.
			 *
			 * @param value new value to be stored in this entry
			 * @return (Does not return)
			 * @throws UnsupportedOperationException always
			 */
			virtual V1 setValue(V1 value)
			{
				this->value = value;
				return value;
			}

			/**
			 * Compares the specified object with this entry for equality.
			 * Returns {@code true} if the given object is also a map entry and
			 * the two entries represent the same mapping.  More formally, two
			 * entries {@code e1} and {@code e2} represent the same mapping
			 * if<pre>
			 *   (e1.getKey()==null ?
			 *    e2.getKey()==null :
			 *    e1.getKey().equals(e2.getKey()))
			 *   &amp;&amp;
			 *   (e1.getValue()==null ?
			 *    e2.getValue()==null :
			 *    e1.getValue().equals(e2.getValue()))</pre>
			 * This ensures that the {@code equals} method works properly across
			 * different implementations of the {@code skiplist.Map.Entry} interface.
			 *
			 * @param o object to be compared for equality with this map entry
			 * @return {@code true} if the specified object is equal to this map
			 *         entry
			 * @see    #hashCode
			 */
			virtual bool equals(void *o)
			{
				if (!(static_cast<typename Map<K,V>::template Entry<K,V>*>(o) != NULL))
				{
					return false;
				}
				typename Map<K,V>::template Entry<K,V> *e = static_cast<typename Map<K,V>::template Entry<K,V>*>(o);
				return eq(key, e->getKey()) && eqValue(value, e->getValue());
			}

			/**
			 * Returns the hash code value for this map entry.  The hash code
			 * of a map entry {@code e} is defined to be: <pre>
			 *   (e.getKey()==null   ? 0 : e.getKey().hashCode()) ^
			 *   (e.getValue()==null ? 0 : e.getValue().hashCode())</pre>
			 * This ensures that {@code e1.equals(e2)} implies that
			 * {@code e1.hashCode()==e2.hashCode()} for any two Entries
			 * {@code e1} and {@code e2}, as required by the general
			 * contract of {@link Object#hashCode}.
			 *
			 * @return the hash code value for this map entry
			 * @see    #equals
			 */
			virtual int hashCode()
			{
				return (key == NULL ? 0 : key->hashCode()) ^ (value == NULL ? 0 : value->hashCode());
			}
		};

	};

}
