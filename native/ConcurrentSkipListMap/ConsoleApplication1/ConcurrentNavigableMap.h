#pragma once

#include "ConcurrentMap.h"
#include "NavigableMap.h"

//JAVA TO C++ CONVERTER NOTE: Forward class declarations:
namespace skiplist { template<typename K, typename V>class NavigableMap; }
namespace skiplist { template<typename K, typename V>class SortedMap; }
namespace skiplist { template<typename E>class NavigableSet; }
namespace skiplist { template<typename E>class Set; }

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
	 * A {@link ConcurrentMap} supporting {@link NavigableMap} operations,
	 * and recursively so for its navigable sub-maps.
	 *
	 * <p>This interface is a member of the
	 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
	 * Java Collections Framework</a>.
	 *
	 * @author Doug Lea
	 * @param <K> the type of keys maintained by this map
	 * @param <V> the type of mapped values
	 * @since 1.6
	 */
	template<typename K, typename V>
	class ConcurrentNavigableMap : public ConcurrentMap<K, V>, public NavigableMap<K, V>
	{
		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
	public:
		virtual ConcurrentNavigableMap<K, V> *subMap(K fromKey, bool fromInclusive, K toKey, bool toInclusive) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *headMap(K toKey, bool inclusive) = 0;


		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *tailMap(K fromKey, bool inclusive) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *subMap(K fromKey, K toKey) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *headMap(K toKey) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual ConcurrentNavigableMap<K, V> *tailMap(K fromKey) = 0;

		/**
		 * Returns a reverse order view of the mappings contained in this map.
		 * The descending map is backed by this map, so changes to the map are
		 * reflected in the descending map, and vice-versa.
		 *
		 * <p>The returned map has an ordering equivalent to
		 * <tt>{@link Collections#reverseOrder(Comparator) Collections.reverseOrder}(comparator())</tt>.
		 * The expression {@code m.descendingMap().descendingMap()} returns a
		 * view of {@code m} essentially equivalent to {@code m}.
		 *
		 * @return a reverse order view of this map
		 */
		virtual ConcurrentNavigableMap<K, V> *descendingMap() = 0;

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
		 * @return a navigable set view of the keys in this map
		 */
		virtual NavigableSet<K> *navigableKeySet() = 0;

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
		virtual Set<K> *keySet() = 0;

		/**
		 * Returns a reverse order {@link NavigableSet} view of the keys contained in this map.
		 * The set's beginIterator returns the keys in descending order.
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
		 * @return a reverse order navigable set view of the keys in this map
		 */
		virtual NavigableSet<K> *descendingKeySet() = 0;
	};

}
