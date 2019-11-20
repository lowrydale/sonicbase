#pragma once

#include "ConcurrentMap.h"
#include "NavigableMap.h"
#include "NavigableSet.h"
#include <memory>

/*
 * %W% %E%
 *
 * Copyright (c) 2006, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

namespace java::util::concurrent
{

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
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> subMap(K fromKey, bool fromInclusive, K toKey, bool toInclusive) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> headMap(K toKey, bool inclusive) = 0;


		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> tailMap(K fromKey, bool inclusive) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> subMap(K fromKey, K toKey) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> headMap(K toKey) = 0;

		/**
		 * @throws ClassCastException       {@inheritDoc}
		 * @throws NullPointerException     {@inheritDoc}
		 * @throws IllegalArgumentException {@inheritDoc}
		 */
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> tailMap(K fromKey) = 0;

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
		virtual std::shared_ptr<ConcurrentNavigableMap<K, V>> descendingMap() = 0;

		/**
		 * Returns a {@link NavigableSet} view of the keys contained in this map.
		 * The set's iterator returns the keys in ascending order.
		 * The set is backed by the map, so changes to the map are
		 * reflected in the set, and vice-versa.  The set supports element
		 * removal, which removes the corresponding mapping from the map,
		 * via the {@code Iterator.remove}, {@code Set.remove},
		 * {@code removeAll}, {@code retainAll}, and {@code clear}
		 * operations.  It does not support the {@code add} or {@code addAll}
		 * operations.
		 *
		 * <p>The view's {@code iterator} is a "weakly consistent" iterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the iterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 *
		 * @return a navigable set view of the keys in this map
		 */
		virtual std::shared_ptr<NavigableSet<K>> navigableKeySet() = 0;

		/**
		 * Returns a {@link NavigableSet} view of the keys contained in this map.
		 * The set's iterator returns the keys in ascending order.
		 * The set is backed by the map, so changes to the map are
		 * reflected in the set, and vice-versa.  The set supports element
		 * removal, which removes the corresponding mapping from the map,
		 * via the {@code Iterator.remove}, {@code Set.remove},
		 * {@code removeAll}, {@code retainAll}, and {@code clear}
		 * operations.  It does not support the {@code add} or {@code addAll}
		 * operations.
		 *
		 * <p>The view's {@code iterator} is a "weakly consistent" iterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the iterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 *
		 * <p>This method is equivalent to method {@code navigableKeySet}.
		 *
		 * @return a navigable set view of the keys in this map
		 */
		virtual std::shared_ptr<NavigableSet<K>> keySet() = 0;

		/**
		 * Returns a reverse order {@link NavigableSet} view of the keys contained in this map.
		 * The set's iterator returns the keys in descending order.
		 * The set is backed by the map, so changes to the map are
		 * reflected in the set, and vice-versa.  The set supports element
		 * removal, which removes the corresponding mapping from the map,
		 * via the {@code Iterator.remove}, {@code Set.remove},
		 * {@code removeAll}, {@code retainAll}, and {@code clear}
		 * operations.  It does not support the {@code add} or {@code addAll}
		 * operations.
		 *
		 * <p>The view's {@code iterator} is a "weakly consistent" iterator
		 * that will never throw {@link ConcurrentModificationException},
		 * and guarantees to traverse elements as they existed upon
		 * construction of the iterator, and may (but is not guaranteed to)
		 * reflect any modifications subsequent to construction.
		 *
		 * @return a reverse order navigable set view of the keys in this map
		 */
		virtual std::shared_ptr<NavigableSet<K>> descendingKeySet() = 0;
	};

}
