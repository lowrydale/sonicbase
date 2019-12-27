#pragma once

#include "Serializable.h"
#include <string>
#include <stdexcept>
#include <memory>

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

namespace java::util::concurrent::atomic
{
	using sun::misc::Unsafe;

	/**
	 * A {@code long} value that may be updated atomically.  See the
	 * {@link java.util.concurrent.atomic} package specification for
	 * description of the properties of atomic variables. An
	 * {@code AtomicLong} is used in applications such as atomically
	 * incremented sequence numbers, and cannot be used as a replacement
	 * for a {@link java.lang.Long}. However, this class does extend
	 * {@code Number} to allow uniform access by tools and utilities that
	 * deal with numerically-based classes.
	 *
	 * @since 1.5
	 * @author Doug Lea
	 */
	class AtomicLong : public Number, public java::io::Serializable
	{
	private:
		static constexpr long long serialVersionUID = 1927816293512124184LL;

		// setup to use Unsafe.compareAndSwapLong for updates
		static const std::shared_ptr<Unsafe> unsafe;
		static const long long valueOffset = 0;

		/**
		 * Records whether the underlying JVM supports lockless
		 * compareAndSwap for longs. While the Unsafe.compareAndSwapLong
		 * method works in either case, some constructions should be
		 * handled at Java level to avoid locking user-visible locks.
		 */
	public:
		static const bool VM_SUPPORTS_LONG_CAS = VMSupportsCS8();

		/**
		 * Returns whether underlying JVM supports lockless CompareAndSet
		 * for longs. Called only once and cached in VM_SUPPORTS_LONG_CAS.
		 */
//JAVA TO C++ CONVERTER NOTE: The following Java 'native' declaration was converted using the Microsoft-specific __declspec(dllimport):
//		private static native boolean VMSupportsCS8();
	private:
		__declspec(dllimport) static bool VMSupportsCS8();

		private:
			class StaticConstructor : public std::enable_shared_from_this<StaticConstructor>
			{
			public:
				StaticConstructor();
			};

		private:
			static AtomicLong::StaticConstructor staticConstructor;


//JAVA TO C++ CONVERTER TODO TASK: 'volatile' has a different meaning in C++:
//ORIGINAL LINE: private volatile long value;
		long long value = 0;

		/**
		 * Creates a new AtomicLong with the given initial value.
		 *
		 * @param initialValue the initial value
		 */
	public:
		AtomicLong(long long initialValue);

		/**
		 * Creates a new AtomicLong with initial value {@code 0}.
		 */
		AtomicLong();

		/**
		 * Gets the current value.
		 *
		 * @return the current value
		 */
		long long get();

		/**
		 * Sets to the given value.
		 *
		 * @param newValue the new value
		 */
		void set(long long newValue);

		/**
		 * Eventually sets to the given value.
		 *
		 * @param newValue the new value
		 * @since 1.6
		 */
		void lazySet(long long newValue);

		/**
		 * Atomically sets to the given value and returns the old value.
		 *
		 * @param newValue the new value
		 * @return the previous value
		 */
		long long getAndSet(long long newValue);

		/**
		 * Atomically sets the value to the given updated value
		 * if the current value {@code ==} the expected value.
		 *
		 * @param expect the expected value
		 * @param update the new value
		 * @return true if successful. False return indicates that
		 * the actual value was not equal to the expected value.
		 */
		bool compareAndSet(long long expect, long long update);

		/**
		 * Atomically sets the value to the given updated value
		 * if the current value {@code ==} the expected value.
		 *
		 * <p>May <a href="package-summary.html#Spurious">fail spuriously</a>
		 * and does not provide ordering guarantees, so is only rarely an
		 * appropriate alternative to {@code compareAndSet}.
		 *
		 * @param expect the expected value
		 * @param update the new value
		 * @return true if successful.
		 */
		bool weakCompareAndSet(long long expect, long long update);

		/**
		 * Atomically increments by one the current value.
		 *
		 * @return the previous value
		 */
		long long getAndIncrement();

		/**
		 * Atomically decrements by one the current value.
		 *
		 * @return the previous value
		 */
		long long getAndDecrement();

		/**
		 * Atomically adds the given value to the current value.
		 *
		 * @param delta the value to add
		 * @return the previous value
		 */
		long long getAndAdd(long long delta);

		/**
		 * Atomically increments by one the current value.
		 *
		 * @return the updated value
		 */
		long long incrementAndGet();

		/**
		 * Atomically decrements by one the current value.
		 *
		 * @return the updated value
		 */
		long long decrementAndGet();

		/**
		 * Atomically adds the given value to the current value.
		 *
		 * @param delta the value to add
		 * @return the updated value
		 */
		long long addAndGet(long long delta);

		/**
		 * Returns the String representation of the current value.
		 * @return the String representation of the current value.
		 */
		virtual std::wstring toString();


		virtual int intValue();

		virtual long long longValue();

		virtual float floatValue();

		virtual double doubleValue();


protected:
		std::shared_ptr<AtomicLong> shared_from_this()
		{
			return std::static_pointer_cast<AtomicLong>(Number::shared_from_this());
		}
	};

}
