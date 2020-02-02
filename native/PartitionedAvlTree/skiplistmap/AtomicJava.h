#pragma once

#include <atomic>

namespace skiplist
{


	template<typename V>
	class AtomicJava
	{
	  private:
	   std::atomic<V> target;

	  public:
		  AtomicJava() {
			  target.store(0);
		  }
	  bool compareAndSet(V expect, V update) {
	    return target.compare_exchange_strong(expect, update);
	  }
	
	  bool weakCompareAndSet(V expect, V update) {
	    return target.compare_exchange_weak(expect, update);
	  }
	
	  void set(V newValue) {
	    target.store(newValue);
	  }
	
	  void lazySet(V newValue) {
	    target.store(newValue);
	  }
	
	  V get() {
	    return target.load();
	  }
	 
	};
}
