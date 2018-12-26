/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package it.unimi.dsi.fastutil.objects;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.AbstractLongCollection;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class AvlTree<K> extends AbstractObject2LongSortedMap<K> implements Serializable, Cloneable {
  protected transient AvlTree.Entry<K> tree;
  protected int count;
  protected transient AvlTree.Entry<K> firstEntry;
  protected transient AvlTree.Entry<K> lastEntry;
  protected transient volatile ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> entries;
  protected transient volatile ObjectSortedSet<K> keys;
  protected transient volatile LongCollection values;
  protected transient boolean modified;
  protected Comparator<? super K> storedComparator;
  protected transient Comparator<? super K> actualComparator;
  private static final long serialVersionUID = -7046029254386353129L;
  private static final boolean ASSERTS = false;
  private transient boolean[] dirPath;

  public AvlTree() {
    this.allocatePaths();
    this.tree = null;
    this.count = 0;
  }

  private void setActualComparator() {
    this.actualComparator = this.storedComparator;
  }

  public AvlTree(Comparator<? super K> c) {
    this();
    this.storedComparator = c;
    this.setActualComparator();
  }

  public AvlTree(Map<? extends K, ? extends Long> m) {
    this();
    this.putAll(m);
  }

  public AvlTree(SortedMap<K, Long> m) {
    this(m.comparator());
    this.putAll(m);
  }

  public AvlTree(Object2LongMap<? extends K> m) {
    this();
    this.putAll(m);
  }

  public AvlTree(Object2LongSortedMap<K> m) {
    this(m.comparator());
    this.putAll(m);
  }

  public AvlTree(K[] k, long[] v, Comparator<? super K> c) {
    this(c);
    if (k.length != v.length) {
      throw new IllegalArgumentException("The key array and the value array have different lengths (" + k.length + " and " + v.length + ")");
    } else {
      for(int i = 0; i < k.length; ++i) {
        this.put(k[i], v[i]);
      }

    }
  }

  public AvlTree(K[] k, long[] v) {
    this(k, v, (Comparator)null);
  }

  final int compare(K k1, K k2) {
    return this.actualComparator == null ? ((Comparable)((Comparable)k1)).compareTo(k2) : this.actualComparator.compare(k1, k2);
  }

  final AvlTree.Entry<K> findKey(K k) {
    AvlTree.Entry<K> e;
    int cmp;
    for(e = this.tree; e != null && (cmp = this.compare(k, e.key)) != 0; e = cmp < 0 ? e.left() : e.right()) {
      ;
    }

    return e;
  }

  public AvlTree.Entry<K> locateKey(K k) {
    AvlTree.Entry<K> e = this.tree;
    AvlTree.Entry<K> last = this.tree;

    int cmp;
    for(cmp = 0; e != null && (cmp = this.compare(k, e.key)) != 0; e = cmp < 0 ? e.left() : e.right()) {
      last = e;
    }

    return cmp == 0 ? e : last;
  }

  public Entry[] nextEntries(Entry[] currList, K k) {

    AvlTree.Entry<K> e = this.tree;
    AvlTree.Entry<K> last = this.tree;

    int cmp;
    for(cmp = 0; e != null && (cmp = this.compare(k, e.key)) != 0; e = cmp < 0 ? e.left() : e.right()) {
      last = e;
    }

    AvlTree.Entry<K> entry = cmp == 0 ? e : last;
    if (this.compare(entry.key, k) < 0) {
      entry = entry.next();
    }
    currList[0] = entry;
    AvlTree.Entry<K> curr = entry;
    if (curr != null) {
      for (int i = 0; i < currList.length - 1; i++) {
        curr = curr.next();
        if (curr == null) {
          break;
        }
        currList[i + 1] = curr;
      }
    }
    return currList;
  }

  private void allocatePaths() {
    this.dirPath = new boolean[48];
  }

  public long doPut(K k, long v) {
    this.modified = false;
    if (this.tree == null) {
      ++this.count;
      this.tree = this.lastEntry = this.firstEntry = new AvlTree.Entry(k, v);
      this.modified = true;
    } else {
      AvlTree.Entry<K> p = this.tree;
      AvlTree.Entry<K> q = null;
      AvlTree.Entry<K> y = this.tree;
      AvlTree.Entry<K> z = null;
      AvlTree.Entry<K> e = null;
      AvlTree.Entry<K> w = null;
      int i = 0;

      while(true) {
        int cmp;
        if ((cmp = this.compare(k, p.key)) == 0) {
          long oldValue = p.value;
          p.value = v;
          return oldValue;
        }

        if (p.balance() != 0) {
          i = 0;
          z = q;
          y = p;
        }

        if (this.dirPath[i++] = cmp > 0) {
          if (p.succ()) {
            ++this.count;
            e = new AvlTree.Entry(k, v);
            this.modified = true;
            if (p.right == null) {
              this.lastEntry = e;
            }

            e.left = p;
            e.right = p.right;
            p.right(e);
            break;
          }

          q = p;
          p = p.right;
        } else {
          if (p.pred()) {
            ++this.count;
            e = new AvlTree.Entry(k, v);
            this.modified = true;
            if (p.left == null) {
              this.firstEntry = e;
            }

            e.right = p;
            e.left = p.left;
            p.left(e);
            break;
          }

          q = p;
          p = p.left;
        }
      }

      p = y;

      for(i = 0; p != e; p = this.dirPath[i++] ? p.right : p.left) {
        if (this.dirPath[i]) {
          p.incBalance();
        } else {
          p.decBalance();
        }
      }

      AvlTree.Entry x;
      if (y.balance() == -2) {
        x = y.left;
        if (x.balance() == -1) {
          w = x;
          if (x.succ()) {
            x.succ(false);
            y.pred(x);
          } else {
            y.left = x.right;
          }

          x.right = y;
          x.balance(0);
          y.balance(0);
        } else {
          w = x.right;
          x.right = w.left;
          w.left = x;
          y.left = w.right;
          w.right = y;
          if (w.balance() == -1) {
            x.balance(0);
            y.balance(1);
          } else if (w.balance() == 0) {
            x.balance(0);
            y.balance(0);
          } else {
            x.balance(-1);
            y.balance(0);
          }

          w.balance(0);
          if (w.pred()) {
            x.succ(w);
            w.pred(false);
          }

          if (w.succ()) {
            y.pred(w);
            w.succ(false);
          }
        }
      } else {
        if (y.balance() != 2) {
          return this.defRetValue;
        }

        x = y.right;
        if (x.balance() == 1) {
          w = x;
          if (x.pred()) {
            x.pred(false);
            y.succ(x);
          } else {
            y.right = x.left;
          }

          x.left = y;
          x.balance(0);
          y.balance(0);
        } else {
          w = x.left;
          x.left = w.right;
          w.right = x;
          y.right = w.left;
          w.left = y;
          if (w.balance() == 1) {
            x.balance(0);
            y.balance(-1);
          } else if (w.balance() == 0) {
            x.balance(0);
            y.balance(0);
          } else {
            x.balance(1);
            y.balance(0);
          }

          w.balance(0);
          if (w.pred()) {
            y.succ(w);
            w.pred(false);
          }

          if (w.succ()) {
            x.pred(w);
            w.succ(false);
          }
        }
      }

      if (z == null) {
        this.tree = w;
      } else if (z.left == y) {
        z.left = w;
      } else {
        z.right = w;
      }
    }

    return this.defRetValue;
  }

  private AvlTree.Entry<K> parent(AvlTree.Entry<K> e) {
    if (e == this.tree) {
      return null;
    } else {
      AvlTree.Entry y = e;

      AvlTree.Entry x;
      AvlTree.Entry p;
      for(x = e; !y.succ(); y = y.right) {
        if (x.pred()) {
          p = x.left;
          if (p == null || p.right != e) {
            while(!y.succ()) {
              y = y.right;
            }

            p = y.right;
          }

          return p;
        }

        x = x.left;
      }

      p = y.right;
      if (p == null || p.left != e) {
        while(!x.pred()) {
          x = x.left;
        }

        p = x.left;
      }

      return p;
    }
  }

  public long removeLong(Object k) {
    this.modified = false;
    if (this.tree == null) {
      return this.defRetValue;
    } else {
      AvlTree.Entry<K> p = this.tree;
      AvlTree.Entry<K> q = null;
      boolean dir = false;
      Object kk = k;

      int cmp;
      while((cmp = this.compare((K) kk, p.key)) != 0) {
        if (dir = cmp > 0) {
          q = p;
          if ((p = p.right()) == null) {
            return this.defRetValue;
          }
        } else {
          q = p;
          if ((p = p.left()) == null) {
            return this.defRetValue;
          }
        }
      }

      if (p.left == null) {
        this.firstEntry = p.next();
      }

      if (p.right == null) {
        this.lastEntry = p.prev();
      }

      AvlTree.Entry y;
      AvlTree.Entry x;
      if (p.succ()) {
        if (p.pred()) {
          if (q != null) {
            if (dir) {
              q.succ(p.right);
            } else {
              q.pred(p.left);
            }
          } else {
            this.tree = dir ? p.right : p.left;
          }
        } else {
          p.prev().right = p.right;
          if (q != null) {
            if (dir) {
              q.right = p.left;
            } else {
              q.left = p.left;
            }
          } else {
            this.tree = p.left;
          }
        }
      } else {
        y = p.right;
        if (y.pred()) {
          y.left = p.left;
          y.pred(p.pred());
          if (!y.pred()) {
            y.prev().right = y;
          }

          if (q != null) {
            if (dir) {
              q.right = y;
            } else {
              q.left = y;
            }
          } else {
            this.tree = y;
          }

          y.balance(p.balance());
          q = y;
          dir = true;
        } else {
          while(true) {
            x = y.left;
            if (x.pred()) {
              if (x.succ()) {
                y.pred(x);
              } else {
                y.left = x.right;
              }

              x.left = p.left;
              if (!p.pred()) {
                p.prev().right = x;
                x.pred(false);
              }

              x.right = p.right;
              x.succ(false);
              if (q != null) {
                if (dir) {
                  q.right = x;
                } else {
                  q.left = x;
                }
              } else {
                this.tree = x;
              }

              x.balance(p.balance());
              q = y;
              dir = false;
              break;
            }

            y = x;
          }
        }
      }

      while(q != null) {
        y = q;
        q = this.parent(q);
        AvlTree.Entry w;
        if (!dir) {
          dir = q != null && q.left != y;
          y.incBalance();
          if (y.balance() == 1) {
            break;
          }

          if (y.balance() == 2) {
            x = y.right;
            if (x.balance() == -1) {
              w = x.left;
              x.left = w.right;
              w.right = x;
              y.right = w.left;
              w.left = y;
              if (w.balance() == 1) {
                x.balance(0);
                y.balance(-1);
              } else if (w.balance() == 0) {
                x.balance(0);
                y.balance(0);
              } else {
                x.balance(1);
                y.balance(0);
              }

              w.balance(0);
              if (w.pred()) {
                y.succ(w);
                w.pred(false);
              }

              if (w.succ()) {
                x.pred(w);
                w.succ(false);
              }

              if (q != null) {
                if (dir) {
                  q.right = w;
                } else {
                  q.left = w;
                }
              } else {
                this.tree = w;
              }
            } else {
              if (q != null) {
                if (dir) {
                  q.right = x;
                } else {
                  q.left = x;
                }
              } else {
                this.tree = x;
              }

              if (x.balance() == 0) {
                y.right = x.left;
                x.left = y;
                x.balance(-1);
                y.balance(1);
                break;
              }

              if (x.pred()) {
                y.succ(true);
                x.pred(false);
              } else {
                y.right = x.left;
              }

              x.left = y;
              y.balance(0);
              x.balance(0);
            }
          }
        } else {
          dir = q != null && q.left != y;
          y.decBalance();
          if (y.balance() == -1) {
            break;
          }

          if (y.balance() == -2) {
            x = y.left;
            if (x.balance() == 1) {
              w = x.right;
              x.right = w.left;
              w.left = x;
              y.left = w.right;
              w.right = y;
              if (w.balance() == -1) {
                x.balance(0);
                y.balance(1);
              } else if (w.balance() == 0) {
                x.balance(0);
                y.balance(0);
              } else {
                x.balance(-1);
                y.balance(0);
              }

              w.balance(0);
              if (w.pred()) {
                x.succ(w);
                w.pred(false);
              }

              if (w.succ()) {
                y.pred(w);
                w.succ(false);
              }

              if (q != null) {
                if (dir) {
                  q.right = w;
                } else {
                  q.left = w;
                }
              } else {
                this.tree = w;
              }
            } else {
              if (q != null) {
                if (dir) {
                  q.right = x;
                } else {
                  q.left = x;
                }
              } else {
                this.tree = x;
              }

              if (x.balance() == 0) {
                y.left = x.right;
                x.right = y;
                x.balance(1);
                y.balance(-1);
                break;
              }

              if (x.succ()) {
                y.pred(true);
                x.succ(false);
              } else {
                y.left = x.right;
              }

              x.right = y;
              y.balance(0);
              x.balance(0);
            }
          }
        }
      }

      this.modified = true;
      --this.count;
      return p.value;
    }
  }

  public Long put(K ok, Long ov) {
    long oldValue = this.doPut(ok, ov);
    return this.modified ? null : oldValue;
  }

  public Long remove(Object ok) {
    long oldValue = this.removeLong(ok);
    return this.modified ? oldValue : null;
  }

  public boolean containsValue(long v) {
    AvlTree<K>.ValueIterator i = new AvlTree.ValueIterator();
    int var6 = this.count;

    long ev;
    do {
      if (var6-- == 0) {
        return false;
      }

      ev = i.nextLong();
    } while(ev != v);

    return true;
  }

  public void clear() {
    this.count = 0;
    this.tree = null;
    this.entries = null;
    this.values = null;
    this.keys = null;
    this.firstEntry = this.lastEntry = null;
  }

  public boolean containsKey(Object k) {
    return this.findKey((K) k) != null;
  }

  public int size() {
    return this.count;
  }

  public boolean isEmpty() {
    return this.count == 0;
  }

  public long getLong(Object k) {
    AvlTree.Entry<K> e = this.findKey((K) k);
    return e == null ? this.defRetValue : e.value;
  }

  public Long get(Object ok) {
    AvlTree.Entry<K> e = this.findKey((K) ok);
    return e == null ? null : e.getValue();
  }

  public K firstKey() {
    if (this.tree == null) {
      throw new NoSuchElementException();
    } else {
      return this.firstEntry.key;
    }
  }

  public K lastKey() {
    if (this.tree == null) {
      throw new NoSuchElementException();
    } else {
      return this.lastEntry.key;
    }
  }

  public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> object2LongEntrySet() {
    if (this.entries == null) {
      this.entries = new AbstractObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>>() {
        final Comparator<? super it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> comparator = new Comparator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>>() {
          public int compare(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> x, it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> y) {
            return AvlTree.this.actualComparator.compare(x.getKey(), y.getKey());
          }
        };

        public Comparator<? super it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> comparator() {
          return this.comparator;
        }

        public ObjectBidirectionalIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> iterator() {
          return AvlTree.this.new EntryIterator();
        }

        public ObjectBidirectionalIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> iterator(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from) {
          return AvlTree.this.new EntryIterator(from.getKey());
        }

        public boolean contains(Object o) {
          if (!(o instanceof java.util.Map.Entry)) {
            return false;
          } else {
            java.util.Map.Entry<K, Long> e = (java.util.Map.Entry)o;
            AvlTree.Entry<K> f = AvlTree.this.findKey(e.getKey());
            return e.equals(f);
          }
        }

        public boolean remove(Object o) {
          if (!(o instanceof java.util.Map.Entry)) {
            return false;
          } else {
            java.util.Map.Entry<K, Long> e = (java.util.Map.Entry)o;
            AvlTree.Entry<K> f = AvlTree.this.findKey(e.getKey());
            if (f != null) {
              AvlTree.this.removeLong(f.key);
            }

            return f != null;
          }
        }

        public int size() {
          return AvlTree.this.count;
        }

        public void clear() {
          AvlTree.this.clear();
        }

        public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> first() {
          return AvlTree.this.firstEntry;
        }

        public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> last() {
          return AvlTree.this.lastEntry;
        }

        public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> subSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from, it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> to) {
          return AvlTree.this.subMap(from.getKey(), to.getKey()).object2LongEntrySet();
        }

        public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> headSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> to) {
          return AvlTree.this.headMap(to.getKey()).object2LongEntrySet();
        }

        public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> tailSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from) {
          return AvlTree.this.tailMap(from.getKey()).object2LongEntrySet();
        }
      };
    }

    return this.entries;
  }

  public ObjectSortedSet<K> keySet() {
    if (this.keys == null) {
      this.keys = new AvlTree.KeySet();
    }

    return this.keys;
  }

  public LongCollection values() {
    if (this.values == null) {
      this.values = new AbstractLongCollection() {
        public LongIterator iterator() {
          return AvlTree.this.new ValueIterator();
        }

        public boolean contains(long k) {
          return AvlTree.this.containsValue(k);
        }

        public int size() {
          return AvlTree.this.count;
        }

        public void clear() {
          AvlTree.this.clear();
        }
      };
    }

    return this.values;
  }

  public Comparator<? super K> comparator() {
    return this.actualComparator;
  }

  public Object2LongSortedMap<K> headMap(K to) {
    return new AvlTree.Submap((Object)null, true, to, false);
  }

  public Object2LongSortedMap<K> tailMap(K from) {
    return new AvlTree.Submap(from, false, (Object)null, true);
  }

  public Object2LongSortedMap<K> subMap(K from, K to) {
    return new AvlTree.Submap(from, false, to, false);
  }

  public AvlTree<K> clone() {
    AvlTree c;
    try {
      c = (AvlTree)super.clone();
    } catch (CloneNotSupportedException var7) {
      throw new InternalError();
    }

    c.keys = null;
    c.values = null;
    c.entries = null;
    c.allocatePaths();
    if (this.count == 0) {
      return c;
    } else {
      AvlTree.Entry<K> rp = new AvlTree.Entry();
      AvlTree.Entry<K> rq = new AvlTree.Entry();
      AvlTree.Entry<K> p = rp;
      rp.left(this.tree);
      AvlTree.Entry<K> q = rq;
      rq.pred((AvlTree.Entry)null);

      while(true) {
        AvlTree.Entry e;
        if (!p.pred()) {
          e = p.left.clone();
          e.pred(q.left);
          e.succ(q);
          q.left(e);
          p = p.left;
          q = q.left;
        } else {
          while(p.succ()) {
            p = p.right;
            if (p == null) {
              q.right = null;
              c.tree = rq.left;

              for(c.firstEntry = c.tree; c.firstEntry.left != null; c.firstEntry = c.firstEntry.left) {
                ;
              }

              for(c.lastEntry = c.tree; c.lastEntry.right != null; c.lastEntry = c.lastEntry.right) {
                ;
              }

              return c;
            }

            q = q.right;
          }

          p = p.right;
          q = q.right;
        }

        if (!p.succ()) {
          e = p.right.clone();
          e.succ(q.right);
          e.pred(q);
          q.right(e);
        }
      }
    }
  }

  private void writeObject(ObjectOutputStream s) throws IOException {
    int n = this.count;
    AvlTree<K>.EntryIterator i = new AvlTree.EntryIterator();
    s.defaultWriteObject();

    while(n-- != 0) {
      AvlTree.Entry<K> e = i.nextEntry();
      s.writeObject(e.key);
      s.writeLong(e.value);
    }

  }

  private AvlTree.Entry<K> readTree(ObjectInputStream s, int n, AvlTree.Entry<K> pred, AvlTree.Entry<K> succ) throws IOException, ClassNotFoundException {
    AvlTree.Entry top;
    if (n == 1) {
      top = new AvlTree.Entry(s.readObject(), s.readLong());
      top.pred(pred);
      top.succ(succ);
      return top;
    } else if (n == 2) {
      top = new AvlTree.Entry(s.readObject(), s.readLong());
      top.right(new AvlTree.Entry(s.readObject(), s.readLong()));
      top.right.pred(top);
      top.balance(1);
      top.pred(pred);
      top.right.succ(succ);
      return top;
    } else {
      int rightN = n / 2;
      int leftN = n - rightN - 1;
      top = new AvlTree.Entry();
      top.left(this.readTree(s, leftN, pred, top));
      top.key = s.readObject();
      top.value = s.readLong();
      top.right(this.readTree(s, rightN, top, succ));
      if (n == (n & -n)) {
        top.balance(1);
      }

      return top;
    }
  }

  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
    s.defaultReadObject();
    this.setActualComparator();
    this.allocatePaths();
    if (this.count != 0) {
      this.tree = this.readTree(s, this.count, (AvlTree.Entry)null, (AvlTree.Entry)null);

      AvlTree.Entry e;
      for(e = this.tree; e.left() != null; e = e.left()) {
        ;
      }

      this.firstEntry = e;

      for(e = this.tree; e.right() != null; e = e.right()) {
        ;
      }

      this.lastEntry = e;
    }

  }

  private static <K> int checkTree(AvlTree.Entry<K> e) {
    return 0;
  }

  private final class Submap extends AbstractObject2LongSortedMap<K> implements Serializable {
    private static final long serialVersionUID = -7046029254386353129L;
    K from;
    K to;
    boolean bottom;
    boolean top;
    protected transient volatile ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> entries;
    protected transient volatile ObjectSortedSet<K> keys;
    protected transient volatile LongCollection values;

    public Submap(K from, boolean bottom, K to, boolean top) {
      if (!bottom && !top && AvlTree.this.compare(from, to) > 0) {
        throw new IllegalArgumentException("Start key (" + from + ") is larger than end key (" + to + ")");
      } else {
        this.from = from;
        this.bottom = bottom;
        this.to = to;
        this.top = top;
        this.defRetValue = AvlTree.this.defRetValue;
      }
    }

    public void clear() {
      AvlTree.Submap.SubmapIterator i = new AvlTree.Submap.SubmapIterator();

      while(i.hasNext()) {
        i.nextEntry();
        i.remove();
      }

    }

    final boolean in(K k) {
      return (this.bottom || AvlTree.this.compare(k, this.from) >= 0) && (this.top || AvlTree.this.compare(k, this.to) < 0);
    }

    public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> object2LongEntrySet() {
      if (this.entries == null) {
        this.entries = new AbstractObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>>() {
          public ObjectBidirectionalIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> iterator() {
            return Submap.this.new SubmapEntryIterator();
          }

          public ObjectBidirectionalIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> iterator(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from) {
            return Submap.this.new SubmapEntryIterator(from.getKey());
          }

          public Comparator<? super it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> comparator() {
            return AvlTree.this.entrySet().comparator();
          }

          public boolean contains(Object o) {
            if (!(o instanceof java.util.Map.Entry)) {
              return false;
            } else {
              java.util.Map.Entry<K, Long> e = (java.util.Map.Entry)o;
              AvlTree.Entry<K> f = AvlTree.this.findKey(e.getKey());
              return f != null && Submap.this.in(f.key) && e.equals(f);
            }
          }

          public boolean remove(Object o) {
            if (!(o instanceof java.util.Map.Entry)) {
              return false;
            } else {
              java.util.Map.Entry<K, Long> e = (java.util.Map.Entry)o;
              AvlTree.Entry<K> f = AvlTree.this.findKey(e.getKey());
              if (f != null && Submap.this.in(f.key)) {
                Submap.this.removeLong(f.key);
              }

              return f != null;
            }
          }

          public int size() {
            int c = 0;
            ObjectBidirectionalIterator i = this.iterator();

            while(i.hasNext()) {
              ++c;
              i.next();
            }

            return c;
          }

          public boolean isEmpty() {
            return !(Submap.this.new SubmapIterator()).hasNext();
          }

          public void clear() {
            Submap.this.clear();
          }

          public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> first() {
            return Submap.this.firstEntry();
          }

          public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> last() {
            return Submap.this.lastEntry();
          }

          public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> subSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from, it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> to) {
            return Submap.this.subMap(from.getKey(), to.getKey()).object2LongEntrySet();
          }

          public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> headSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> to) {
            return Submap.this.headMap(to.getKey()).object2LongEntrySet();
          }

          public ObjectSortedSet<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> tailSet(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> from) {
            return Submap.this.tailMap(from.getKey()).object2LongEntrySet();
          }
        };
      }

      return this.entries;
    }

    public ObjectSortedSet<K> keySet() {
      if (this.keys == null) {
        this.keys = new AvlTree.Submap.KeySet();
      }

      return this.keys;
    }

    public LongCollection values() {
      if (this.values == null) {
        this.values = new AbstractLongCollection() {
          public LongIterator iterator() {
            return Submap.this.new SubmapValueIterator();
          }

          public boolean contains(long k) {
            return Submap.this.containsValue(k);
          }

          public int size() {
            return Submap.this.size();
          }

          public void clear() {
            Submap.this.clear();
          }
        };
      }

      return this.values;
    }

    public boolean containsKey(Object k) {
      return this.in((K) k) && AvlTree.this.containsKey(k);
    }

    public boolean containsValue(long v) {
      AvlTree.Submap.SubmapIterator i = new AvlTree.Submap.SubmapIterator();

      long ev;
      do {
        if (!i.hasNext()) {
          return false;
        }

        ev = i.nextEntry().value;
      } while(ev != v);

      return true;
    }

    public long getLong(Object k) {
      AvlTree.Entry e;
      return this.in((K) k) && (e = AvlTree.this.findKey((K) k)) != null ? e.value : this.defRetValue;
    }

    public Long get(Object ok) {
      AvlTree.Entry<K> e;
      return this.in((K) ok) && (e = AvlTree.this.findKey((K) ok)) != null ? e.getValue() : null;
    }

    public long doPut(K k, long v) {
      AvlTree.this.modified = false;
      if (!this.in(k)) {
        throw new IllegalArgumentException("Key (" + k + ") out of range [" + (this.bottom ? "-" : String.valueOf(this.from)) + ", " + (this.top ? "-" : String.valueOf(this.to)) + ")");
      } else {
        long oldValue = AvlTree.this.put(k, v);
        return AvlTree.this.modified ? this.defRetValue : oldValue;
      }
    }

    public Long put(K ok, Long ov) {
      long oldValue = this.doPut(ok, ov);
      return AvlTree.this.modified ? null : oldValue;
    }

    public long removeLong(Object k) {
      AvlTree.this.modified = false;
      if (!this.in((K) k)) {
        return this.defRetValue;
      } else {
        long oldValue = AvlTree.this.removeLong(k);
        return AvlTree.this.modified ? oldValue : this.defRetValue;
      }
    }

    public Long remove(Object ok) {
      long oldValue = this.removeLong(ok);
      return AvlTree.this.modified ? oldValue : null;
    }

    public int size() {
      AvlTree<K>.Submap.SubmapIterator i = new AvlTree.Submap.SubmapIterator();
      int n = 0;

      while(i.hasNext()) {
        ++n;
        i.nextEntry();
      }

      return n;
    }

    public boolean isEmpty() {
      return !(new AvlTree.Submap.SubmapIterator()).hasNext();
    }

    public Comparator<? super K> comparator() {
      return AvlTree.this.actualComparator;
    }

    public Object2LongSortedMap<K> headMap(K to) {
      if (this.top) {
        return AvlTree.this.new Submap(this.from, this.bottom, to, false);
      } else {
        return AvlTree.this.compare(to, this.to) < 0 ? AvlTree.this.new Submap(this.from, this.bottom, to, false) : this;
      }
    }

    public Object2LongSortedMap<K> tailMap(K from) {
      if (this.bottom) {
        return AvlTree.this.new Submap(from, false, this.to, this.top);
      } else {
        return AvlTree.this.compare(from, this.from) > 0 ? AvlTree.this.new Submap(from, false, this.to, this.top) : this;
      }
    }

    public Object2LongSortedMap<K> subMap(K from, K to) {
      if (this.top && this.bottom) {
        return AvlTree.this.new Submap(from, false, to, false);
      } else {
        if (!this.top) {
          to = AvlTree.this.compare(to, this.to) < 0 ? to : this.to;
        }

        if (!this.bottom) {
          from = AvlTree.this.compare(from, this.from) > 0 ? from : this.from;
        }

        return !this.top && !this.bottom && from == this.from && to == this.to ? this : AvlTree.this.new Submap(from, false, to, false);
      }
    }

    public AvlTree.Entry<K> firstEntry() {
      if (AvlTree.this.tree == null) {
        return null;
      } else {
        AvlTree.Entry e;
        if (this.bottom) {
          e = AvlTree.this.firstEntry;
        } else {
          e = AvlTree.this.locateKey(this.from);
          if (AvlTree.this.compare((K) e.key, this.from) < 0) {
            e = e.next();
          }
        }

        return e != null && (this.top || AvlTree.this.compare((K) e.key, this.to) < 0) ? e : null;
      }
    }

    public AvlTree.Entry<K> lastEntry() {
      if (AvlTree.this.tree == null) {
        return null;
      } else {
        AvlTree.Entry<K> e;
        if (this.top) {
          e = AvlTree.this.lastEntry;
        } else {
          e = AvlTree.this.locateKey(this.to);
          if (AvlTree.this.compare(e.key, this.to) >= 0) {
            e = e.prev();
          }
        }

        return e != null && (this.bottom || AvlTree.this.compare(e.key, this.from) >= 0) ? e : null;
      }
    }

    public K firstKey() {
      AvlTree.Entry<K> e = this.firstEntry();
      if (e == null) {
        throw new NoSuchElementException();
      } else {
        return e.key;
      }
    }

    public K lastKey() {
      AvlTree.Entry<K> e = this.lastEntry();
      if (e == null) {
        throw new NoSuchElementException();
      } else {
        return e.key;
      }
    }

    private final class SubmapValueIterator extends AvlTree<K>.Submap.SubmapIterator implements LongListIterator {
      private SubmapValueIterator() {
        super();
      }

      public long nextLong() {
        return this.nextEntry().value;
      }

      public long previousLong() {
        return this.previousEntry().value;
      }

      public void set(long v) {
        throw new UnsupportedOperationException();
      }

      public void add(long v) {
        throw new UnsupportedOperationException();
      }

      public Long next() {
        return this.nextEntry().value;
      }

      public Long previous() {
        return this.previousEntry().value;
      }

      public void set(Long ok) {
        throw new UnsupportedOperationException();
      }

      public void add(Long ok) {
        throw new UnsupportedOperationException();
      }
    }

    private final class SubmapKeyIterator extends AvlTree<K>.Submap.SubmapIterator implements ObjectListIterator<K> {
      public SubmapKeyIterator() {
        super();
      }

      public SubmapKeyIterator(K from) {
        super(from);
      }

      public K next() {
        return this.nextEntry().key;
      }

      public K previous() {
        return this.previousEntry().key;
      }

      public void set(K k) {
        throw new UnsupportedOperationException();
      }

      public void add(K k) {
        throw new UnsupportedOperationException();
      }
    }

    private class SubmapEntryIterator extends AvlTree<K>.Submap.SubmapIterator implements ObjectListIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> {
      SubmapEntryIterator() {
        super();
      }

      SubmapEntryIterator(K k) {
        super(k);
      }

      public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> next() {
        return this.nextEntry();
      }

      public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> previous() {
        return this.previousEntry();
      }

      public void set(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> ok) {
        throw new UnsupportedOperationException();
      }

      public void add(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> ok) {
        throw new UnsupportedOperationException();
      }
    }

    private class SubmapIterator extends AvlTree<K>.TreeIterator {
      SubmapIterator() {
        super();
        this.next = Submap.this.firstEntry();
      }

      SubmapIterator(K k) {
        this();
        if (this.next != null) {
          if (!Submap.this.bottom && AvlTree.this.compare(k, this.next.key) < 0) {
            this.prev = null;
          } else if (!Submap.this.top && AvlTree.this.compare(k, (this.prev = Submap.this.lastEntry()).key) >= 0) {
            this.next = null;
          } else {
            this.next = AvlTree.this.locateKey(k);
            if (AvlTree.this.compare(this.next.key, k) <= 0) {
              this.prev = this.next;
              this.next = this.next.next();
            } else {
              this.prev = this.next.prev();
            }
          }
        }

      }

      void updatePrevious() {
        this.prev = this.prev.prev();
        if (!Submap.this.bottom && this.prev != null && AvlTree.this.compare(this.prev.key, Submap.this.from) < 0) {
          this.prev = null;
        }

      }

      void updateNext() {
        this.next = this.next.next();
        if (!Submap.this.top && this.next != null && AvlTree.this.compare(this.next.key, Submap.this.to) >= 0) {
          this.next = null;
        }

      }
    }

    private class KeySet extends AbstractObject2LongSortedMap<K>.KeySet {
      private KeySet() {
        super();
        //TODO: what about this?
        //super(Submap.this);
      }

      public ObjectBidirectionalIterator<K> iterator() {
        return Submap.this.new SubmapKeyIterator();
      }

      public ObjectBidirectionalIterator<K> iterator(K from) {
        return Submap.this.new SubmapKeyIterator(from);
      }
    }
  }

  private final class ValueIterator extends AvlTree<K>.TreeIterator implements LongListIterator {
    private ValueIterator() {
      super();
    }

    public long nextLong() {
      return this.nextEntry().value;
    }

    public long previousLong() {
      return this.previousEntry().value;
    }

    public void set(long v) {
      throw new UnsupportedOperationException();
    }

    public void add(long v) {
      throw new UnsupportedOperationException();
    }

    public Long next() {
      return this.nextEntry().value;
    }

    public Long previous() {
      return this.previousEntry().value;
    }

    public void set(Long ok) {
      throw new UnsupportedOperationException();
    }

    public void add(Long ok) {
      throw new UnsupportedOperationException();
    }
  }

  private class KeySet extends AbstractObject2LongSortedMap<K>.KeySet {
    private KeySet() {
      super();
      //TODO: what about this?
      //super(AvlTree.this);
    }

    public ObjectBidirectionalIterator<K> iterator() {
      return AvlTree.this.new KeyIterator();
    }

    public ObjectBidirectionalIterator<K> iterator(K from) {
      return AvlTree.this.new KeyIterator(from);
    }
  }

  private final class KeyIterator extends AvlTree<K>.TreeIterator implements ObjectListIterator<K> {
    public KeyIterator() {
      super();
    }

    public KeyIterator(K k) {
      super(k);
    }

    public K next() {
      return this.nextEntry().key;
    }

    public K previous() {
      return this.previousEntry().key;
    }

    public void set(K k) {
      throw new UnsupportedOperationException();
    }

    public void add(K k) {
      throw new UnsupportedOperationException();
    }
  }

  private class EntryIterator extends AvlTree<K>.TreeIterator implements ObjectListIterator<it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K>> {
    EntryIterator() {
      super();
    }

    EntryIterator(K k) {
      super(k);
    }

    public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> next() {
      return this.nextEntry();
    }

    public it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> previous() {
      return this.previousEntry();
    }

    public void set(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> ok) {
      throw new UnsupportedOperationException();
    }

    public void add(it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> ok) {
      throw new UnsupportedOperationException();
    }
  }

  private class TreeIterator {
    AvlTree.Entry<K> prev;
    AvlTree.Entry<K> next;
    AvlTree.Entry<K> curr;
    int index = 0;

    TreeIterator() {
      this.next = AvlTree.this.firstEntry;
    }

    TreeIterator(K k) {
      if ((this.next = AvlTree.this.locateKey(k)) != null) {
        if (AvlTree.this.compare(this.next.key, k) <= 0) {
          this.prev = this.next;
          this.next = this.next.next();
        } else {
          this.prev = this.next.prev();
        }
      }

    }

    public boolean hasNext() {
      return this.next != null;
    }

    public boolean hasPrevious() {
      return this.prev != null;
    }

    void updateNext() {
      this.next = this.next.next();
    }

    AvlTree.Entry<K> nextEntry() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      } else {
        this.curr = this.prev = this.next;
        ++this.index;
        this.updateNext();
        return this.curr;
      }
    }

    void updatePrevious() {
      this.prev = this.prev.prev();
    }

    AvlTree.Entry<K> previousEntry() {
      if (!this.hasPrevious()) {
        throw new NoSuchElementException();
      } else {
        this.curr = this.next = this.prev;
        --this.index;
        this.updatePrevious();
        return this.curr;
      }
    }

    public int nextIndex() {
      return this.index;
    }

    public int previousIndex() {
      return this.index - 1;
    }

    public void remove() {
      if (this.curr == null) {
        throw new IllegalStateException();
      } else {
        if (this.curr == this.prev) {
          --this.index;
        }

        this.next = this.prev = this.curr;
        this.updatePrevious();
        this.updateNext();
        AvlTree.this.removeLong(this.curr.key);
        this.curr = null;
      }
    }

    public int skip(int n) {
      int i = n;

      while(i-- != 0 && this.hasNext()) {
        this.nextEntry();
      }

      return n - i - 1;
    }

    public int back(int n) {
      int i = n;

      while(i-- != 0 && this.hasPrevious()) {
        this.previousEntry();
      }

      return n - i - 1;
    }
  }

  public static final class Entry<K> implements Cloneable, it.unimi.dsi.fastutil.objects.Object2LongMap.Entry<K> {
    private static final int SUCC_MASK = -2147483648;
    private static final int PRED_MASK = 1073741824;
    private static final int BALANCE_MASK = 255;
    K key;
    long value;
    AvlTree.Entry<K> left;
    AvlTree.Entry<K> right;
    int info;

    Entry() {
    }

    Entry(K k, long v) {
      this.key = k;
      this.value = v;
      this.info = -1073741824;
    }

    AvlTree.Entry<K> left() {
      return (this.info & 1073741824) != 0 ? null : this.left;
    }

    AvlTree.Entry<K> right() {
      return (this.info & -2147483648) != 0 ? null : this.right;
    }

    boolean pred() {
      return (this.info & 1073741824) != 0;
    }

    boolean succ() {
      return (this.info & -2147483648) != 0;
    }

    void pred(boolean pred) {
      if (pred) {
        this.info |= 1073741824;
      } else {
        this.info &= -1073741825;
      }

    }

    void succ(boolean succ) {
      if (succ) {
        this.info |= -2147483648;
      } else {
        this.info &= 2147483647;
      }

    }

    void pred(AvlTree.Entry<K> pred) {
      this.info |= 1073741824;
      this.left = pred;
    }

    void succ(AvlTree.Entry<K> succ) {
      this.info |= -2147483648;
      this.right = succ;
    }

    void left(AvlTree.Entry<K> left) {
      this.info &= -1073741825;
      this.left = left;
    }

    void right(AvlTree.Entry<K> right) {
      this.info &= 2147483647;
      this.right = right;
    }

    int balance() {
      return (byte)this.info;
    }

    void balance(int level) {
      this.info &= -256;
      this.info |= level & 255;
    }

    void incBalance() {
      this.info = this.info & -256 | (byte)this.info + 1 & 255;
    }

    protected void decBalance() {
      this.info = this.info & -256 | (byte)this.info - 1 & 255;
    }

    AvlTree.Entry<K> next() {
      AvlTree.Entry<K> next = this.right;
      if ((this.info & -2147483648) == 0) {
        while((next.info & 1073741824) == 0) {
          next = next.left;
        }
      }

      return next;
    }

    AvlTree.Entry<K> prev() {
      AvlTree.Entry<K> prev = this.left;
      if ((this.info & 1073741824) == 0) {
        while((prev.info & -2147483648) == 0) {
          prev = prev.right;
        }
      }

      return prev;
    }

    public K getKey() {
      return this.key;
    }

    public Long getValue() {
      return this.value;
    }

    public long getLongValue() {
      return this.value;
    }

    public long setValue(long value) {
      long oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    public Long setValue(Long value) {
      return this.setValue(value);
    }

    public AvlTree.Entry<K> clone() {
      AvlTree.Entry c;
      try {
        c = (AvlTree.Entry)super.clone();
      } catch (CloneNotSupportedException var3) {
        throw new InternalError();
      }

      c.key = this.key;
      c.value = this.value;
      c.info = this.info;
      return c;
    }

    public boolean equals(Object o) {
      if (!(o instanceof java.util.Map.Entry)) {
        return false;
      } else {
        boolean var10000;
        label21: {
          java.util.Map.Entry<K, Long> e = (java.util.Map.Entry)o;
          if (this.key == null) {
            if (e.getKey() != null) {
              break label21;
            }
          } else if (!this.key.equals(e.getKey())) {
            break label21;
          }

          if (this.value == (Long)e.getValue()) {
            var10000 = true;
            return var10000;
          }
        }

        var10000 = false;
        return var10000;
      }
    }

    public int hashCode() {
      return this.key.hashCode() ^ HashCommon.long2int(this.value);
    }

    public String toString() {
      return this.key + "=>" + this.value;
    }
  }
}
