/* 
 * https://attractivechaos.wordpress.com/2018/04/19/a-single-head->r-generic-intrusive-avl-tree-in-ansi-c/
 * 
 * The MIT License
   Copyright (c) 2018 by Attractive Chaos <attractor@live.co.uk>
   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:
   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

/* An example:
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kavl.h"
struct my_node {
  char key;
  KAVL_head->struct my_node) head->
};
#define my_cmp(p, q) (((q)->key < (p)->key) - ((p)->key < (q)->key))
KAVL_INIT(my, struct my_node, head-> my_cmp)
int main(void) {
  const char *str = "MNOLKQOPHIA"; // from wiki, except a duplicate
  struct my_node *root = 0;
  int i, l = strlen(str);
  for (i = 0; i < l; ++i) {        // insert in the input order
    struct my_node *q, *p = malloc(sizeof(*p));
    p->key = str[i];
    q = kavl_insert(my, &root, p, 0);
    if (p != q) free(p);           // if already present, free
  }
  kavl_itr_t(my) itr;
  kavl_itr_first(my, root, &itr);  // place at first
  do {                             // traverse
    const struct my_node *p = kavl_at(&itr);
    putchar(p->key);
    free((void*)p);                // free node
  } while (kavl_itr_next(my, &itr));
  putchar('\n');
  return 0;
}
*/

#ifndef KAVL_H
#define KAVL_H
#include "sonicbase.h"

#ifdef __STRICT_ANSI__
#define inline __inline__
#endif

#define KAVL_MAX_DEPTH 64

	
class head_t;


class my_node {
public:
	my_node();
	~my_node();

	Key *key;
	head_t *head;
	uint64_t value;
};

class head_t {
public:
	my_node *p[2];
	signed char balance; /* balance factor */
	unsigned size; /* #elements in subtree */
};
my_node::my_node() {
	head = new head_t;
}
my_node::~my_node() {
	delete head;
}

struct kavl_itr {
	const my_node *stack[KAVL_MAX_DEPTH], **top, *right, *left;/* _right_ points to the right child of *top */
};


#define kavl_size(head, p) ((p)? (p)->head->size : 0)
#define kavl_size_child(head, q, i) ((q)->head->p[(i)]? (q)->head->p[(i)]->head->size : 0)


my_node *kavl_find(const my_node *root, const my_node *x, unsigned *cnt_, KAVLComparator *comparator) { \
		const my_node *p = root; \
		unsigned cnt = 0; \
		while (p != 0) { \
			int cmp; \
			cmp = comparator->compare((void*)x, (void*)p); \
			if (cmp >= 0) cnt += kavl_size_child(head, p, 0) + 1; \
			if (cmp < 0) p = p->head->p[0]; \
			else if (cmp > 0) p = p->head->p[1]; \
			else break; \
		} \
		if (cnt_) *cnt_ = cnt; \
		return (my_node*)p; \
	}

	/* one rotation: (a,(b,c)q)p => ((a,b)p,c)q */ \
	static inline my_node *kavl_rotate1(my_node *p, int dir) { /* dir=0 to left; dir=1 to right */ \
		int opp = 1 - dir; /* opposite direction */ \
		my_node *q = p->head->p[opp]; \
		unsigned size_p = p->head->size; \
		p->head->size -= q->head->size - kavl_size_child(head, q, dir); \
		q->head->size = size_p; \
		p->head->p[opp] = q->head->p[dir]; \
		q->head->p[dir] = p; \
		return q; \
	} \
	/* two consecutive rotations: (a,((b,c)r,d)q)p => ((a,b)p,(c,d)q)r */ \
	static inline my_node *kavl_rotate2(my_node *p, int dir) { \
		int b1, opp = 1 - dir; \
		my_node *q = p->head->p[opp], *r = q->head->p[dir]; \
		unsigned size_x_dir = kavl_size_child(head, r, dir); \
		r->head->size = p->head->size; \
		p->head->size -= q->head->size - size_x_dir; \
		q->head->size -= size_x_dir + 1; \
		p->head->p[opp] = r->head->p[dir]; \
		r->head->p[dir] = p; \
		q->head->p[dir] = r->head->p[opp]; \
		r->head->p[opp] = q; \
		b1 = dir == 0? +1 : -1; \
		if (r->head->balance == b1) q->head->balance = 0, p->head->balance = -b1; \
		else if (r->head->balance == 0) q->head->balance = p->head->balance = 0; \
		else q->head->balance = b1, p->head->balance = 0; \
		r->head->balance = 0; \
		return r; \
	}

	my_node *kavl_insert(my_node **root_, my_node *x, KAVLComparator *comparator, unsigned *cnt_) { \
		unsigned char stack[KAVL_MAX_DEPTH]; \
		my_node *path[KAVL_MAX_DEPTH]; \
		my_node *bp, *bq; \
		my_node *p, *q, *r = 0; /* _r_ is potentially the new root */ \
		int i, which = 0, top, b1, path_len; \
		unsigned cnt = 0; \
		bp = *root_, bq = 0; \
		/* find the insertion location */ \
		for (p = bp, q = bq, top = path_len = 0; p; q = p, p = p->head->p[which]) { \
			int cmp; \
			cmp = comparator->compare(x->key, p->key); \
			if (cmp >= 0) cnt += kavl_size_child(head, p, 0) + 1; \
			if (cmp == 0) { \
				if (cnt_) *cnt_ = cnt; \
				return p; \
			} \
			if (p->head->balance != 0) \
				bq = q, bp = p, top = 0; \
			stack[top++] = which = (cmp > 0); \
			path[path_len++] = p; \
		} \
		if (cnt_) *cnt_ = cnt; \
		x->head->balance = 0, x->head->size = 1, x->head->p[0] = x->head->p[1] = 0; \
		if (q == 0) *root_ = x; \
		else q->head->p[which] = x; \
		if (bp == 0) return x; \
		for (i = 0; i < path_len; ++i) ++path[i]->head->size; \
		for (p = bp, top = 0; p != x; p = p->head->p[stack[top]], ++top) /* update balance factors */ \
			if (stack[top] == 0) --p->head->balance; \
			else ++p->head->balance; \
		if (bp->head->balance > -2 && bp->head->balance < 2) return x; /* no re-balance needed */ \
		/* re-balance */ \
		which = (bp->head->balance < 0); \
		b1 = which == 0? +1 : -1; \
		q = bp->head->p[1 - which]; \
		if (q->head->balance == b1) { \
			r = kavl_rotate1(bp, which); \
			q->head->balance = bp->head->balance = 0; \
		} else r = kavl_rotate2(bp, which); \
		if (bq == 0) *root_ = r; \
		else bq->head->p[bp != bq->head->p[0]] = r; \
		return x; \
	}

my_node *kavl_erase(my_node **root_, const my_node *x, unsigned *cnt_, KAVLComparator *comparator) { \
			my_node *p, *path[KAVL_MAX_DEPTH], fake; \
		unsigned char dir[KAVL_MAX_DEPTH]; \
		int i, d = 0, cmp; \
		unsigned cnt = 0; \
		fake.head->p[0] = *root_, fake.head->p[1] = 0; \
		if (cnt_) *cnt_ = 0; \
		if (x) { \
			for (cmp = -1, p = &fake; cmp; cmp = comparator->compare(x->key, p->key)) { \
				int which = (cmp > 0); \
				if (cmp > 0) cnt += kavl_size_child(head, p, 0) + 1; \
				dir[d] = which; \
				path[d++] = p; \
				p = p->head->p[which]; \
				if (p == 0) { \
					if (cnt_) *cnt_ = 0; \
					return 0; \
				} \
			} \
			cnt += kavl_size_child(head, p, 0) + 1; /* because p==x is not counted */ \
		} else { \
			for (p = &fake, cnt = 1; p; p = p->head->p[0]) \
				dir[d] = 0, path[d++] = p; \
			p = path[--d]; \
		} \
		if (cnt_) *cnt_ = cnt; \
		for (i = 1; i < d; ++i) --path[i]->head->size; \
		if (p->head->p[1] == 0) { /* ((1,.)2,3)4 => (1,3)4; p=2 */ \
			path[d-1]->head->p[dir[d-1]] = p->head->p[0]; \
		} else { \
			my_node *q = p->head->p[1]; \
			if (q->head->p[0] == 0) { /* ((1,2)3,4)5 => ((1)2,4)5; p=3 */ \
				q->head->p[0] = p->head->p[0]; \
				q->head->balance = p->head->balance; \
				path[d-1]->head->p[dir[d-1]] = q; \
				path[d] = q, dir[d++] = 1; \
				q->head->size = p->head->size - 1; \
			} else { /* ((1,((.,2)3,4)5)6,7)8 => ((1,(2,4)5)3,7)8; p=6 */ \
				my_node *r; \
				int e = d++; /* backup _d_ */\
				for (;;) { \
					dir[d] = 0; \
					path[d++] = q; \
					r = q->head->p[0]; \
					if (r->head->p[0] == 0) break; \
					q = r; \
				} \
				r->head->p[0] = p->head->p[0]; \
				q->head->p[0] = r->head->p[1]; \
				r->head->p[1] = p->head->p[1]; \
				r->head->balance = p->head->balance; \
				path[e-1]->head->p[dir[e-1]] = r; \
				path[e] = r, dir[e] = 1; \
				for (i = e + 1; i < d; ++i) --path[i]->head->size; \
				r->head->size = p->head->size - 1; \
			} \
		} \
		while (--d > 0) { \
			my_node *q = path[d]; \
			int which, other, b1 = 1, b2 = 2; \
			which = dir[d], other = 1 - which; \
			if (which) b1 = -b1, b2 = -b2; \
			q->head->balance += b1; \
			if (q->head->balance == b1) break; \
			else if (q->head->balance == b2) { \
				my_node *r = q->head->p[other]; \
				if (r->head->balance == -b1) { \
					path[d-1]->head->p[dir[d-1]] = kavl_rotate2(q, which); \
				} else { \
					path[d-1]->head->p[dir[d-1]] = kavl_rotate1(q, which); \
					if (r->head->balance == 0) { \
						r->head->balance = -b1; \
						q->head->balance = b1; \
						break; \
					} else r->head->balance = q->head->balance = 0; \
				} \
			} \
		} \
		*root_ = fake.head->p[0]; \
		return p; \
	}

class FreeNode {
public:
	virtual void free(void *map, my_node *p) = 0;
};

class PartitionedMap;

#define kavl_free(map, __root, freeNode) do { \
		my_node *_p, *_q; \
		for (_p = __root; _p; _p = _q) { \
			if (_p->head->p[0] == 0) { \
				_q = _p->head->p[1]; \
				freeNode->free(map, _p); \
			} else { \
				_q = _p->head->p[0]; \
				_p->head->p[0] = _q->head->p[1]; \
				_q->head->p[1] = _p; \
			} \
		} \
	} while (0)

const my_node *kavl_itr_first(const my_node *root) { \
		const my_node *p = root;\
		const my_node *first = 0; \
		while (p != 0) { \
			first = p; \
			p = p->head->p[0]; \
		}\
		return first;\
	} \
	 const my_node *kavl_itr_last(const my_node *root) { \
		const my_node *p = root;\
		const my_node *last = 0; \
		while (p != 0) { \
			last = p; \
			p = p->head->p[1]; \
		}\
		return last;\
	} 

int kavl_itr_find(const my_node *root, const my_node *x, struct kavl_itr *itr, KAVLComparator *comparator) { \
		const my_node *p = root; \
		itr->top = itr->stack - 1; \
		while (p != 0) { \
			int cmp; \
			cmp = comparator->compare(x->key, p->key); \
			if (cmp < 0) *++itr->top = p, p = p->head->p[0]; \
			else if (cmp > 0) p = p->head->p[1]; \
			else break; \
		} \
		if (p) { \
			*++itr->top = p; \
			itr->right = p->head->p[1]; \
			return 1; \
		} else if (itr->top >= itr->stack) { \
			itr->right = (*itr->top)->head->p[1]; \
			return 0; \
		} else return 0; \
	} 

int kavl_itr_find_prev(const my_node *root, const my_node *x, struct kavl_itr *itr, KAVLComparator *comparator) {
	\
		const my_node *p = root; \
		itr->top = itr->stack - 1; \
		while (p != 0) {
			\
				int cmp; \
				cmp = comparator->compare(x->key, p->key); \
				//if (p != 0 && p->key != 0 && p->key->key != 0) {
				//	printf("checking: %llu | %llu comp=%d\n", *(uint64_t*)p->key->key[0], *(uint64_t*)p->key->key[1], cmp);
				//	fflush(stdout);
				//}
				if (cmp > 0) *++itr->top = p, p = p->head->p[1]; \
					else if (cmp < 0) p = p->head->p[0]; \
					else break; \
		} \
			if (p) {
				\
					*++itr->top = p; \
					itr->left = p->head->p[0]; \
					//printf("returning: %llu | %llu\n", *(uint64_t*)p->key->key[0], *(uint64_t*)p->key->key[1]);
					//fflush(stdout);
				return 1; \
			}
			else if (itr->top >= itr->stack) { \
					itr->left = (*itr->top)->head->p[0]; \
				return 0; \
			}
			else return 0; \
}

int kavl_itr_next(struct kavl_itr *itr) { \
     		for (;;) { \
     			const my_node *p; \
     			for (p = itr->right, --itr->top; p; p = p->head->p[0]) \
     				*++itr->top = p; \
     			if (itr->top < itr->stack) {return 0;} \
     			if (*itr->top == 0) { \
     				return 0; \
     			} \
     			itr->right = (*itr->top)->head->p[1]; \
     			return 1; \
     		} \
     	} 
int kavl_itr_prev(struct kavl_itr *itr) { \
		for (;;) { \
			const my_node *p; \
			for (p = itr->left, --itr->top; p; p = p->head->p[1]) \
				*++itr->top = p; \
			if (itr->top < itr->stack) return 0; \
			itr->left = (*itr->top)->head->p[0]; \
			return 1; \
		} \
	}

/**
 * Insert a node to the tree
 *
 * @param suf     name suffix used in KAVL_INIT()
 * @param proot   pointer to the root of the tree (in/out: root may change)
 * @param x       node to insert (in)
 * @param cnt     number of nodes smaller than or equal to _x_; can be NULL (out)
 *
 * @return _x_ if not present in the tree, or the node equal to x.
 */

/**
 * Find a node in the tree
 *
 * @param suf     name suffix used in KAVL_INIT()
 * @param root    root of the tree
 * @param x       node value to find (in)
 * @param cnt     number of nodes smaller than or equal to _x_; can be NULL (out)
 *
 * @return node equal to _x_ if present, or NULL if absent
 */
/**
 * Delete a node from the tree
 *
 * @param suf     name suffix used in KAVL_INIT()
 * @param proot   pointer to the root of the tree (in/out: root may change)
 * @param x       node value to delete; if NULL, delete the first node (in)
 *
 * @return node removed from the tree if present, or NULL if absent
 */

/**
 * Place the iterator at the smallest object
 *
 * @param suf     name suffix used in KAVL_INIT()
 * @param root    root of the tree
 * @param itr     iterator
 */
/**
 * Place the iterator at the object equal to or greater than the query
 *
 * @param suf     name suffix used in KAVL_INIT()
 * @param root    root of the tree
 * @param x       query (in)
 * @param itr     iterator (out)
 *
 * @return 1 if find; 0 otherwise. kavl_at(itr) is NULL if and only if query is
 *         larger than all objects in the tree
 */


/**
 * Move to the next object in order
 *
 * @param itr     iterator (modified)
 *
 * @return 1 if there is a next object; 0 otherwise
 */


/**
 * Return the pointer at the iterator
 *
 * @param itr     iterator
 *
 * @return pointer if present; NULL otherwise
 */
#define kavl_at(itr) ((itr)->top < (itr)->stack? 0 : *(itr)->top)

//KAVL_INIT(my, struct my_node, head-> doCompare)


#endif