/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _alloc {
    char *mem;
    size_t size;
} alloc_t, *alloc_pt;

typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;

/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;


/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status _mem_invalidate_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    if(!pool_store){
        // allocate the pool store with initial capacity
        pool_store = (pool_mgr_pt*)calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        if(!pool_store){  // If could not allocate initial capacity //
            return ALLOC_FAIL;
        }
        pool_store_size = 0;  // pool_store elements used //
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;  // Initial number of pool_store elements total //
        return ALLOC_OK;
    } else {
        return ALLOC_CALLED_AGAIN;
    }

    // note: holds pointers only, other functions to allocate/deallocate
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if(pool_store){
        // make sure all pool managers have been deallocated
        for(int i = 0; i < pool_store_capacity; ++i){
            if(pool_store[i]) {
                return ALLOC_NOT_FREED;
            }
        }
        // can free the pool store array
        free(pool_store);
        // update static variables
        pool_store = NULL;
        pool_store_size = 0;
        pool_store_capacity = 0;
        return ALLOC_OK;
    } else {
        return ALLOC_CALLED_AGAIN;
    }
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    if(!pool_store) return NULL;
    // expand the pool store, if necessary
    // ----------LATER---------------//

    // allocate a new mem pool mgr
    pool_mgr_pt new_mgr = (pool_mgr_pt)calloc(1, sizeof(pool_mgr_t));
    // check success, on error return null
    if(!new_mgr) return NULL;

    // allocate a new memory pool
    void * new_mem = malloc(size);
    // check success, on error deallocate mgr and return null
    if(!new_mem) {
        free(new_mgr);
        return NULL;
    }

    // allocate a new node heap
    node_pt new_heap = (node_pt)calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    // check success, on error deallocate mgr/pool and return null
    if(!new_heap) {
        free(new_mem);
        free(new_mgr);
        return NULL;
    }

    // allocate a new gap index
    gap_pt new_gap = (gap_pt)calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    // check success, on error deallocate mgr/pool/heap and return null
    if(!new_gap) {
        free(new_heap);
        free(new_mem);
        free(new_mgr);
        return NULL;
    }
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    new_heap[0].alloc_record.mem = new_mem;
    new_heap[0].alloc_record.size = size;
    new_heap[0].used = 1;
    new_heap[0].allocated = 0;
    new_heap[0].prev = NULL;
    new_heap[0].next = NULL;

    //   initialize top node of gap index
    new_gap[0].size = size;  // Total pool size //
    new_gap[0].node = new_heap;  // First node in node heap //

    //   initialize pool mgr
    new_mgr->pool.mem = new_mem;
    new_mgr->pool.policy = policy;
    new_mgr->pool.total_size = size;
    new_mgr->pool.alloc_size = 0;
    new_mgr->pool.num_allocs = 0;
    new_mgr->pool.num_gaps = 1;
    new_mgr->node_heap = new_heap;
    new_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    new_mgr->used_nodes = 1;
    new_mgr->gap_ix = new_gap;
    new_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    //   link pool mgr to pool store
    pool_store[pool_store_size] = new_mgr;
    pool_store_size ++;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt)new_mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt)pool;
    // check if this pool is allocated
    if(mgr->pool.alloc_size > 0) {
        return ALLOC_NOT_FREED;
    }
    // check if pool has only one gap
    if(mgr->pool.num_gaps > 1) {
        return ALLOC_NOT_FREED;
    }
    // check if it has zero allocations
    if(mgr->pool.num_allocs > 0) {
        return ALLOC_NOT_FREED;
    }
    // free memory pool
    free(mgr->pool.mem);
    // free node heap
    free(mgr->node_heap);
    // free gap index
    free(mgr->gap_ix);
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    for(int i = 0; i < pool_store_capacity; ++i){
        if(pool_store[i] == mgr){
            pool_store[i] = NULL;
            break;
        }
    }

    // free mgr
    free(mgr);

    return ALLOC_OK;
}

void * mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt)pool;
    // check if any gaps, return null if none
    if(!mgr->gap_ix) return NULL;

    // expand heap node, if necessary, quit on error
    alloc_status status = ALLOC_FAIL;
    // --------------LATER----------//
    // check used nodes fewer than total nodes, quit on error
    if(mgr->used_nodes >= mgr->total_nodes) return NULL;

    // get a node for allocation:
    node_pt gap_node = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(mgr->pool.policy == FIRST_FIT) {
        node_pt current_node = mgr->node_heap;
        while(current_node) {
            if(current_node->allocated == 0 && current_node->alloc_record.size >= size) {
                gap_node = current_node;  // Found node //
                break;
            }
            current_node = current_node->next;
        }
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else {
        gap_pt gap_ix = mgr->gap_ix;  // sorted in order of increasing size //
        for(int i = 0; i < pool->num_gaps; ++i){
            if(gap_ix[i].size >= size) {
                gap_node = gap_ix[i].node;  // Found node //
                break;
            }
        }
    }
    // check if node found
    if(!gap_node) return NULL;  // No gap node found //

    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs ++;
    mgr->pool.alloc_size += size;

    // calculate the size of the remaining gap, if any
    size_t remaining_gap = gap_node->alloc_record.size - size;

    // remove node from gap index
    status = _mem_remove_from_gap_ix(mgr, gap_node->alloc_record.size, gap_node);
    assert(status == ALLOC_OK);

    // convert gap_node to an allocation node of given size
    gap_node->allocated = 1;
    gap_node->alloc_record.size = size;

    // adjust node heap:
    //   if remaining gap, need a new node
    if(remaining_gap > 0){
        //   find an unused one in the node heap
        node_pt new_gap_node = NULL;
        node_pt current_node = mgr->node_heap;
        for(int i = 0; i < mgr->total_nodes; ++i) {
            if(current_node[i].used == 0) {
                new_gap_node = &current_node[i];
                break;
            }
        }
        //   make sure one was found
        if(!new_gap_node) return NULL;

        //   initialize it to a gap node
        new_gap_node->used = 1;
        new_gap_node->allocated = 0;
        new_gap_node->alloc_record.mem = gap_node->alloc_record.mem + size;
        new_gap_node->alloc_record.size = remaining_gap;

        //   update metadata (used_nodes)
        mgr->used_nodes += 1;

        //   update linked list (new node right after the node for allocation)
        new_gap_node->next = gap_node->next;
        if(gap_node->next) gap_node->next->prev = new_gap_node;
        new_gap_node->prev = gap_node;
        gap_node->next = new_gap_node;

        //   add to gap index
        status = _mem_add_to_gap_ix(mgr, remaining_gap, new_gap_node);
        //   check if successful
        assert(status == ALLOC_OK);
        // ----------- LATER ------------//
        /*if(status != ALLOC_OK) {
            status = _mem_resize_gap_ix(mgr);
            assert(status == ALLOC_OK);
            if(status == ALLOC_OK) status = _mem_add_to_gap_ix(mgr, remaining_gap, new_gap_node);
        }*/
    }

    // return allocation record by casting the node to (alloc_pt)
    return gap_node->alloc_record.mem;
}

alloc_status mem_del_alloc(pool_pt pool, void * alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt delete_node = NULL;
    // find the node in the node heap
    for(int i = 0; i < mgr->total_nodes; ++i) {
        // this is node-to-delete
        if(mgr->node_heap[i].alloc_record.mem == alloc) {
            delete_node = &mgr->node_heap[i];
            break;
        }
    }

    // make sure it's found
    if(!delete_node) return ALLOC_FAIL;

    // convert to gap node
    delete_node->allocated = 0;

    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs --;
    mgr->pool.alloc_size -= delete_node->alloc_record.size;

    alloc_status status = ALLOC_FAIL;

    // if the next node in the list is also a gap, merge into node-to-delete
    if(delete_node->next && delete_node->next->used ==  1 && delete_node->next->allocated == 0) {
        //   remove the next node from gap index
        status = _mem_remove_from_gap_ix(mgr,delete_node->next->alloc_record.size, delete_node->next);
        //   check success
        assert(status == ALLOC_OK);
        //   add the size to the node-to-delete
        delete_node->alloc_record.size += delete_node->next->alloc_record.size;
        //   update node as unused
        delete_node->next->used = 0;
        //   update metadata (used nodes)
        mgr->used_nodes --;

        //   update linked list:
        /*
                        if (next->next) {
                            next->next->prev = node_to_del;
                            node_to_del->next = next->next;
                        } else {
                            node_to_del->next = NULL;
                        }
                        next->next = NULL;
                        next->prev = NULL;
         */
        node_pt next = delete_node->next;
        if(next->next) {  // Not end of list //
            next->next->prev = delete_node;
            delete_node->next = next->next;
        } else {
            delete_node->next = NULL;
        }
        next->next = NULL;
        next->prev = NULL;
    }

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if(delete_node->prev && delete_node->prev->used ==  1 && delete_node->prev->allocated == 0) {
        //   remove the previous node from gap index
        status = _mem_remove_from_gap_ix(mgr, delete_node->prev->alloc_record.size, delete_node->prev);
        //   check success
        assert(status == ALLOC_OK);
        //   add the size of node-to-delete to the previous
        delete_node->prev->alloc_record.size += delete_node->alloc_record.size;
        //   update node-to-delete as unused
        delete_node->used = 0;
        //   update metadata (used_nodes)
        mgr->used_nodes--;
        //   update linked list
        /*
                        if (node_to_del->next) {
                            prev->next = node_to_del->next;
                            node_to_del->next->prev = prev;
                        } else {
                            prev->next = NULL;
                        }
                        node_to_del->next = NULL;
                        node_to_del->prev = NULL;
         */
        node_pt prev = delete_node->prev;
        if (delete_node->next) {
            prev->next = delete_node->next;
            delete_node->next->prev = prev;
        } else {
            prev->next = NULL;
        }
        delete_node->next = NULL;
        delete_node->prev = NULL;

        //   change the node to add to the previous node!
        delete_node = prev;
    }

        // add the resulting node to the gap index
        status = _mem_add_to_gap_ix(mgr, delete_node->alloc_record.size, delete_node);
        // check success
        assert(status == ALLOC_OK);

    return status;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt  mgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    pool_segment_pt segs = (pool_segment_pt)calloc(mgr->used_nodes, sizeof(pool_segment_t));
    // check successful
    assert(segments);
    // loop through the node heap and the segments array
    node_pt current_node = mgr->node_heap;
    for(unsigned i = 0; i < mgr->used_nodes; ++i) {
        //    for each node, write the size and allocated in the segment
        segs[i].size = current_node->alloc_record.size;
        segs[i].allocated = current_node->allocated;

        current_node = current_node->next;
    }
    *segments = segs;
    *num_segments = mgr->used_nodes;
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above

    return ALLOC_FAIL;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    //----------LATER----------------//

    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps ++;

    // sort the gap index (call the function)
    alloc_status status = _mem_sort_gap_ix(pool_mgr);
    assert(status == ALLOC_OK);

    return status;

    /*
    // resize the gap index, if necessary
    alloc_status result = _mem_resize_gap_ix(pool_mgr);
    assert(result == ALLOC_OK);
    if (result != ALLOC_OK) return ALLOC_FAIL;

    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
    pool_mgr->pool.num_gaps ++;

    // sort the gap index after addition
    result = _mem_sort_gap_ix(pool_mgr);
    assert(result == ALLOC_OK);
    if (result != ALLOC_OK) return ALLOC_FAIL;


    return result;*/
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    gap_pt gap_ix = pool_mgr->gap_ix;
    // find the position of the node in the gap index
    int position = -1;
    for(unsigned i = 0; i < pool_mgr->pool.num_gaps; ++i){
        if(gap_ix[i].node == node){
            position = i;
            break;
        }
    }
    if(position < 0) return ALLOC_FAIL;

    // loop from there to the end of the array:
    for(int i = position; i < pool_mgr->pool.num_gaps - 1; ++i){
        //    pull the entries (i.e. copy over) one position up
        //    this effectively deletes the chosen node
        gap_ix[i].size = gap_ix[i + 1].size;
        gap_ix[i].node = gap_ix[i + 1].node;
    }

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps --;
    // zero out the element at position num_gaps!
    gap_ix[pool_mgr->pool.num_gaps].size = 0;
    gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    gap_pt gap_ix = pool_mgr->gap_ix;
    for(unsigned i = pool_mgr->pool.num_gaps - 1; i > 0; --i){
        //    if the size of the current entry is less than the previous (u - 1)
        //    or if the sizes are the same but the current entry points to a
        //    node with a lower address of pool allocation address (mem)
        //       swap them (by copying) (remember to use a temporary variable)
        if(gap_ix[i].size < gap_ix[i - 1].size){
            gap_t temp = gap_ix[i];
            gap_ix[i] = gap_ix[i - 1];
            gap_ix[i - 1] = temp;
        } else if (gap_ix[i].size == gap_ix[i - 1].size && gap_ix[i].node < gap_ix[i - 1].node) {
            node_pt temp = gap_ix[i].node;
            gap_ix[i].node = gap_ix[i - 1].node;
            gap_ix[i - 1].node = temp;
        } else break;
    }

    return ALLOC_OK;
}

static alloc_status _mem_invalidate_gap_ix(pool_mgr_pt pool_mgr) {
    return ALLOC_FAIL;
}

