#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/resource.h>
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/resource.h>

// -----------------------------------------------------------
// Timing Helpers
// 
// now_ns(): Returns the current monotonic time in nanoseconds. Useful for
// precise performance measurements unaffected by system clock adjustments.
//
// ns_to_ms(): Converts nanoseconds to milliseconds for easier readability.
// -------------------------------------------------------------

static inline long long now_ns(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}


static inline double ns_to_ms(long long ns) { return (double)ns / 1.0e6; }


// ---------------------------------------------------------------------------
// Memory Usage Helper
//
// get_max_rss_raw(): Returns the maximum resident set size (peak memory usage)
// of the current process. On Linux, this value is reported in kilobytes,
// while on BSD/macOS systems it is in bytes.
// -----------------------------------------------------------------------

static long get_max_rss_raw(void) {
	struct rusage ru;
	if (getrusage(RUSAGE_SELF, &ru) != 0) return -1;
	// On Linux ru_maxrss is in kbs. On some BSD/macOS it is in bytes.
	return ru.ru_maxrss;
}

// -----------------------------------------------------------------------------
// Thread-Safe Ring Buffer for Chunks
//
// This module implements a fixed-size circular queue (ring buffer) that stores
// `Chunk` structures. It supports concurrent producers and consumers using
// pthread mutexes and condition variables.
//
// Structures:
//   - Chunk: Represents a data segment with a start offset and length.
//   - ChunkQueue: Holds the circular buffer, capacity, indices, and sync objects.
//
// Functions:
//   - cq_init(): Initializes the queue with a given capacity.
//   - cq_destroy(): Frees resources associated with the queue.
//   - cq_push(): Adds a new chunk to the queue (blocks if full).
//   - cq_pop(): Removes and returns a chunk from the queue (blocks if empty).
//
// Thread-safety is guaranteed using `pthread_mutex_t` and `pthread_cond_t`.
// -----------------------------------------------------------------------------

typedef struct { size_t start; size_t len; } Chunk;


typedef struct {
	Chunk *buf; // ring buffer storage
	size_t cap; // capacity
	size_t head, tail, cnt; // ring indices / count
	pthread_mutex_t mu;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
} ChunkQueue;


static void cq_init(ChunkQueue *q, size_t cap) {
	q->buf = (Chunk*)malloc(sizeof(Chunk) * cap);
	
	if (!q->buf) { perror("malloc"); exit(1); }
	
	q->cap = cap; q->head = q->tail = q->cnt = 0;
	pthread_mutex_init(&q->mu, NULL);
	pthread_cond_init(&q->not_empty, NULL);
	pthread_cond_init(&q->not_full, NULL);
}


static void cq_destroy(ChunkQueue *q) {
	free(q->buf);
	pthread_mutex_destroy(&q->mu);
	pthread_cond_destroy(&q->not_empty);
	pthread_cond_destroy(&q->not_full);
}


static void cq_push(ChunkQueue *q, Chunk c) {
	pthread_mutex_lock(&q->mu);
	while (q->cnt == q->cap) pthread_cond_wait(&q->not_full, &q->mu);
	q->buf[q->tail] = c;
	q->tail = (q->tail + 1) % q->cap;
	q->cnt++;
	pthread_cond_signal(&q->not_empty);
	pthread_mutex_unlock(&q->mu);
}


static Chunk cq_pop(ChunkQueue *q) {
	pthread_mutex_lock(&q->mu);
	while (q->cnt == 0) pthread_cond_wait(&q->not_empty, &q->mu);
	Chunk c = q->buf[q->head];
	q->head = (q->head + 1) % q->cap;
	q->cnt--;
	pthread_cond_signal(&q->not_full);
	pthread_mutex_unlock(&q->mu);
	return c;
}

// -----------------------------------------------------------------------------
// Context Structure for Map-Reduce Pipeline
//
// This structure holds all shared state used by mapper and reducer threads.
//
// Fields:
//   - arr: Input integer array to be processed (modified in-place by mappers).
//   - n: Total number of elements in the input array.
//   - out: Final merged output array produced by the reducer.
//   - num_workers: Number of mapper threads (and number of chunks).
//   - queue: Thread-safe communication channel (ChunkQueue) from mappers to reducer.
//   - maps_done: Counter tracking how many map tasks have completed.
// 
// The Context object coordinates data sharing and synchronization between
// producer (mapper) and consumer (reducer) threads in a parallel processing setup.
// -----------------------------------------------------------------------------

typedef struct {
	int *arr; // input array (modified in-place by mappers)
	size_t n; // total length
	int *out; // final merged output
	size_t num_workers; // number of map workers (and chunks)
	ChunkQueue queue; // channel: mapper -> reducer
	volatile size_t maps_done; // number of finished map tasks
} Context;


static int cmp_int(const void *a, const void *b) {
int ia = *(const int*)a, ib = *(const int*)b;
return (ia > ib) - (ia < ib);
}

// -----------------------------------------------------------------------------
// Mapper Thread Implementation
//
// Each mapper thread is responsible for sorting a specific portion ("chunk")
// of the shared input array. After sorting, it publishes a descriptor of that
// chunk to the reducer thread via a thread-safe queue.
//
// Components:
//   cmp_int()   : Comparison function used by qsort() for ascending order.
//   WorkerArg   : Structure holding a pointer to the shared Context and
//                   the worker’s index (0 .. num_workers-1).
//   map_worker(): The main worker thread routine. It computes its assigned
//                   data range, sorts it in place, and pushes the resulting
//                   chunk metadata into the shared ChunkQueue.
//
// Workflow:
//   Determine the start offset and length of this worker's chunk.
//   Sort that subarray using qsort() and cmp_int().
//   Send a Chunk descriptor (start, len) to the reducer via cq_push().
//   Atomically increment maps_done to signal task completion.
//
// Synchronization & Communication:
//   - Uses a shared Context to access the input array and ChunkQueue.
//   - Inter-thread communication occurs through message passing (ChunkQueue)
//     protected by a mutex and condition variables.
// -----------------------------------------------------------------------------

typedef struct {
	Context *cx;
	size_t idx; // worker index
} WorkerArg;


static void *map_worker(void *arg) {
	WorkerArg *wa = (WorkerArg*)arg;
	Context *cx = wa->cx;
	size_t idx = wa->idx;


	// Compute my chunk [start, end)
	size_t base = (cx->n / cx->num_workers);
	size_t rem = (cx->n % cx->num_workers);
	size_t start = idx * base + (idx < rem ? idx : rem);
	size_t len = base + (idx < rem ? 1 : 0);

	if (len > 1) qsort(cx->arr + start, len, sizeof(int), cmp_int);

	// Publish my sorted chunk to the reducer via the queue (message passing).
	Chunk c = { .start = start, .len = len };
	cq_push(&cx->queue, c);

	__sync_fetch_and_add(&cx->maps_done, 1); // atomic increment
	return NULL;
}

// -----------------------------------------------------------------------------
// kway_merge()
// 
// Performs a k-way merge of multiple sorted chunks into a single sorted array.
//
// Purpose:
//   This function represents the "reduce" step of the MapReduce-style pipeline.
//   It takes K sorted subarrays (produced by mapper threads) and merges them
//   efficiently into a final sorted output array.
//
// Parameters:
//   - cx      : Shared Context containing the input and output arrays.
//   - chunks  : Array of Chunk descriptors (each defines a sorted run).
//   - k       : Number of sorted runs (chunks).
//
// Implementation details:
//   A binary min-heap of size K is used to efficiently select the smallest
//     next element among all sorted chunks.
//   Initially, the first element from each non-empty chunk is inserted into
//     the heap.
//   Then, the algorithm repeatedly extracts the smallest element, appends
//     it to the output array, and replaces it with the next element from the
//     same chunk until all data is consumed.
//
// Complexity:
//   Time  : O(N log K), where N = total number of elements, K = number of chunks.
//   Space : O(K) for the heap


typedef struct {
	int value; // current value from run
	size_t run; // which run (chunk) this came from
	size_t idx; // index within the run (offset)
} HeapNode;


typedef struct {
	HeapNode *a; size_t n; // 1-based binary heap stored at [0..n-1], we will use 0-based heap
} MinHeap;


static void heap_swap(HeapNode *x, HeapNode *y) { HeapNode t = *x; *x = *y; *y = t; }


static void heap_sift_up(MinHeap *h, size_t i) {
	while (i > 0) {
		size_t p = (i - 1) / 2;
		if (h->a[p].value <= h->a[i].value) break;
		heap_swap(&h->a[p], &h->a[i]);
		i = p;
	}
}


static void heap_sift_down(MinHeap *h, size_t i) {
	for (;;) {
		size_t l = 2*i + 1, r = l + 1, m = i;
		if (l < h->n && h->a[l].value < h->a[m].value) m = l;
		if (r < h->n && h->a[r].value < h->a[m].value) m = r;
		if (m == i) break;
		heap_swap(&h->a[m], &h->a[i]);
		i = m;
	}
}


static void kway_merge(Context *cx, const Chunk *chunks, size_t k) {
	// Build min-heap with first element of each non-empty run.
	HeapNode *nodes = (HeapNode*)malloc(sizeof(HeapNode) * k);
	MinHeap h = { .a = nodes, .n = 0 };


	size_t total = 0;
	for (size_t i = 0; i < k; ++i) {
		total += chunks[i].len;
		if (chunks[i].len == 0) continue;
		HeapNode nd = {
		.value = cx->arr[chunks[i].start],
		.run = i,
		.idx = 0
		};
		h.a[h.n++] = nd;
	}
	// Heapify
	for (ssize_t i = (ssize_t)h.n/2 - 1; i >= 0; --i) heap_sift_down(&h, (size_t)i);


	size_t out_i = 0;
	while (h.n > 0) {
		HeapNode root = h.a[0];
		cx->out[out_i++] = root.value;

		// advance in the run
		size_t run = root.run;
		size_t idx = root.idx + 1;
		if (idx < chunks[run].len) {
			root.value = cx->arr[chunks[run].start + idx];
			root.idx = idx;
			h.a[0] = root;
		} else {
			// remove root: move last to root, decrease size
			h.a[0] = h.a[h.n - 1];
			h.n--;
		}
		  if (h.n > 0) heap_sift_down(&h, 0);
	}
}

// Reducer thread
typedef struct { Context *cx; } ReducerArg;


static void *reducer_thread(void *arg) {
	ReducerArg *ra = (ReducerArg*)arg;
	Context *cx = ra->cx;


	// Collect exactly num_workers chunk descriptors from the queue.
	Chunk *chunks = (Chunk*)calloc(cx->num_workers, sizeof(Chunk));
	if (!chunks) { perror("calloc"); exit(1); }


	for (size_t i = 0; i < cx->num_workers; ++i) {
		chunks[i] = cq_pop(&cx->queue);
	}

	// Merge all sorted runs into cx->out using k-way merge.
	kway_merge(cx, chunks, cx->num_workers);


	free(chunks);
	return NULL;
}

// Utilities
static void fill_random(int *a, size_t n, unsigned seed) {
	// Simple deterministic PRNG (xorshift32) for reproducibility, portable.
	uint32_t x = seed ? seed : 2463534242u;
	for (size_t i = 0; i < n; ++i) {
		x ^= x << 13; x ^= x >> 17; x ^= x << 5;
		a[i] = (int)(x & 0x7fffffff); // non-negative
	}
}


static int is_sorted(const int *a, size_t n) {
	for (size_t i = 1; i < n; ++i) if (a[i-1] > a[i]) return 0;
	return 1;
}


// ------------------------------ Main program -------------------------------
static void usage(const char *prog) {
	fprintf(stderr,
	"Usage: %s [-n length] [-w workers] [-seed S] [-verify]\n"
	" -n total array length (default 131072)\n"
	" -w number of workers (1,2,4,8…) (default 4)\n"
	" -seed RNG seed (default 12345)\n"
	" -verify check final array is sorted (debug)\n",
	prog);
}


int main(int argc, char **argv) {
    size_t n = 131072;
    size_t workers = 4;
    unsigned seed = 12345;
    int verify = 0;
    int auto_mode = 0;

    int opt;
    while ((opt = getopt(argc, argv, "n:w:s:hva")) != -1) {
        switch (opt) {
            case 'n': n = (size_t)strtoull(optarg, NULL, 10); break;
            case 'w': workers = (size_t)strtoull(optarg, NULL, 10); break;
            case 's': seed = (unsigned)strtoul(optarg, NULL, 10); break;
            case 'v': verify = 1; break;
            case 'a': auto_mode = 1; break;
            case 'h': default: usage(argv[0]); return 2;
        }
    }

    if (auto_mode) {
        int list[] = {1, 2, 4, 8};
        for (int i = 0; i < 4; i++) {
            char cmd[128];
            snprintf(cmd, sizeof(cmd),
                     "./part1_threads_sort -n %zu -w %d%s",
                     n, list[i], verify ? " -v" : "");
            printf("\n>>> Running with %d workers...\n", list[i]);
            system(cmd);
        }
        return 0;
    }

 
    if (workers == 0) workers = 1;
    if (workers > n) workers = n;

    Context cx = {0};
    cx.n = n;
    cx.num_workers = workers;
    cx.arr = (int*)malloc(sizeof(int) * n);
    cx.out = (int*)malloc(sizeof(int) * n);
    if (!cx.arr || !cx.out) { perror("malloc"); return 1; }

    fill_random(cx.arr, n, seed);
    cq_init(&cx.queue, workers);

    pthread_t reducer;
    ReducerArg ra = { .cx = &cx };
    if (pthread_create(&reducer, NULL, reducer_thread, &ra) != 0) {
        perror("pthread_create(reducer)");
        return 1;
    }

    pthread_t *ths = (pthread_t*)malloc(sizeof(pthread_t) * workers);
    WorkerArg *args = (WorkerArg*)malloc(sizeof(WorkerArg) * workers);
    if (!ths || !args) { perror("malloc"); return 1; }

    long long t0 = now_ns();
    for (size_t i = 0; i < workers; ++i) {
        args[i].cx = &cx;
        args[i].idx = i;
        if (pthread_create(&ths[i], NULL, map_worker, &args[i]) != 0) {
            perror("pthread_create(worker)");
            return 1;
        }
    }
    for (size_t i = 0; i < workers; ++i)
        pthread_join(ths[i], NULL);
    long long t1 = now_ns();

    long rss_after_map = get_max_rss_raw();
    pthread_join(reducer, NULL);
    long long t2 = now_ns();

    if (verify && !is_sorted(cx.out, n))
        fprintf(stderr, "ERROR: output is not sorted!\n");

    printf("ArrayLength=%zu Workers=%zu Seed=%u\n", n, workers, seed);
    printf("MapTime_ms=%.3f ReduceTime_ms=%.3f Total_ms=%.3f\n",
           ns_to_ms(t1 - t0), ns_to_ms(t2 - t1), ns_to_ms(t2 - t0));
    printf("MaxRSS_raw=%ld (kilobytes on Linux)\n", rss_after_map);

    cq_destroy(&cx.queue);
    free(ths); free(args);
    free(cx.arr); free(cx.out);
    return 0;
}






