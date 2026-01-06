// Part 2 — Max-Value Aggregation with Constrained Shared Memory (Threads)
//
// Goal
// -----
// Compute the global maximum of a large integer array using multiple threads
// that all update a *single shared integer* (the global maximum buffer).
// Synchronization prevents race conditions when multiple workers attempt to
// update the buffer concurrently.
//
// Map phase   : Each worker scans its chunk and computes a local maximum.
// Update step : Worker reads the current global value and writes only if larger.
// Reduce phase: The final global maximum is read from the single shared integer.
//
// We provide three update methods to study synchronization cost:
//   0 = naive   (no synchronization, for demonstration; may be incorrect)
//   1 = mutex   (pthread mutex protects the single integer)
//   2 = atomic  (C11 CAS on a single _Atomic int; buffer truly 1 integer)
//
// Build
// -----
//   gcc -O2 -std=c11 -pthread -o part2_threads_max part2_threads_max.c
//
// Run (examples)
// --------------
//   ./part2_threads_max -n 131072 -w 1 -m mutex -v
//   ./part2_threads_max -n 131072 -w 2 -m mutex -v
//   ./part2_threads_max -n 131072 -w 4 -m mutex -v
//   ./part2_threads_max -n 131072 -w 8 -m mutex -v
//   ./part2_threads_max -n 131072 -w 8 -m atomic -v
//   ./part2_threads_max -n 131072 -w 8 -m naive -v   # likely WRONG at scale
//
// Notes
// -----
// • The "shared memory region limited to one integer" requirement is respected:
//   - The result buffer is exactly one integer (int or _Atomic int).
//   - Synchronization primitives (mutex) are separate kernel/user objects; they
//     are not part of the single-value data buffer.
// • We report map time (scan + updates) and reduce time (read the final value).
// • getrusage().ru_maxrss is used as a memory proxy (KB on Linux).

#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/resource.h>
#include <stdatomic.h>

static inline long long now_ns(void){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts); return (long long)ts.tv_sec*1000000000LL+ts.tv_nsec; }
static inline double ns_to_ms(long long ns){ return (double)ns/1.0e6; }
static long get_max_rss_raw(void){ struct rusage ru; if(getrusage(RUSAGE_SELF,&ru)!=0) return -1; return ru.ru_maxrss; }

// Data & options
typedef enum { MODE_NAIVE=0, MODE_MUTEX=1, MODE_ATOMIC=2 } Mode;

typedef struct { size_t start, len; } Chunk;

typedef struct {
    int *arr; size_t n; size_t num_workers; unsigned seed; int verify; Mode mode;
    // Shared single-integer buffer for the global maximum
    int gmax_plain;              // used by NAIVE/MUTEX (same storage), protected by mutex when MODE_MUTEX
    _Atomic int gmax_atomic;     // used by ATOMIC (truly one atomic int)
    pthread_mutex_t gmax_mu;     // only used in MODE_MUTEX
} Ctx;

static void fill_random(int *a, size_t n, unsigned seed){ uint32_t x=seed?seed:2463534242u; 
	for(size_t i=0;i<n;++i){
		x^=x<<13; x^=x>>17; x^=x<<5; a[i]=(int)(x & 0x7fffffff);
	} 
}
	
static int max_sequential(const int *a, size_t n){
	int m=a[0]; 
	for(size_t i=1;i<n;++i) if(a[i]>m) m=a[i]; return m; 
}

//Worker logic 
typedef struct { Ctx *cx; size_t idx; } WArg;

static void *worker(void *arg){
    WArg *wa=(WArg*)arg; Ctx *cx=wa->cx; size_t i=wa->idx;
    // chunk bounds
    size_t base=cx->n/cx->num_workers, rem=cx->n%cx->num_workers;
    size_t start=i*base + (i<rem? i: rem);
    size_t len  =base + (i<rem? 1: 0);

    // compute local maximum
    int local_max = (len? cx->arr[start] : INT32_MIN);
    for(size_t k=1;k<len;++k){ 
		int v=cx->arr[start+k]; 
		if(v>local_max) local_max=v; 
	}

    if(len==0) return NULL; // nothing to update

    switch(cx->mode){
        case MODE_NAIVE: {
            if(local_max > cx->gmax_plain) cx->gmax_plain = local_max; // data race!
            break;
        }
        case MODE_MUTEX: {
            pthread_mutex_lock(&cx->gmax_mu);
            if(local_max > cx->gmax_plain) cx->gmax_plain = local_max;
            pthread_mutex_unlock(&cx->gmax_mu);
            break;
        }
        case MODE_ATOMIC: {
            int prev = atomic_load_explicit(&cx->gmax_atomic, memory_order_relaxed);
            while(local_max > prev){
                if(atomic_compare_exchange_weak_explicit(&cx->gmax_atomic, &prev, local_max,
                                                         memory_order_release, memory_order_relaxed)){
                    break; // success
                }
                // on failure, prev is updated to the latest value; loop continues if still smaller
            }
            break;
        }
    }
    return NULL;
}

// MAIN / CLI
static void usage(const char *p){
    fprintf(stderr,
        "Usage: %s [-n length] [-w workers] [-s seed] [-m mode] [-v]\n"
        "  -n    total array length (default 131072)\n"
        "  -w    number of workers (1,2,4,8…) (default 4)\n"
        "  -s    RNG seed (default 12345)\n"
        "  -m    update mode: naive | mutex | atomic (default mutex)\n"
        "  -v    verify result against sequential max\n", p);
}

static Mode parse_mode(const char *s){
    if(!s) return MODE_MUTEX;
    if(strcmp(s,"naive")==0) return MODE_NAIVE;
    if(strcmp(s,"mutex")==0) return MODE_MUTEX;
    if(strcmp(s,"atomic")==0) return MODE_ATOMIC;
    return MODE_MUTEX;
}

int main(int argc, char **argv){
    Ctx cx = {0};
    cx.n = 131072;
    cx.num_workers = 4;
    cx.seed = 12345;
    cx.verify = 0;
    cx.mode = MODE_MUTEX;

    int auto_mode = 0; 
    int opt; char *mode_str = NULL;

    while((opt = getopt(argc, argv, "n:w:s:m:hva")) != -1){
        switch(opt){
            case 'n': cx.n = (size_t)strtoull(optarg, NULL, 10); break;
            case 'w': cx.num_workers = (size_t)strtoull(optarg, NULL, 10); break;
            case 's': cx.seed = (unsigned)strtoul(optarg, NULL, 10); break;
            case 'm': mode_str = optarg; break;
            case 'v': cx.verify = 1; break;
            case 'a': auto_mode = 1; break;
            case 'h': default: usage(argv[0]); return 2;
        }
    }

    // auto Test Mode (runs 1, 2, 4, 8 workers)
    if (auto_mode) {
        int list[] = {1, 2, 4, 8};
        for (int i = 0; i < 4; i++) {
            char cmd[128];
            snprintf(cmd, sizeof(cmd),
                     "./part2_threads_max -n %zu -w %d -m mutex%s",
                     cx.n, list[i], cx.verify ? " -v" : "");
            printf("\n>>> Running with %d workers (mutex mode)...\n", list[i]);
            (void)system(cmd);
        }

        printf("\n>>> Running with 8 workers (atomic mode)...\n");
        system("./part2_threads_max -n 131072 -w 8 -m atomic -v");

        printf("\n>>> Running with 8 workers (naive mode)...\n");
        system("./part2_threads_max -n 131072 -w 8 -m naive -v");
        return 0;
    }

    cx.mode = parse_mode(mode_str);
    if (cx.num_workers == 0) cx.num_workers = 1;
    if (cx.num_workers > cx.n) cx.num_workers = cx.n;

    cx.arr = (int*)malloc(sizeof(int) * cx.n);
    if (!cx.arr) { perror("malloc"); return 1; }
    fill_random(cx.arr, cx.n, cx.seed);

    if (cx.mode == MODE_ATOMIC)
        atomic_store(&cx.gmax_atomic, INT32_MIN);
    else
        cx.gmax_plain = INT32_MIN;

    pthread_mutex_init(&cx.gmax_mu, NULL);

    pthread_t *ths = (pthread_t*)malloc(sizeof(pthread_t) * cx.num_workers);
    WArg *args = (WArg*)malloc(sizeof(WArg) * cx.num_workers);
    if (!ths || !args) { perror("malloc"); return 1; }

    long long t0 = now_ns();
    for (size_t i = 0; i < cx.num_workers; ++i) {
        args[i].cx = &cx;
        args[i].idx = i;
        if (pthread_create(&ths[i], NULL, worker, &args[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    for (size_t i = 0; i < cx.num_workers; ++i)
        pthread_join(ths[i], NULL);
    long long t1 = now_ns();

    int global_max = (cx.mode == MODE_ATOMIC)
                        ? atomic_load(&cx.gmax_atomic)
                        : cx.gmax_plain;
    long long t2 = now_ns();

    long rss = get_max_rss_raw();

    if (cx.verify) {
        int truth = max_sequential(cx.arr, cx.n);
        if (truth != global_max) {
            fprintf(stderr, "ERROR: wrong global max! got=%d expected=%d\n",
                    global_max, truth);
            return 1;
        }
    }

    printf("ArrayLength=%zu Workers=%zu Seed=%u\n", cx.n, cx.num_workers, cx.seed);
    printf("Mode=%s\n", (cx.mode == MODE_NAIVE ? "naive" :
                         cx.mode == MODE_MUTEX ? "mutex" : "atomic"));
    printf("MapTime_ms=%.3f ReduceTime_ms=%.3f Total_ms=%.3f\n",
           ns_to_ms(t1 - t0), ns_to_ms(t2 - t1), ns_to_ms(t2 - t0));
#if defined(__linux__)
    printf("MaxRSS_raw=%ld (kilobytes on Linux)\n", rss);
#else
    printf("MaxRSS_raw=%ld (bytes on this platform)\n", rss);
#endif
    printf("GlobalMax=%d\n", global_max);

    pthread_mutex_destroy(&cx.gmax_mu);
    free(ths); free(args); free(cx.arr);
    return 0;
}

