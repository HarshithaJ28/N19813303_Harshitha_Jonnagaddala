#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread
int num_threads = 1;      // Number of threads (configurable)
int keys[NUM_KEYS];

// Read-write lock for each bucket
pthread_rwlock_t bucket_locks[NUM_BUCKETS];

typedef struct bucket_entry {
    int key;
    int val;
    struct bucket_entry *next;
} bucket_entry;

bucket_entry *table[NUM_BUCKETS];

void panic(char *msg) {
    printf("%s\n", msg);
    exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Insert remains exclusive with write lock
void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    pthread_rwlock_wrlock(&bucket_locks[i]);
    
    // Check if key already exists
    bucket_entry *current;
    for (current = table[i]; current != NULL; current = current->next) {
        if (current->key == key) {
            current->val = val;  // Update existing value
            pthread_rwlock_unlock(&bucket_locks[i]);
            return;
        }
    }
    
    // Key doesn't exist, create new entry
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) {
        pthread_rwlock_unlock(&bucket_locks[i]);
        panic("No memory to allocate bucket!");
    }
    e->key = key;
    e->val = val;
    e->next = table[i];
    table[i] = e;
    
    pthread_rwlock_unlock(&bucket_locks[i]);
}

// Optimized retrieve with read lock allowing parallel reads
bucket_entry * retrieve(int key) {
    int i = key % NUM_BUCKETS;
    pthread_rwlock_rdlock(&bucket_locks[i]);
    
    bucket_entry *b;
    for (b = table[i]; b != NULL; b = b->next) {
        if (b->key == key) {
            bucket_entry *result = malloc(sizeof(bucket_entry));
            if (!result) {
                pthread_rwlock_unlock(&bucket_locks[i]);
                panic("No memory to allocate result!");
            }
            // Return a copy of the entry
            result->key = b->key;
            result->val = b->val;
            result->next = NULL;
            pthread_rwlock_unlock(&bucket_locks[i]);
            return result;
        }
    }
    
    pthread_rwlock_unlock(&bucket_locks[i]);
    return NULL;
}

void * put_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;
    
    // Each thread handles its portion of keys
    for (key = tid; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid);
    }
    
    pthread_exit(NULL);
}

void * get_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;
    long lost = 0;
    
    for (key = tid; key < NUM_KEYS; key += num_threads) {
        bucket_entry *entry = retrieve(keys[key]);
        if (entry == NULL) {
            lost++;
        } else {
            free(entry);  // Free the copied entry
        }
    }
    
    printf("[thread %ld] %ld keys lost!\n", tid, lost);
    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    long i;
    pthread_t *threads;
    double start, end;

    if (argc != 2) {
        panic("usage: ./parallel_mutex <num_threads>");
    }
    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }

    // Initialize hash table and rwlocks
    memset(table, 0, sizeof(bucket_entry*) * NUM_BUCKETS);
    for (i = 0; i < NUM_BUCKETS; i++) {
        pthread_rwlock_init(&bucket_locks[i], NULL);
    }

    // Initialize random keys
    srandom(time(NULL));
    for (i = 0; i < NUM_KEYS; i++) {
        keys[i] = random();
    }

    threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
    if (!threads) {
        panic("out of memory allocating thread handles");
    }

    // Insert keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }
    
    // Wait for all insertions to complete
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);

    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t) * num_threads);

    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
    }

    // Collect count of lost keys
    long total_lost = 0;
    long *lost_keys = (long *) malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();
    
    printf("[main] Retrieved %ld/%d keys in %f seconds\n", 
           NUM_KEYS - total_lost, NUM_KEYS, end - start);

    // Cleanup
    free(lost_keys);
    free(threads);
    
    // Cleanup hash table and rwlocks
    for (i = 0; i < NUM_BUCKETS; i++) {
        pthread_rwlock_wrlock(&bucket_locks[i]);
        bucket_entry *current = table[i];
        while (current != NULL) {
            bucket_entry *next = current->next;
            free(current);
            current = next;
        }
        pthread_rwlock_unlock(&bucket_locks[i]);
        pthread_rwlock_destroy(&bucket_locks[i]);
    }

    return 0;
}
