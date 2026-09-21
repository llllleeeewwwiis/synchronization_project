#ifndef SYNC_UTILS_H
#define SYNC_UTILS_H

#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define DIE(msg) do { perror(msg); exit(EXIT_FAILURE); } while (0)

/* Logging (timestamped) */
uint64_t now_ms(void);
#define LOG(fmt, ...) \
  do { fprintf(stdout, "[%10llu ms] " fmt "\n", (unsigned long long)now_ms(), ##__VA_ARGS__); fflush(stdout); } while (0)

/* Thread helpers */
typedef void* (*thread_fn)(void*);
pthread_t spawn(thread_fn fn, void *arg, const char *name);
void join(pthread_t t);

/* Random jitter for schedule perturbation */
void jitter_us(int min_us, int max_us);

/* ---------- Reader-Writer Lock for Conference Schedule ---------- */

/* Writer-priority reader-writer lock (readers_writers.c). */
typedef struct {
  pthread_mutex_t m;
  sem_t rlock;
  sem_t wlock;
  int readers;
  int readers_waiting;
  int writers_waiting;
  bool writer_active;
} rwlock_t;

int  rw_init(rwlock_t *rw);
void rw_destroy(rwlock_t *rw);
void rw_rlock(rwlock_t *rw);
void rw_runlock(rwlock_t *rw);
void rw_wlock(rwlock_t *rw);
void rw_wunlock(rwlock_t *rw);

/* Monitor-style reader-writer lock (rwlock_monitor.c). */
#define RWM_WRITER_BATCH 3
#define RWM_PHASE_WRITER 0
#define RWM_PHASE_READER 1

typedef struct {
  pthread_mutex_t m;
  pthread_cond_t can_read;
  pthread_cond_t can_write;
  int AR; /* Active readers. */
  int WR; /* Waiting readers. */
  int AW; /* Active writer (0 or 1). */
  int WW; /* Waiting writers. */
  int writer_batch_count;
  int phase;
  /* Each reader phase admits only the readers waiting when it begins. */
  uint64_t next_reader_ticket;
  uint64_t reader_batch_end;
  int reader_batch_remaining;
} rwlock_monitor_t;

int  rwm_init(rwlock_monitor_t *rw);
void rwm_destroy(rwlock_monitor_t *rw);
void rwm_rlock(rwlock_monitor_t *rw);
void rwm_runlock(rwlock_monitor_t *rw);
void rwm_wlock(rwlock_monitor_t *rw);
void rwm_wunlock(rwlock_monitor_t *rw);

/* ---------- Bounded Buffer for Producer-Consumer (Snacks) ---------- */

/* Food tray structure */
typedef struct {
  int tray_id;           // Unique tray identifier
  char *food_name;       // Name of food on tray (dynamically allocated)
  int prepared_by;       // Cook who prepared it
} food_tray_t;

/* Bounded FIFO buffer. */
typedef struct {
  food_tray_t **buffer;
  sem_t empty;
  sem_t full;
  pthread_mutex_t m;
  int cap;
  int head;
  int tail;
} bb_t;

int  bb_init(bb_t *q, int capacity);
void bb_destroy(bb_t *q);
void bb_put(bb_t *q, food_tray_t *tray);   /* blocks if full */
food_tray_t* bb_take(bb_t *q);             /* blocks if empty, returns tray to consume */

/* Helper functions for food trays */
food_tray_t* create_food_tray(int tray_id, const char *food_name, int cook_id);
void free_food_tray(food_tray_t *tray);

#endif
