#define _XOPEN_SOURCE 700
#include <unistd.h>
#include "sync_utils.h"
#include <sys/time.h>
#include <string.h>
#include <errno.h>

static void wait_for_sem(sem_t *sem) {
  while (sem_wait(sem) == -1) {
    if (errno != EINTR) DIE("sem_wait");
  }
}

int usleep(unsigned int usec);
uint64_t now_ms(void) {
  struct timeval tv; gettimeofday(&tv, NULL);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000ULL;
}

pthread_t spawn(thread_fn fn, void *arg, const char *name) {
  (void)name; /* name useful for extended logging */
  pthread_t t;
  if (pthread_create(&t, NULL, fn, arg)) DIE("pthread_create");
  return t;
}
void join(pthread_t t) { if (pthread_join(t, NULL)) DIE("pthread_join"); }

void jitter_us(int min_us, int max_us) {
  int span = (max_us > min_us) ? (max_us - min_us) : 1;
  int d = min_us + (rand() % span);
  usleep(d);
}

/* ------- Reader-Writer Lock: initialization and cleanup ------- */
int rw_init(rwlock_t *rw) {
  if (!rw) {
    errno = EINVAL;
    return -1;
  }
  rw->readers = 0;
  rw->readers_waiting = 0;
  rw->writers_waiting = 0;
  rw->writer_active = false;

  int err = pthread_mutex_init(&rw->m, NULL);
  if (err) {
    errno = err;
    return -1;
  }
  if (sem_init(&rw->rlock, 0, 0) == -1) {
    err = errno;
    pthread_mutex_destroy(&rw->m);
    errno = err;
    return -1;
  }
  if (sem_init(&rw->wlock, 0, 0) == -1) {
    err = errno;
    sem_destroy(&rw->rlock);
    pthread_mutex_destroy(&rw->m);
    errno = err;
    return -1;
  }
  return 0;
}

void rw_destroy(rwlock_t *rw) {
  sem_destroy(&rw->rlock);
  sem_destroy(&rw->wlock);
  pthread_mutex_destroy(&rw->m);
}
/* RW lock functions are implemented in readers_writers.c */

/* ------- Food Tray Helper Functions ------- */
food_tray_t* create_food_tray(int tray_id, const char *food_name, int cook_id) {
  food_tray_t *tray = malloc(sizeof(food_tray_t));
  if (!tray) DIE("malloc food_tray");
  
  tray->tray_id = tray_id;
  tray->food_name = strdup(food_name);
  if (!tray->food_name) DIE("strdup food_name");
  tray->prepared_by = cook_id;
  
  return tray;
}

void free_food_tray(food_tray_t *tray) {
  if (tray) {
    free(tray->food_name);
    free(tray);
  }
}

/* ------- Bounded Buffer: initialization and operations ------- */
int bb_init(bb_t *q, int capacity) {
  if (!q || capacity <= 0) {
    errno = EINVAL;
    return -1;
  }
  q->buffer = calloc((size_t)capacity, sizeof(*q->buffer));
  if (!q->buffer) return -1;
  q->cap = capacity;
  q->head = 0;
  q->tail = 0;

  int err = pthread_mutex_init(&q->m, NULL);
  if (err) goto fail_buffer;
  if (sem_init(&q->empty, 0, (unsigned int)capacity) == -1) {
    err = errno;
    goto fail_mutex;
  }
  if (sem_init(&q->full, 0, 0) == -1) {
    err = errno;
    sem_destroy(&q->empty);
    goto fail_mutex;
  }
  return 0;

fail_mutex:
  pthread_mutex_destroy(&q->m);
fail_buffer:
  free(q->buffer);
  q->buffer = NULL;
  errno = err;
  return -1;
}

void bb_destroy(bb_t *q) {
  /* All producers and consumers must have finished before destruction. */
  sem_destroy(&q->empty);
  sem_destroy(&q->full);
  pthread_mutex_destroy(&q->m);
  free(q->buffer);
  q->buffer = NULL;
}

void bb_put(bb_t *q, food_tray_t *tray) {
  wait_for_sem(&q->empty);
  pthread_mutex_lock(&q->m);
  q->buffer[q->tail] = tray;
  q->tail = (q->tail + 1) % q->cap;
  pthread_mutex_unlock(&q->m);
  if (sem_post(&q->full) == -1) DIE("sem_post");
}

food_tray_t* bb_take(bb_t *q) {
  wait_for_sem(&q->full);
  pthread_mutex_lock(&q->m);
  food_tray_t *tray = q->buffer[q->head];
  q->buffer[q->head] = NULL;
  q->head = (q->head + 1) % q->cap;
  pthread_mutex_unlock(&q->m);
  if (sem_post(&q->empty) == -1) DIE("sem_post");
  return tray;
}
