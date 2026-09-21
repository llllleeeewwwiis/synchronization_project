#define _XOPEN_SOURCE 700
#include <unistd.h>
#include "readers_writers.h"
#include "sync_utils.h"
#include <errno.h>

int usleep(unsigned int usec);
extern int rw_init(rwlock_t *rw);   /* in sync_utils.c: sets m,wlock, counters */
extern void rw_destroy(rwlock_t *rw);

static rwlock_t board;
static int schedule_version = 0;
static int violation_count = 0;
static int readers_in_cs = 0;
static int writers_in_cs = 0;
static pthread_mutex_t instrumentation_mutex = PTHREAD_MUTEX_INITIALIZER;

int get_final_schedule_version(void) {
  return schedule_version;
}
int get_violation_count(void) {
  return violation_count;
}

static void wait_for_sem(sem_t *sem) {
  while (sem_wait(sem) == -1) {
    if (errno != EINTR) DIE("sem_wait");
  }
}

void rw_rlock(rwlock_t *rw) {
  pthread_mutex_lock(&rw->m);
  if (rw->writer_active || rw->writers_waiting > 0) {
    rw->readers_waiting++;
    pthread_mutex_unlock(&rw->m);
    /* The unlocking writer reserves a reader slot before waking us. */
    wait_for_sem(&rw->rlock);
    return;
  }
  rw->readers++;
  pthread_mutex_unlock(&rw->m);
}

void rw_runlock(rwlock_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->readers--;
  if (rw->readers == 0 && rw->writers_waiting > 0) {
    rw->writers_waiting--;
    rw->writer_active = true;
    if (sem_post(&rw->wlock) == -1) DIE("sem_post");
  }
  pthread_mutex_unlock(&rw->m);
}

void rw_wlock(rwlock_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->writers_waiting++;
  if (rw->readers > 0 || rw->writer_active) {
    pthread_mutex_unlock(&rw->m);
    /* Ownership is handed directly to one waiting writer. */
    wait_for_sem(&rw->wlock);
    return;
  }
  rw->writers_waiting--;
  rw->writer_active = true;
  pthread_mutex_unlock(&rw->m);
}

void rw_wunlock(rwlock_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->writer_active = false;
  if (rw->writers_waiting > 0) {
    rw->writers_waiting--;
    rw->writer_active = true;
    if (sem_post(&rw->wlock) == -1) DIE("sem_post");
  } else {
    /* Reserve every admitted reader before allowing new writers to arrive. */
    while (rw->readers_waiting > 0) {
      rw->readers_waiting--;
      rw->readers++;
      if (sem_post(&rw->rlock) == -1) DIE("sem_post");
    }
  }
  pthread_mutex_unlock(&rw->m);
}

static void* reader(void* arg) {
  long id = (long)arg;
  for (int k=0;k<5;k++) {
    jitter_us(500, 4000);
    rw_rlock(&board);
    pthread_mutex_lock(&instrumentation_mutex);
    readers_in_cs++;
    if (writers_in_cs > 0) violation_count++;
    pthread_mutex_unlock(&instrumentation_mutex);
    int v = schedule_version;
    LOG("Attendee#%ld reads schedule v%d", id, v);
    jitter_us(200, 800);
    pthread_mutex_lock(&instrumentation_mutex);
    readers_in_cs--;
    pthread_mutex_unlock(&instrumentation_mutex);
    rw_runlock(&board);
  }
  return NULL;
}

static void* writer(void* arg) {
  long id = (long)arg;
  for (int k=0;k<3;k++) {
    jitter_us(2000, 6000);
    rw_wlock(&board);
    pthread_mutex_lock(&instrumentation_mutex);
    writers_in_cs++;
    if (writers_in_cs > 1 || readers_in_cs > 0) violation_count++;
    pthread_mutex_unlock(&instrumentation_mutex);
    schedule_version++;
    LOG("Organizer#%ld updates schedule to v%d", id, schedule_version);
    jitter_us(200, 800);
    pthread_mutex_lock(&instrumentation_mutex);
    writers_in_cs--;
    pthread_mutex_unlock(&instrumentation_mutex);
    rw_wunlock(&board);
  }
  return NULL;
}

int schedule_run(void) {
  if (rw_init(&board)) DIE("rw_init");
  pthread_t readers[8], writers[2];
  for (long i=0;i<8;i++) readers[i] = spawn(reader, (void*)i, "reader");
  for (long i=0;i<2;i++) writers[i] = spawn(writer, (void*)i, "writer");
  for (int i=0;i<8;i++) join(readers[i]);
  for (int i=0;i<2;i++) join(writers[i]);
  rw_destroy(&board);
  LOG("Schedule (readers–writers) complete.");
  return 0;
}
