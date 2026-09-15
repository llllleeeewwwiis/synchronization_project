#include "sync_utils.h"

int rwm_init(rwlock_monitor_t *rw) {
  /* TODO: initialize all fields
   * - mutex + condition variables
   * - counters AR/WR/AW/WW
   * - writer_batch_count
   * - phase (RWM_PHASE_WRITER or RWM_PHASE_READER)
   */
  (void)rw;
  return 0;
}

void rwm_destroy(rwlock_monitor_t *rw) {
  // TODO: destroy mutex + condition variables
  (void)rw;
}

void rwm_rlock(rwlock_monitor_t *rw) {
  /* TODO: reader lock (writer-priority + batch fairness)
   * - wait while writer active OR (writers waiting AND phase is writer)
   * - update WR/AR
   * - use while around pthread_cond_wait (Mesa semantics)
   */
  (void)rw;
}

void rwm_runlock(rwlock_monitor_t *rw) {
  /* TODO: reader unlock
   * - update AR
   * - if last reader, decide who to wake (writer vs reader batch)
   */
  (void)rw;
}

void rwm_wlock(rwlock_monitor_t *rw) {
  /* TODO: writer lock (writer-priority + batch fairness)
   * - wait while readers/writers active OR reader-phase with waiting readers
   * - update WW/AW
   * - increment writer_batch_count when readers are waiting
   */
  (void)rw;
}

void rwm_wunlock(rwlock_monitor_t *rw) {
  /* TODO: writer unlock
   * - update AW
   * - if WR>0 and writer_batch_count reached RWM_WRITER_BATCH, switch to reader phase
   * - otherwise continue writers or release readers as needed
   */
  (void)rw;
}
