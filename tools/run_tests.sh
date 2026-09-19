#!/usr/bin/env bash
# Usage: tools/run_tests.sh <bin-dir> [loops] [quick]
BIN="${1:?bin directory}"
LOOPS="${2:-1}"
QUICK="${3:-}"
cd "$BIN" || exit 1

fails=0
total=0
run() {
    [ -e "./$1" ] || [ -e "./$1.exe" ] || return 0
    total=$((total + 1))
    if timeout 180 "./$1" "$2" > run.out 2>&1; then
        printf '  ok      %s %s\n' "$1" "$2"
    else
        rc=$?
        fails=$((fails + 1))
        printf '  FAIL    %s %s (rc=%s)\n' "$1" "$2" "$rc"
        tail -20 run.out | sed 's/^/          /'
    fi
}

for i in $(seq 1 "$LOOPS"); do
    [ "$LOOPS" -gt 1 ] && echo "=== pass $i of $LOOPS ==="
    run SchedulerPoolRun x
    run SchedulerConfigScopeTest x
    run SchedulerSlabExitTest x
    run SchedulerDagUnsubmittedTest x
    run SchedulerSkipListPqTest x
    run SchedulerMainAwayTest x
    run SchedulerThreadScopeTest x
    run SchedulerBlockInPlaceTest x
    run SchedulerBlockInPlaceTest s
    run SchedulerBlockInPlaceTest s0
    run SchedulerPforModeTest i
    run SchedulerPforModeTest o
    run SchedulerLateFreeTest x
    run SchedulerMainHelpTest x
    run SchedulerMainPushToTest x
    run SchedulerBatchTest x
    run SchedulerPinTest x
    run SchedulerPinTest f
    run SchedulerRecordTest m
    run SchedulerRecordTest p
    run SchedulerSemCvTest m
    run SchedulerSemCvTest p
    run SchedulerFtlTest x
    run SchedulerKStealTest x
    run SchedulerKStealTest p
    [ -n "$QUICK" ] && continue
    run SchedulerPeriodicTest m
    run SchedulerPeriodicTest p
    run SchedulerReclaimTest m
    run SchedulerReclaimTest p
    run SchedulerCoroutineTest m
    run SchedulerCoroutineTest p
    run SchedulerFutureTest m
    run SchedulerIoAsyncTest m
    run SchedulerIoAsyncTest p
    run SchedulerFiberGrowTest x
    run SchedulerFiberGrowTest l
    run SchedulerLambdaSuspendTest x
    run SchedulerHangHuntTest x
    run SchedulerSlotCheckTest x
    run SchedulerSlotCheckTest k
    run SchedulerWaitAllocTest x
    run SchedulerPforAllocTest x
    run SchedulerStatsTest x
done

echo
echo "failures: $fails of $total"
[ "$fails" -eq 0 ]
