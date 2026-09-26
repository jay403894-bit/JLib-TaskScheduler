// A lambda task that suspends must abort with the lambda message. Exit code 3 = child aborted as expected.
#include <TaskScheduler.h>
#include <cstdio>
#include <cstring>
#include "spawn_self.h"
using namespace JLib;
int main(int argc, char** argv) {
	if (argc > 1 && std::strcmp(argv[1], "child") == 0) {
		TaskScheduler::Init(2);
		auto& s = TaskScheduler::Instance();
		WaitGroup outer; outer.n.store(1);
		Task* t = s.CreateTask([&s]() {
			WaitGroup inner; inner.n.store(1);
			Task* x = s.CreateTask(+[](void*) {}, nullptr);
			x->waitGroup = &inner;
			s.Push(x);
			s.WaitFor(inner);   // lambda suspends -> must abort
		});
		t->waitGroup = &outer;
		s.Push(t);
		s.WaitFor(outer);
		std::printf("CHILD DID NOT ABORT\n");
		return 0;
	}
	const JLibTest::ChildResult r = JLibTest::RunSelf("child", argv[0]);
	if (!r.started) { std::printf("could not start the child\n"); return 2; }
	std::printf("child exit code 0x%lX -> %s\n", r.code,
		r.aborted ? "aborted as expected" : (r.timedOut ? "TIMED OUT" : "NOT aborted"));
	return r.aborted ? 0 : 1;
}
