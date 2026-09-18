// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

// Re-runs this executable with one extra argument and reports how the child ended.
//
// Several tests check a tripwire whose whole job is to abort the process. Only a child can be
// allowed to hit it, so those tests re-exec themselves. That was written with CreateProcessA and
// so the tests did not build on Linux at all -- which is how they came to be Windows-only by
// accident rather than by nature.

#include <cstdio>

#if defined(_WIN32)
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif
    #include <windows.h>
#else
    #include <sys/types.h>
    #include <sys/wait.h>
    #include <unistd.h>
    #include <time.h>
#endif

namespace JLibTest {

    struct ChildResult {
        bool          started  = false;
        bool          timedOut = false;
        bool          aborted  = false;   // ended abnormally: nonzero status, or killed by a signal
        unsigned long code     = 0;
    };

    inline ChildResult RunSelf(const char* arg, const char* argv0 = nullptr,
                               unsigned timeoutMs = 20000) {
        ChildResult r;

#if defined(_WIN32)
        char cmd[MAX_PATH + 64];
        if (GetModuleFileNameA(nullptr, cmd, MAX_PATH) == 0) return r;
        std::snprintf(cmd + std::strlen(cmd), sizeof(cmd) - std::strlen(cmd), " %s", arg);

        STARTUPINFOA si{ sizeof(si) };
        si.dwFlags     = STARTF_USESTDHANDLES;
        si.hStdError   = GetStdHandle(STD_ERROR_HANDLE);
        si.hStdOutput  = GetStdHandle(STD_OUTPUT_HANDLE);
        si.hStdInput   = GetStdHandle(STD_INPUT_HANDLE);
        PROCESS_INFORMATION pi{};
        if (!CreateProcessA(nullptr, cmd, nullptr, nullptr, TRUE, 0, nullptr, nullptr, &si, &pi))
            return r;

        r.started = true;
        if (WaitForSingleObject(pi.hProcess, timeoutMs) == WAIT_TIMEOUT) {
            r.timedOut = true;
            TerminateProcess(pi.hProcess, 1);
            WaitForSingleObject(pi.hProcess, 5000);
        }
        DWORD code = 0;
        GetExitCodeProcess(pi.hProcess, &code);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        r.code    = (unsigned long)code;
        r.aborted = !r.timedOut && code != 0;
        return r;
#else
        const char* self = "/proc/self/exe";
        if (access(self, X_OK) != 0) self = argv0;
        if (!self) return r;

        const pid_t pid = fork();
        if (pid < 0) return r;
        if (pid == 0) {
            execl(self, self, arg, (char*)nullptr);
            _exit(127);
        }

        r.started = true;
        int status = 0;
        for (unsigned waited = 0; ; ++waited) {
            const pid_t got = waitpid(pid, &status, WNOHANG);
            if (got == pid) break;
            if (got < 0) return r;
            if (waited >= timeoutMs) {
                r.timedOut = true;
                kill(pid, SIGKILL);
                waitpid(pid, &status, 0);
                break;
            }
            struct timespec ts { 0, 1000000 };   // 1 ms
            nanosleep(&ts, nullptr);
        }

        if (WIFSIGNALED(status)) {
            r.code    = (unsigned long)(128 + WTERMSIG(status));
            r.aborted = !r.timedOut;
        } else {
            r.code    = (unsigned long)WEXITSTATUS(status);
            r.aborted = !r.timedOut && r.code != 0;
        }
        return r;
#endif
    }

}
