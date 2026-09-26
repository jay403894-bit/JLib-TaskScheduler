; SPDX-License-Identifier: BSD-3-Clause
; Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

; The AVX gate, defined in src/win32/FiberInit.cpp. Zero means "no AVX on this CPU", which is also
; the value it holds before its initialiser runs -- see the long comment beside it for why that is
; the safe direction to fail in.
EXTERN JLibCtxHasAvx:BYTE

.code
ContextSwitch PROC
    ; RCX = 'from' pointer (ptr to rsp), RDX = 'to' pointer (ptr to rsp)

    ; 0. Retire any live upper-YMM state BEFORE the legacy-SSE block below.
    ; A fiber that parks straight out of an AVX kernel leaves the upper halves of YMM live, and
    ; every movdqa below then pays an SSE/AVX transition. Measured at 85.8 ns/switch against
    ; 9.2 ns with this instruction present -- see bench/context_switch.cpp, which measures this
    ; exact routine as one of its arms.
    ;
    ; Destroying that state here is LEGAL, not a liberty: the upper halves of YMM are volatile
    ; across a call under the Win64 ABI, and a context switch is an opaque call. Anything the
    ; compiler had live up there is already spilled to the fiber's own stack, which travels with
    ; the fiber. tests/avx_suspend_test.cpp is the standing check on that.
    ;
    ; One vzeroupper covers the whole routine -- the saves below AND the restores after the stack
    ; swap -- because nothing in between re-dirties the upper state.
    ;
    ; Branched rather than unconditional because vzeroupper is itself an AVX instruction; see
    ; FiberInit.cpp. The load is from one always-hot cache line and the branch is perfectly
    ; predicted, so it does not show above the noise floor of the switch it guards.
    cmp byte ptr [JLibCtxHasAvx], 0
    je  @F
    vzeroupper
@@:

    ; 1. Save Callee-Saved GPRs
    push rbx
    push rbp
    push rdi
    push rsi
    push r12
    push r13
    push r14
    push r15

    ; 1b. Save the TEB's stack bounds into the outgoing frame, so they travel with the context.
    ; The TEB describes the RUNNING stack to the OS: the unwinder rejects any frame outside
    ; [StackLimit, StackBase] (a C++ throw on a fiber then kills the process), and __chkstk walks
    ; pages down from StackLimit for any frame over a page -- with the thread's own limit that walk
    ; runs through memory that is not this stack. gs: is the CURRENT thread's TEB, so these reads
    ; and the writes after the swap act on whichever worker runs the switch: migration-safe.
    ; 3 values + 8 pad keep RSP 8 mod 16 for the block below.
    ; JLIB_CTL_NO_TEB (negative control, diagnostic builds only): same 32-byte slot, no TEB access.
IFNDEF JLIB_CTL_NO_TEB
    mov rax, qword ptr gs:[8]        ; NT_TIB.StackBase (top)
    push rax
    mov rax, qword ptr gs:[10h]      ; NT_TIB.StackLimit (lowest committed)
    push rax
    mov rax, qword ptr gs:[1478h]    ; TEB.DeallocationStack (reservation base)
    push rax
    sub rsp, 8
ELSE
    sub rsp, 32
ENDIF

    ; 2. Save Non-Volatile XMM Registers (6 through 15).
    ; After 8 pushes + the 32-byte TEB block RSP is 8 mod 16 -- the 'call' into here pushed an
    ; 8-byte return address onto a 16-aligned stack, and 96 bytes preserve that offset. Reserve
    ; 168 = 160 (10 * 16 for xmm6-15) + 8 dummy: the extra 8 realigns RSP back to 16,
    ; so the XMM block is 16-aligned and movdqa (aligned) is legal. The dummy 8 bytes
    ; sit at [rsp+160 .. rsp+168), between the XMM block and the GPR pushes.
    sub rsp, 168
    movdqa [rsp + 0], xmm6
    movdqa [rsp + 16], xmm7
    movdqa [rsp + 32], xmm8
    movdqa [rsp + 48], xmm9
    movdqa [rsp + 64], xmm10
    movdqa [rsp + 80], xmm11
    movdqa [rsp + 96], xmm12
    movdqa [rsp + 112], xmm13
    movdqa [rsp + 128], xmm14
    movdqa [rsp + 144], xmm15

    ; 2b. Save nonvolatile FP control state into the formerly-dummy 8 bytes at [rsp+160].
    ; MXCSR control bits (rounding, FTZ/DAZ, exception masks) and the x87 control word are
    ; ABI-nonvolatile, and MXCSR is one physical register shared by all fibers on a worker.
    ; Without this, a fiber that sets a rounding/FTZ mode and yields silently leaks it into
    ; whatever fiber resumes next. 4 bytes MXCSR + 2 bytes FCW fit in the alignment slack.
    stmxcsr dword ptr [rsp + 160]
    fnstcw  word  ptr [rsp + 164]

    ; 3. Swap Stack Pointers
    mov [rcx], rsp    ; Save old RSP (16-aligned)
    mov rsp, [rdx]    ; Load new RSP (16-aligned)

    ; 4. Restore nonvolatile FP control state (Fiber::Init seeds sane defaults), then XMM.
    ldmxcsr dword ptr [rsp + 160]
    fldcw   word  ptr [rsp + 164]
    movdqa xmm6,  [rsp + 0]
    movdqa xmm7,  [rsp + 16]
    movdqa xmm8,  [rsp + 32]
    movdqa xmm9,  [rsp + 48]
    movdqa xmm10, [rsp + 64]
    movdqa xmm11, [rsp + 80]
    movdqa xmm12, [rsp + 96]
    movdqa xmm13, [rsp + 112]
    movdqa xmm14, [rsp + 128]
    movdqa xmm15, [rsp + 144]
    add rsp, 168      ; drop XMM block + dummy (back to the TEB block)

    ; 4b. Install the incoming context's stack bounds in this thread's TEB (see 1b).
IFNDEF JLIB_CTL_NO_TEB
    add rsp, 8
    pop rax
    mov qword ptr gs:[1478h], rax
    pop rax
    mov qword ptr gs:[10h], rax
    pop rax
    mov qword ptr gs:[8], rax
ELSE
    add rsp, 32
ENDIF

    ; 5. Restore Callee-Saved GPRs
    pop r15
    pop r14
    pop r13
    pop r12
    pop rsi
    pop rdi
    pop rbp
    pop rbx

    ret
ContextSwitch ENDP

; Entry trampoline for freshly-initialized fibers. The restore in ContextSwitch lands
; here at a 16-aligned RSP (0 mod 16). Fiber::Init seeded RBX with the C++ entry point.
; Entering via 'call' pushes an 8-byte return address, so the C++ wrapper is entered at
; 8 mod 16 -- the alignment the ABI/compiler expects of a normally-called function.
; The wrapper never returns (it ContextSwitches away); ud2 traps if it ever does.
FiberTrampoline PROC
    call rbx
    ud2
FiberTrampoline ENDP
END
