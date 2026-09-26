// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#include "TaskScheduler.h"

namespace JLibTest {

    inline JLib::Task* MakeCtxTask(JLib::TaskScheduler& s, void (*fn)(void*), void* ctx,
                                   JLib::StackClass stack = JLib::StackClass::Standard) {
        return s.CreateTask(fn, ctx, JLib::TaskType::Fiber, stack);
    }

}
