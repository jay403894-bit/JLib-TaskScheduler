// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "RunqDeque.h"

int JLibRunqDequeProbe() {
    JLib::RunqDeque d;
    return d.empty() ? 0 : 1;
}
