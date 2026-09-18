// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#if defined(JLIB_TSAN)
extern "C" {
	void* __tsan_get_current_fiber(void);
	void* __tsan_create_fiber(unsigned flags);
	void  __tsan_destroy_fiber(void* fiber);
	void  __tsan_switch_to_fiber(void* fiber, unsigned flags);
	void  __tsan_set_fiber_name(void* fiber, const char* name);
}
#endif

namespace JLib {
namespace tsan {

#if defined(JLIB_TSAN)
	inline constexpr bool kEnabled = true;

	inline void* CreateFiber()      { return __tsan_create_fiber(0); }
	inline void* CurrentFiber()     { return __tsan_get_current_fiber(); }
	inline void  Destroy(void* f)   { if (f) __tsan_destroy_fiber(f); }
	inline void  Name(void* f, const char* n) { if (f) __tsan_set_fiber_name(f, n); }

	inline void  SwitchTo(void* f)  { if (f) __tsan_switch_to_fiber(f, 0); }
#else
	inline constexpr bool kEnabled = false;

	inline void* CreateFiber()      { return nullptr; }
	inline void* CurrentFiber()     { return nullptr; }
	inline void  Destroy(void*)     {}
	inline void  Name(void*, const char*) {}
	inline void  SwitchTo(void*)    {}
#endif

}
}
