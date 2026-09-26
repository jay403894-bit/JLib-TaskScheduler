// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cassert>

namespace JLib {

	class WaitPrimitive {
	public:
		virtual ~WaitPrimitive();
	protected:
		WaitPrimitive();
		
		void LeaveRegistry() noexcept;

		// True once Join has begun shutting the pool down (stopFlag).
		static bool PoolStopping() noexcept;

		virtual void DrainForShutdown() {
			assert(false && "WaitPrimitive: derived destructor must call LeaveRegistry() FIRST -- "
			                "this object was drained while being destroyed");
		}

	private:
		WaitPrimitive* nextPrimitive_ = nullptr;
		WaitPrimitive* prevPrimitive_ = nullptr;
		friend class TaskScheduler;
	};

}
