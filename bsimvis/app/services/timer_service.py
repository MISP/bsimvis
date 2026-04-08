import time
import threading
from contextlib import contextmanager

class Timer:
    def __init__(self, job_id=None):
        self.job_id = job_id
        self.start_time = time.time()
        self.timings = {
            "pure_python": 0.0,
            "db_queries": 0.0,
            "lua_scripts": 0.0
        }
        self.details = []
        self._lock = threading.Lock()

    def record(self, operation, duration, category):
        """Records an operation duration and category."""
        with self._lock:
            if category in self.timings:
                self.timings[category] += duration
            
            self.details.append({
                "op": operation,
                "time": round(duration, 6),
                "cat": category,
                "ts": round(time.time() - self.start_time, 4)
            })

    def finalize(self):
        """Calculates final stats and returns them."""
        total_time = time.time() - self.start_time
        with self._lock:
            # Python time is what's left after stripping DB and Lua
            self.timings["pure_python"] = max(0, total_time - self.timings["db_queries"] - self.timings["lua_scripts"])
            
            return {
                "job_id": self.job_id,
                "total_time": round(total_time, 4),
                "python_time": round(self.timings["pure_python"], 4),
                "db_time": round(self.timings["db_queries"], 4),
                "lua_time": round(self.timings["lua_scripts"], 4),
                "ops_count": len(self.details),
                "details": self.details
            }

# Set up thread-local storage for the active timer
_active_timers = threading.local()

def get_active_timer() -> Timer:
    """Returns the current active timer in this thread, or None."""
    return getattr(_active_timers, "timer", None)

@contextmanager
def job_timer(job_id):
    """Context manager to track a job's performance."""
    timer = Timer(job_id)
    _active_timers.timer = timer
    try:
        yield timer
    finally:
        if hasattr(_active_timers, "timer"):
            del _active_timers.timer
