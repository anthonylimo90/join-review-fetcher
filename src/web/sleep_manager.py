"""Sleep prevention manager using macOS caffeinate."""
import subprocess
from typing import Optional


class SleepManager:
    """Manage caffeinate process to prevent macOS sleep during scraping."""

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None

    def start(self):
        """Start caffeinate to prevent system sleep.

        Flags:
        -d: Prevent display sleep
        -i: Prevent system idle sleep
        -m: Prevent disk sleep
        -s: Prevent system sleep (on AC power)
        """
        if self.is_active:
            return  # Already running

        try:
            self.process = subprocess.Popen(
                ['caffeinate', '-dims'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except FileNotFoundError:
            # caffeinate not available (non-macOS system)
            self.process = None

    def stop(self):
        """Terminate caffeinate process."""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            finally:
                self.process = None

    @property
    def is_active(self) -> bool:
        """Check if caffeinate is currently running."""
        return self.process is not None and self.process.poll() is None

    def __del__(self):
        """Ensure caffeinate is stopped when object is destroyed."""
        self.stop()


# Global sleep manager instance
sleep_manager = SleepManager()
