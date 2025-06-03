import time
from datetime import datetime
import threading

class ResourceMonitor(threading.Thread):
    def __init__(self, process, interval=0.5):
        super().__init__()
        self.process = process
        self.interval = interval
        self.cpu_readings = []
        self.mem_readings = []
        self._running = True

    def run(self):
        while self._running:
            try:
                cpu = self.process.cpu_percent(interval=None)
                mem = self.process.memory_info().rss / (1024 ** 2)
                self.cpu_readings.append(cpu)
                self.mem_readings.append(mem)
                time.sleep(self.interval)
            except Exception:
                break

    def stop(self):
        self._running = False

    def get_average_cpu(self):
        return sum(self.cpu_readings) / len(self.cpu_readings) if self.cpu_readings else 0

    def get_max_memory(self):
        return max(self.mem_readings) if self.mem_readings else 0
