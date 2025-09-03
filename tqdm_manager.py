# helpers/tqdm_manager.py
import sys
import threading
import time
import math
import requests
from tqdm import tqdm

BAR_TYPE_OTHER = "other"
BAR_TYPE_FILE = "file"
BAR_TYPE_CHUNK = "chunk"

_instance = None

class TqdmManager:
    def _format_size(self, n_bytes: int) -> str:
        """Format size from bytes -> KB or MB."""
        KB = 1024
        MB = KB * 1024
        if n_bytes < MB:
            return f"{n_bytes / KB:.1f} KB"
        else:
            return f"{n_bytes / MB:.1f} MB"

    def _format_elapsed(self, seconds: float) -> str:
        """Format elapsed time into s, mm:ss, or hh:mm:ss."""
        if seconds < 60:
            return f"{math.ceil(seconds)}s"
        elif seconds < 3600:
            mins, secs = divmod(math.ceil(seconds), 60)
            return f"{mins:02}m {secs:02}s"
        else:
            hrs, rem = divmod(math.ceil(seconds), 3600)
            mins, secs = divmod(rem, 60)
            return f"{hrs}h {mins:02}m {secs:02}s"

    def _format_time(self, seconds):
        mins, secs = divmod(int(seconds), 60)
        return f"{mins:02}:{secs:02}"

    def _refresh_positions(self):
        pos = self.position_base
        for bar_type in [BAR_TYPE_OTHER, BAR_TYPE_FILE, BAR_TYPE_CHUNK]:
            for _, bar in self.bars[bar_type]:
                bar.position = pos
                bar.refresh()
                pos += 1

    def _generate_desc(self, bar_type, bar_id, metadata):
        if bar_type == BAR_TYPE_CHUNK:
            return f"{bar_id[:5].capitalize()} {bar_id[5:]}"
        elif bar_type == BAR_TYPE_FILE:
            slot = metadata.get("slot", "")
            fname = metadata.get("filename", "file")
            return f"[Slot {slot}] {fname}"
        elif bar_type == BAR_TYPE_OTHER:
            return metadata.get("label", bar_id)
        return bar_id

    def clear_terminal_below_cursor(self):
        """Clears the terminal below the current cursor position."""
        with self.lock:
            sys.stdout.write("\033[J")  # ANSI code: erase below cursor
            sys.stdout.flush()

    def temporarily_disable_bars(self, pairs=[(15, 0.5), (45, 0.5), (90, 0.5), (120, 0.5)]):
        def worker(wait_before, disable_duration):
            time.sleep(wait_before)
            # self.call_http_url("Hiding all the bars now")
            with self.lock:
                for _, bar in self.bar_ids.values():
                    # bar.clear()
                    # bar.pause_timer()
                    # time.sleep(20)
                    bar.disable = True
                    self.clear_terminal_below_cursor()
            time.sleep(disable_duration)
            # self.call_http_url("Showing all the bars now")
            with self.lock:
                for _, bar in self.bar_ids.values():
                    bar.disable = False
                    bar.refresh()
                    # time.sleep(20)
                    # bar.resume_timer()

        for wait_before, disable_duration in pairs:
            threading.Thread(
                target=worker,
                args=(wait_before, disable_duration),
                daemon=True
            ).start()

    def call_http_url(self, message, timeout=10):
        # return
        try:        
            requests.post("https://ntfy.sh/anand_alerts", data=message.encode(encoding='utf-8'), timeout=timeout)
        except requests.exceptions.RequestException as e:
            print(f"Error calling URL: {e}")
            return None

    def __init__(self, base_position=0):
        self.lock = threading.RLock()
        self.bars = {BAR_TYPE_OTHER: [], BAR_TYPE_FILE: [], BAR_TYPE_CHUNK: []}
        self.metadata = {}
        self.start_times = {}
        self.last_values = {}
        self.bar_ids = {}  # bar_id -> (bar_type, tqdm instance)
        self.position_base = base_position
        self.next_position = 0

    def _get_position(self, bar_type):
        pos = self.position_base
        # if bar_type == BAR_TYPE_OTHER:
        #     return pos
        # pos += len(self.bars[BAR_TYPE_OTHER])
        if bar_type == BAR_TYPE_FILE:
            pos += len(self.bars[BAR_TYPE_FILE])
        if bar_type == BAR_TYPE_CHUNK:
            pos += len(self.bars[BAR_TYPE_FILE]) + len(self.bars[BAR_TYPE_CHUNK])
        return pos

    def create_bar(self, bar_type, bar_id, total, metadata=None, unit='it', unit_scale=False, unit_divisor=1):
        with self.lock:
            if bar_id in self.bar_ids:
                return  # already created

            position = self._get_position(bar_type)
            self.call_http_url(f"Bar position is: {position}, Creating bar: {bar_id}, bars.length: {len(self.bars)}, chunk_bars.length: {len(self.bars[BAR_TYPE_CHUNK])}, file_bars.length: {len(self.bars[BAR_TYPE_FILE])}")
            desc = self._generate_desc(bar_type, bar_id, metadata or {})

            bar = None
            if bar_type == BAR_TYPE_CHUNK:
                if position == 1:
                    divider = tqdm(
                        total=1,
                        bar_format="{bar}",
                        position=0,
                        dynamic_ncols=True,
                        ascii=" =",
                        leave=False
                        )
                    divider.update(1)
                    self.bars[bar_type].append(("divider", divider))
                    self.metadata["divider"] = {"type": bar_type, "meta": {}}
                    self.bar_ids["divider"] = (bar_type, divider)

                bar = tqdm( # bar = PausableTqdm(
                    total=total,
                    desc=desc,
                    position=position + 1,
                    dynamic_ncols=True,
                    unit=unit,
                    unit_scale=unit_scale,
                    unit_divisor=unit_divisor,
                    bar_format=("{l_bar}{bar}| {n_fmt}{unit}/{total_fmt}{unit} "
                        "[{elapsed}<{remaining}, {rate_fmt}{postfix}]"),
                    # leave=(bar_type != BAR_TYPE_OTHER),
                    leave=False
                )
                bar.update(1)
                bar.update(-1)
            elif bar_type == BAR_TYPE_FILE:
                for each_bar_type, each_bar in self.bar_ids.values():
                    if each_bar_type == BAR_TYPE_CHUNK:
                        tqdm.write(f"bar positon: {each_bar.position}")
                        each_bar.position = each_bar.position + 1
                        each_bar.refresh()
                
                bar = tqdm( # bar = PausableTqdm(
                    total=total,
                    desc=desc,
                    position=position,
                    dynamic_ncols=True,
                    unit=unit,
                    unit_scale=unit_scale,
                    unit_divisor=unit_divisor,
                    bar_format=("{l_bar}{bar}| {n_fmt}{unit}/{total_fmt}{unit} "
                        "[{elapsed}<{remaining}, {rate_fmt}{postfix}]"),
                    # leave=(bar_type != BAR_TYPE_OTHER),
                    leave=False
                )
                bar.update(1)
                bar.update(-1)

            self.bars[bar_type].append((bar_id, bar))
            self.metadata[bar_id] = {"type": bar_type, "meta": metadata or {}}
            self.start_times[bar_id] = time.time()
            self.last_values[bar_id] = 0
            self.bar_ids[bar_id] = (bar_type, bar)

    def progress(self, bar_id, current, show_eta=True):
        with self.lock:
            if bar_id not in self.bar_ids:
                return
            bar_type, bar = self.bar_ids[bar_id]
            last = self.last_values.get(bar_id, 0)
            delta = current - last
            self.last_values[bar_id] = current

            if delta > 0:
                bar.update(delta)

            postfix = ""
            if show_eta and bar.total and current > 0:
                elapsed = time.time() - self.start_times.get(bar_id, time.time())
                est_total = elapsed * (bar.total / float(current))
                eta = max(0.0, est_total - elapsed)
                postfix = f"ETA {self._format_time(eta)}"

            if postfix:
                bar.set_postfix_str(postfix)

    def finish_bar(self, bar_id):
        with self.lock:
            if bar_id not in self.bar_ids:
                return
            bar_type, bar = self.bar_ids[bar_id]

            if bar_type == BAR_TYPE_CHUNK:
                total = self._format_size(bar.total)
                elapsed = self._format_elapsed(bar.format_dict["elapsed"])

                postfix = f"size={total} • elapsed={elapsed}"
                bar.bar_format = f"{{desc}} ✓ {postfix}"
                bar.refresh()
            elif bar_type == BAR_TYPE_FILE:
                bar.close()
                del self.bar_ids[bar_id]
                bar.refresh()
                for each_bar_type, each_bar in self.bar_ids.values():
                    if each_bar_type == BAR_TYPE_CHUNK:
                        tqdm.write(f"bar positon: {each_bar.position}")
                        each_bar.position = each_bar.position - 1
                        each_bar.refresh()

            # Remove from list
            self.bars[bar_type] = [(bid, b) for bid, b in self.bars[bar_type] if bid != bar_id]

            # Cleanup
            # del self.bar_ids[bar_id]
            self.metadata.pop(bar_id, None)
            self.start_times.pop(bar_id, None)
            self.last_values.pop(bar_id, None)

            self._refresh_positions()

    # -------------------------- Event-queue interface --------------------------
    def attach_event_queue(self, q: "mp.Queue"):
        """
        Attach a multiprocessing Queue and start a background thread in the main
        process to consume progress events.
        """
        self._event_queue = q
        if self._event_thread and self._event_thread.is_alive():
            return
        self._stop_event.clear()
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()

    def _event_loop(self):
        while not self._stop_event.is_set():
            try:
                msg = self._event_queue.get(timeout=0.2)
            except Empty:
                continue
            if msg is None:
                # graceful stop signal
                break
            try:
                op = msg.get("op")
                if op == "create":
                    self.create_bar(
                        bar_type=msg["bar_type"],
                        bar_id=msg["bar_id"],
                        total=msg.get("total", 0),
                        metadata=msg.get("metadata", {}),
                        unit=msg.get("unit", "it"),
                        unit_scale=msg.get("unit_scale", False),
                        unit_divisor=msg.get("unit_divisor", 1),
                    )
                elif op == "update":
                    self.progress(msg["bar_id"], msg["current"])
                elif op == "finish":
                    self.finish_bar(msg["bar_id"])
                elif op == "close_all":
                    # Optional: close all file bars (e.g., on shutdown)
                    for bid in list(self.bar_ids.keys()):
                        self.finish_bar(bid)
                else:
                    # Unknown op; ignore
                    pass
            except Exception:
                # never let UI thread crash
                pass

    def stop_event_loop(self):
        if not self._event_thread:
            return
        self._stop_event.set()
        # Unblock queue .get()
        try:
            if self._event_queue is not None:
                self._event_queue.put_nowait(None)
        except Exception:
            pass
        self._event_thread.join(timeout=2)
        self._event_thread = None

# --------------------------- module-level helpers ---------------------------
def create_event_queue(ctx: "mp.context.BaseContext" = None) -> "mp.Queue":
    """
    Create a multiprocessing Queue appropriate for your start method.
    Call this in the main process and pass it into workers.
    """
    if ctx is None:
        ctx = mp.get_context()  # use default
    return ctx.Queue()

def get_tqdm_manager():
    global _instance
    if _instance is None:
        _instance = TqdmManager(base_position=0)
    return _instance