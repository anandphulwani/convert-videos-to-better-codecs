import sys
import threading
import time
import requests
from queue import Empty
import multiprocessing as mp
from tqdm import tqdm
from helpers.format_elapsed import format_elapsed
from helpers.format_size import format_size
from helpers.format_time import format_time
from helpers.clear_terminal_below_cursor import clear_terminal_below_cursor
from config import MAX_WORKERS

from helpers.call_http_url import call_http_url

BAR_TYPE_OTHER = "other"
BAR_TYPE_FILE = "file"
BAR_TYPE_CHUNK = "chunk"

_instance = None

class TqdmManager:
    def change_state_of_bars(self, disable):
        with self.lock:
            for bar_list in self.bars.values():
                for _, bar in bar_list:
                    bar.disable = disable
                    bar.refresh()
            clear_terminal_below_cursor() if disable else None

    def refresh_bars(self):
        self.change_state_of_bars(True)
        self.change_state_of_bars(False)

    def __init__(self, base_position=0):
        self.lock = threading.RLock()
        self.bars = {BAR_TYPE_OTHER: [], BAR_TYPE_FILE: [], BAR_TYPE_CHUNK: []}
        self.start_times = {}
        self.last_values = {}
        self.position_base = base_position
        self._event_thread = None
        self._event_queue = None
        self._stop_event = threading.Event()
        self.chunk_bar_limit = 2
        self.create_slot_bars(base_position)

    def bar_id_exists(self, bar_id):
        return any(bar_id == existing_id for bars in self.bars.values() for existing_id, _ in bars)

    def get_or_pop_bar(self, bar_id, bar_type=None, pop=False):
        if bar_type is not None:
            bar_list = self.bars.get(bar_type, [])
            for i, (existing_id, bar) in enumerate(bar_list):
                if existing_id == bar_id:
                    if pop:
                        bar_id_, bar_ = bar_list.pop(i)
                    else:
                        bar_id_, bar_ = bar_list[i]
                    return bar_type, bar_id_, bar_
        else:
            for bt, bar_list in self.bars.items():
                for i, (existing_id, bar) in enumerate(bar_list):
                    if existing_id == bar_id:
                        if pop:
                            bar_id_, bar_ = bar_list.pop(i)
                        else:
                            bar_id_, bar_ = bar_list[i]
                        return bt, bar_id_, bar_
        return None

    def _generate_desc(self, bar_type, bar_id, metadata):
        if bar_type == BAR_TYPE_CHUNK:
            return f"{bar_id[:5].capitalize()} {bar_id[5:]}"
        elif bar_type == BAR_TYPE_FILE:
            slot_no = metadata.get("slot_no")
            crf = f"{metadata.get('crf'):02}"
            fname = metadata.get("filename", metadata.get("label", "file"))
            return f"Slot {slot_no} | [{crf}] {fname}" if slot_no and crf else f"{fname}"
        elif bar_type == BAR_TYPE_OTHER:
            return metadata.get("label", bar_id)
        return bar_id

    def create_slot_bar(self, slot_no):
        bar = tqdm(
            total=1,
            desc=f"Slot {slot_no:02} | Waiting...",
            position=slot_no - 1,
            dynamic_ncols=True,
            bar_format=("{desc}"),
            leave=False
        )
        bar.update(1)
        self.bars[BAR_TYPE_FILE].append((f"waiting_file_slot_{slot_no:02}", bar))

    def create_slot_bars(self, no_of_bars):
        with self.lock:
            for index in range(1, no_of_bars + 1):
                self.create_slot_bar(index)

    def _get_position(self, bar_type):
        pos = self.position_base
        if bar_type == BAR_TYPE_FILE:
            return None
        if bar_type == BAR_TYPE_CHUNK:
            pos += 1 + len(self.bars[BAR_TYPE_CHUNK])
        return pos

    def create_bar(self, bar_type, bar_id, total, metadata=None, unit='it', unit_scale=False, unit_divisor=1):
        with self.lock:
            if self.bar_id_exists(bar_id):
                return

            position = self._get_position(bar_type)
            desc = self._generate_desc(bar_type, bar_id, metadata or {})

            if bar_type == BAR_TYPE_CHUNK:
                bar = self._create_chunk_bar(bar_id, total, position, unit, unit_scale, unit_divisor, desc)
            elif bar_type == BAR_TYPE_FILE:
                bar = self._create_file_bar(bar_id, total, unit, unit_scale, unit_divisor, desc)

            self.bars[bar_type].append((bar_id, bar))
            self.start_times[bar_id] = time.time()
            self.last_values[bar_id] = 0

            # call_http_url(f"CHUNK BARS CHECK: {len(self.bars[BAR_TYPE_CHUNK])} > {self.chunk_bar_limit}")
            if len(self.bars[BAR_TYPE_CHUNK]) > (self.chunk_bar_limit + 1): # +1 is for divider
                # call_http_url(f"Inside repositioner")
                no_of_bars_to_removed = len(self.bars[BAR_TYPE_CHUNK]) - self.chunk_bar_limit
                call_http_url(f"no_of_bars_to_removed: {no_of_bars_to_removed}")
                bar_list = self.bars[BAR_TYPE_CHUNK]
                first_chunk_bar_positon = abs(getattr(bar_list[1][1], "pos", getattr(bar_list[1][1], "_pos", "no pos attr")))

                call_http_url(f"first_chunk_bar_positon: {first_chunk_bar_positon}")

                # Step 1: Identify candidates for removal (those with " ✓ " in description)
                bars_to_remove = [bar for bar in bar_list if " ✓ " in bar[1].description]

                # Step 2: Remove only the required number
                for bar in bars_to_remove[:no_of_bars_to_removed]:
                    bar.close()
                    bar_list.remove(bar)

                # Step 3: Reindex remaining bars in incrementing order
                if bar_list:  # ensure not empty
                    for i, (bar_id, bar) in enumerate(bar_list):
                        bar.pos = first_chunk_bar_positon + i
                        bar.position = first_chunk_bar_positon + i

            self.refresh_bars()
    
    def _create_chunk_bar(self, bar_id, total, position, unit, unit_scale, unit_divisor, desc):
        if not self.bar_id_exists("divider"):
            self._create_divider(position)
            position += 1

        bar = tqdm(
            total=total,
            desc=desc,
            position=position,
            dynamic_ncols=True,
            unit=unit,
            unit_scale=unit_scale,
            unit_divisor=unit_divisor,
            bar_format="{l_bar}{bar}| {n_fmt}{unit}/{total_fmt}{unit} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            leave=False
        )
        bar.update(0)
        return bar

    def _create_divider(self, position):
        divider = tqdm(
            total=1,
            bar_format="{bar}",
            position=position,
            dynamic_ncols=True,
            ascii=" =",
            leave=False
        )
        divider.update(1)
        self.bars[BAR_TYPE_CHUNK].append(("divider", divider))

    def _create_file_bar(self, bar_id, total, unit, unit_scale, unit_divisor, desc):
        waiting_key = f"waiting_{bar_id}"
        if self.bar_id_exists(waiting_key):
            self._clear_waiting_bar(waiting_key)

        position = int(bar_id[-2:])
        bar = tqdm(
            total=total,
            desc=desc,
            position=position - 1,
            dynamic_ncols=True,
            unit=unit,
            unit_scale=unit_scale,
            unit_divisor=unit_divisor,
            bar_format="{l_bar}{bar}| {n_fmt}{unit}/{total_fmt}{unit} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            leave=False
        )
        bar.update(0)
        return bar

    def _clear_waiting_bar(self, waiting_key):
        _, _, wait_bar = self.get_or_pop_bar(waiting_key, pop = True)
        with tqdm.get_lock():
            wait_bar.clear()
            wait_bar.refresh()
            wait_bar.close()
            del wait_bar
            clear_terminal_below_cursor()

    def progress(self, bar_id, current, show_eta=True):
        with self.lock:
            if not self.bar_id_exists(bar_id):
                return
            _, _, bar = self.get_or_pop_bar(bar_id)
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
                postfix = f"ETA {format_time(eta)}"

            if postfix:
                bar.set_postfix_str(postfix)

    def finish_bar(self, bar_id):
        with self.lock:
            if not self.bar_id_exists(bar_id):
                return
            bar_type, _, bar = self.get_or_pop_bar(bar_id)

            if bar_type == BAR_TYPE_CHUNK:
                total = format_size(bar.total)
                elapsed = format_elapsed(bar.format_dict["elapsed"])

                postfix = f"size={total} • elapsed={elapsed}"
                bar.bar_format = f"{{desc}} ✓ {postfix}"
                bar.refresh()

                # Cleanup
                self.start_times.pop(bar_id, None)
                self.last_values.pop(bar_id, None)
                self.refresh_bars()
            elif bar_type == BAR_TYPE_FILE:
                bar.close()
                self.create_slot_bar(int(bar_id[-2:]))
                self.get_or_pop_bar(bar_id, pop = True)
                # Cleanup
                self.start_times.pop(bar_id, None)
                self.last_values.pop(bar_id, None)
                self.refresh_bars()

# region Queue creation
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
                    for bar_list in self.bars.values():
                        for bar_id, _ in bar_list:
                            self.finish_bar(bar_id)
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
        _instance = TqdmManager(base_position=6)
    return _instance
# endregion

