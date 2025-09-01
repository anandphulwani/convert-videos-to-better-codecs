import threading
import time
import random
from queue import Empty
from enum import Enum
import multiprocessing as mp
from tqdm import tqdm
from dataclasses import dataclass
from helpers.format_elapsed import format_elapsed
from helpers.format_size import format_size
from helpers.format_time import format_time
from helpers.clear_terminal_below_cursor import clear_terminal_below_cursor
from datetime import datetime

class BAR_TYPE(Enum):
    OTHER = 1
    FILE_WAITING = 2
    FILE = 3
    CHUNK_DIVIDER = 4
    CHUNK = 5

@dataclass
class BarEntry:
    bar_id: str
    bar_type: BAR_TYPE
    bar: tqdm
    total: float | None = None
    metadata: dict | None = None
    desc: str | None = None
    position: int | None = None
    start_time: float | None = None
    end_time: float | None = None
    last_value: float | int | None = None
    bar_format: str | None = None
    is_done: bool = False

_instance = None
_event_queue = None

class TqdmManager:
    def change_state_of_bars(self, disable):
        with self.lock:
            for bar_list in self.bars.values():
                for _, bar_entry in bar_list:
                    bar = bar_entry.bar
                    bar.disable = disable
                    bar.refresh()
            clear_terminal_below_cursor() if disable else None

    def refresh_bars(self):
        if len(self.bars[BAR_TYPE.CHUNK]) > self.chunk_bar_limit:
            self._enforce_chunk_bar_limit()
        self.change_state_of_bars(True)
        self.change_state_of_bars(False)

    def remove_bar_from_gui(self, bar):
        with tqdm.get_lock():
            bar.clear()
            bar.refresh()
            bar.close()
            del bar
            clear_terminal_below_cursor()
    
    def remove_bar_and_get_bar_entry(self, bar_id, bar_type = None, isRefreshBars = True):
        res = self.get_or_pop_bar(bar_id, bar_type, pop=True)
        bar_entry = res[2]
        bar = bar_entry.bar
        self.remove_bar_from_gui(bar)
        isRefreshBars = False
        self.refresh_bars() if isRefreshBars else None
        return bar_entry

    def __init__(self, base_position=0):
        self.lock = threading.RLock()
        self.bars = {bar_type: [] for bar_type in BAR_TYPE}
        self.base_position = base_position
        self._event_thread = None
        self._event_queue = None
        self._stop_event = threading.Event()
        self.chunk_bar_limit = 10
        self.create_slot_bars(base_position)

    def bar_id_exists(self, bar_id):
        return any(bar_id == existing_id for bars in self.bars.values() for existing_id, _ in bars)

    def get_or_pop_bar(self, bar_id, bar_type=None, pop=False):
        bar_sources = (
            [(bar_type, self.bars.get(bar_type))]
            if bar_type is not None
            else self.bars.items()
        )

        for bt, bar_list in bar_sources:
            for i, (existing_id, bar_entry) in enumerate(bar_list):
                if existing_id == bar_id:
                    bar_id_, bar_entry_ = bar_list.pop(i) if pop else (existing_id, bar_entry)
                    return bt, bar_id_, bar_entry_

        return None

    def _generate_desc(self, bar_type, bar_id, metadata):
        if bar_type == BAR_TYPE.CHUNK:
            return f"{bar_id[:5].capitalize()} {bar_id[5:]}"
        elif bar_type == BAR_TYPE.FILE:
            slot_no = metadata.get("slot_no")
            crf = f"{metadata.get('crf'):02}"
            fname = metadata.get("filename", metadata.get("label", "file"))
            return f"Slot {slot_no} | [{crf}] {fname}" if slot_no and crf else f"{fname}"
        elif bar_type == BAR_TYPE.FILE_WAITING:
            slot_no = metadata.get("slot_no")
            return f"Slot {slot_no:02} | Waiting..."
        elif bar_type == BAR_TYPE.OTHER:
            return metadata.get("name", metadata.get("filename", bar_id))
        return bar_id

    def create_slot_bar(self, slot_no):
        msg = {
            "bar_type": BAR_TYPE.FILE_WAITING,
            "bar_id": f"waiting_file_slot_{slot_no:02}",
            "metadata": {"slot_no": f"{slot_no:02}"},
        }
        self.create_bar(msg)

    def create_slot_bars(self, no_of_bars):
        with self.lock:
            for index in range(1, no_of_bars + 1):
                self.create_slot_bar(index)

    def _get_position(self, bar_type, bar_id = None):
        pos = self.base_position
        if bar_type == BAR_TYPE.FILE:
            return len(self.bars[BAR_TYPE.OTHER]) + int(bar_id[-2:]) - 1 
        if bar_type == BAR_TYPE.FILE_WAITING:
            return len(self.bars[BAR_TYPE.OTHER]) + int(bar_id[-2:]) - 1
        if bar_type == BAR_TYPE.CHUNK_DIVIDER:
            return len(self.bars[BAR_TYPE.OTHER]) + self.base_position
        if bar_type == BAR_TYPE.CHUNK:
            pos = (max(bar[1].position for bar in self.bars[BAR_TYPE.CHUNK]) + 1
                if self.bars[BAR_TYPE.CHUNK]
                else len(self.bars[BAR_TYPE.OTHER]) + self.base_position + 1)
        return pos

    def _snapshot_bar_state(self, bar):
        try:
            current_n = getattr(bar, "n", 0)
        except Exception:
            current_n = 0
        return {"n": max(0, int(current_n))}

    def shift_pos(self, bar_id, new_position, bar_entry_keep):
        bar_type = bar_entry_keep.bar_type

        # get & remove the old bar
        self.remove_bar_and_get_bar_entry(bar_id, bar_type)
        state = self._snapshot_bar_state(bar_entry_keep.bar) if bar_entry_keep.bar else {"n": 0}
        
        bar = self._create_tqdm_bar(bar_type, bar_id, None, None, new_position, None, bar_entry_keep)
        bar.refresh()

        if not bar_entry_keep.is_done and bar_type != BAR_TYPE.CHUNK_DIVIDER:
            # restore state
            n = state.get("n", 0)
            bar.update(n)

        bar.refresh()

    def shift_all_bars_one_step_down(self):
        with self.lock:
            # 1) Collect (bar_id, bar_entry) pairs flat
            items = []
            for bar_list in self.bars.values():
                for bar_id, bar_entry in bar_list:
                    items.append((bar_id, bar_entry))

            # 2) Sort by current position descending (tie-break by id for determinism)
            items.sort(key=lambda x: (getattr(x[1], "position", 0), x[0]), reverse=True)

            # 3) Shift in that order
            for bar_id, bar_entry in items:
                self.shift_pos(bar_id, bar_entry.position + 1, bar_entry)

    def shift_all_bars_one_step_up(self):
        with self.lock:
            # 1) Collect all (bar_id, bar_entry) pairs
            items = []
            for bar_list in self.bars.values():
                for bar_id, bar_entry in bar_list:
                    items.append((bar_id, bar_entry))

            # 2) Validate positions before mutating
            for bar_id, bar_entry in items:
                if getattr(bar_entry, "position", 0) < 1:
                    raise ValueError(
                        f"Invalid position for bar_id={bar_id}: {bar_entry.position}. "
                        "Position must be >= 1 before shifting."
                    )

            # 3) Sort ASC by position (tie-break by id for determinism)
            items.sort(key=lambda x: (x[1].position, x[0]))

            # 4) Shift up in that order
            for bar_id, bar_entry in items:
                self.shift_pos(bar_id, bar_entry.position - 1, bar_entry)

    def _enforce_chunk_bar_limit(self):
        bar_list = self.bars[BAR_TYPE.CHUNK]
        if not bar_list:
            return

        first_chunk_bar = bar_list[0][1].bar
        first_chunk_bar_positon = abs(getattr(
            first_chunk_bar, "pos", 
            getattr(first_chunk_bar, "_pos", self.base_position + 1)
        ))

        # Step 1: Identify candidates for removal (✓ in description)
        bars_to_remove = [pair for pair in bar_list if " ✓ " in pair[1].bar.bar_format]
        if not bars_to_remove:
            return

        # Step 2: Remove only the required number
        no_of_bars_to_removed = len(bar_list) - self.chunk_bar_limit
        for bar_id_rm, _ in bars_to_remove[:no_of_bars_to_removed]:
            self.remove_bar_and_get_bar_entry(bar_id_rm)

        # Step 3: Recreate remaining chunk bars in contiguous order
        if self.bars[BAR_TYPE.CHUNK]:
            remaining = [pair[0] for pair in self.bars[BAR_TYPE.CHUNK]]
            for i, bar_id_keep in enumerate(remaining):
                new_pos = first_chunk_bar_positon + i
                bar_entry_keep = self.get_or_pop_bar(bar_id_keep, pop=False)[2]
                self.shift_pos(bar_id_keep, new_pos, bar_entry_keep)

    def _create_tqdm_bar(self, bar_type, bar_id, total=None, desc=None, position=None, metadata={}, older_bar_entry=None):
        is_done=False
        start_time=None
        end_time=None
        last_value=None
        if older_bar_entry:
            total=older_bar_entry.total
            metadata=older_bar_entry.metadata
            desc=older_bar_entry.desc
            start_time=older_bar_entry.start_time
            end_time=older_bar_entry.end_time
            last_value=older_bar_entry.last_value
            is_done=older_bar_entry.is_done

        if bar_type == BAR_TYPE.CHUNK:
            if not self.bar_id_exists("chunk_divider"):
                bar = self._create_tqdm_bar(BAR_TYPE.CHUNK_DIVIDER, "chunk_divider")
        elif bar_type == BAR_TYPE.FILE:
            waiting_key = f"waiting_{bar_id}"
            if self.bar_id_exists(waiting_key):
                self.remove_bar_and_get_bar_entry(waiting_key)        

        if bar_type == BAR_TYPE.CHUNK or bar_type == BAR_TYPE.FILE:
            position = (
                position
                if position else
                self._get_position(bar_type, bar_id)
            )
            bar_format="{l_bar}{bar}| {n_fmt}{unit}/{total_fmt}{unit} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
            ascii=None
            unit=metadata.get("unit", "B")
            unit_scale=metadata.get("unit_scale", True)
            unit_divisor=metadata.get("unit_divisor", 1024)
        elif bar_type == BAR_TYPE.FILE_WAITING or bar_type == BAR_TYPE.CHUNK_DIVIDER:
            total=1
            desc=desc if bar_type == BAR_TYPE.FILE_WAITING else None
            position = (
                position
                if position else
                self._get_position(bar_type, bar_id)
            )
            bar_format="{desc}" if bar_type == BAR_TYPE.FILE_WAITING else "{bar}"
            ascii=None if bar_type == BAR_TYPE.FILE_WAITING else " ="         
            unit=metadata.get("unit", "")
            unit_scale=metadata.get("unit_scale", False)
            unit_divisor=metadata.get("unit_divisor", 1000)
        elif bar_type == BAR_TYPE.OTHER:
            total=total
            position = (
                position
                if position else
                0
            )
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
            ascii=None
            unit=metadata.get("unit", "it")
            unit_scale=metadata.get("unit_scale", False)
            unit_divisor=metadata.get("unit_divisor", 1000)
        else:
            raise ValueError("Unsupported bar type")

        bar = tqdm(
            total=total,
            desc=desc,
            position=position,
            bar_format=bar_format,
            ascii=ascii,
            unit=unit,
            unit_scale=unit_scale,
            unit_divisor=unit_divisor,
            dynamic_ncols=True,
            leave=False
        )
        bar.update(1 if bar_type == BAR_TYPE.CHUNK_DIVIDER or bar_type == BAR_TYPE.FILE_WAITING else 0)
        bar_entry = BarEntry(
            bar_id=bar_id,
            bar_type=bar_type,
            bar=bar,
            total=total,
            metadata=metadata,
            desc=desc,
            position=position,
            start_time=start_time or time.time(),
            end_time=end_time or None,
            last_value=last_value or 0,
            bar_format=bar_format,
            is_done=is_done
        )
        if bar_entry.is_done:
            bar.bar_format = older_bar_entry.bar_format
            bar_entry.bar_format = older_bar_entry.bar_format
        self.bars[bar_type].append((bar_id, bar_entry))
        return bar

    def create_bar(self, msg):
        with self.lock:
            bar_type = msg["bar_type"]
            bar_id = msg["bar_id"]
            total = msg.get("total")
            metadata = msg.get("metadata")

            if self.bar_id_exists(bar_id):
                return

            if bar_type == BAR_TYPE.OTHER:
                self.shift_all_bars_one_step_down()

            desc = self._generate_desc(bar_type, bar_id, metadata)
            self._create_tqdm_bar(bar_type, bar_id, total, desc, None, metadata)

            self.refresh_bars()

    def progress(self, bar_id, current, show_eta=True):
        with self.lock:
            if not self.bar_id_exists(bar_id):
                return
            _, _, bar_entry = self.get_or_pop_bar(bar_id)
            bar = bar_entry.bar

            last = bar_entry.last_value or 0
            delta = current - last
            bar_entry.last_value = current

            if delta > 0:
                bar.update(delta)

            postfix = ""
            if show_eta and bar.total and current > 0:
                elapsed = time.time() - bar_entry.start_time or time.time()
                est_total = elapsed * (bar.total / float(current))
                eta = max(0.0, est_total - elapsed)
                postfix = f"ETA {format_time(eta)}"

            if postfix:
                bar.set_postfix_str(postfix)

    def finish_bar(self, bar_id):
        with self.lock:
            if not self.bar_id_exists(bar_id):
                return
            bar_type, _, bar_entry = self.get_or_pop_bar(bar_id)
            bar = bar_entry.bar
            bar_entry.end_time=time.time()

            if bar_type == BAR_TYPE.CHUNK:
                total = format_size(bar.total)
                elapsed = format_elapsed(bar_entry.end_time-bar_entry.start_time)

                postfix = f"size={total} • elapsed={elapsed}"
                bar.bar_format = f"{{desc}} ✓ {postfix}"
                bar.refresh()

                bar_entry.bar_format = bar.bar_format
            elif bar_type == BAR_TYPE.FILE:
                self.remove_bar_and_get_bar_entry(bar_id, isRefreshBars=False)
                self.create_slot_bar(int(bar_id[-2:]))
            elif bar_type == BAR_TYPE.OTHER:
                self.remove_bar_and_get_bar_entry(bar_id, isRefreshBars=False)
                self.shift_all_bars_one_step_up()
            bar_entry.is_done=True
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
                    self.create_bar(msg)
                elif op == "update":
                    self.progress(msg["bar_id"], msg["current"])
                elif op == "finish":
                    self.finish_bar(msg["bar_id"])
                elif op == "close_all":
                    # Optional: close all file bars (e.g., on shutdown)
                    for bar_list in self.bars.values():
                        for bar_id, _ in list(bar_list):
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

def get_random_value_for_id():
    now = datetime.now()
    # Format as YYYYMMDDhhmmssSSSSSS (SSSSSS = full 6-digit microseconds)
    timestamp = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond:06d}"

    number = random.randint(1, 9999)
    formatted_number = f"{number:04}"

    return f"{timestamp}{formatted_number}"

def get_tqdm_manager():
    global _instance, _event_queue
    if _instance is None:
        _instance = TqdmManager(base_position=6)
    if _event_queue is None:
        _event_queue = create_event_queue()
        _instance.attach_event_queue(_event_queue)
    return _instance

def get_event_queue():
    return _event_queue
# endregion

