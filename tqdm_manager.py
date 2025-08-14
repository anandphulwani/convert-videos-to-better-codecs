# tqdm_manager.py
import multiprocessing
from tqdm import tqdm
from collections import OrderedDict
import threading
import time

class TqdmManager:
    def __init__(self, max_workers):
        self.lock = multiprocessing.RLock()
        self.max_workers = max_workers

        self._bars = {
            'misc': OrderedDict(),   # (c)
            'file': OrderedDict(),   # (b)
            'chunk': OrderedDict(),  # (a)
        }

        self._positions = {
            'misc': 0,
            'file': max_workers,
            'divider': max_workers + 1,
            'chunk': max_workers + 2,
        }

        self._active = True
        self._refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._refresh_thread.start()

    def _refresh_loop(self):
        while self._active:
            with self.lock:
                self._render_all()
            time.sleep(0.5)

    def _render_all(self):
        pos = self._positions['misc']

        # Show all misc bars (skip completed ones)
        for key, bar in list(self._bars['misc'].items()):
            if bar.n >= bar.total:
                bar.close()
                del self._bars['misc'][key]
            else:
                bar.refresh()
                pos += 1

        # File bars
        for _, bar in self._bars['file'].items():
            bar.refresh()

        # Divider
        # tqdm.write("=" * 50)

        # Chunk bars
        for _, bar in self._bars['chunk'].items():
            bar.refresh()

    def stop(self):
        self._active = False
        for cat in self._bars.values():
            for bar in cat.values():
                bar.close()

    def register_bar(self, bar_type, key, total, desc="", unit='B', extra_data=None):
        with self.lock:
            if bar_type == 'misc':
                position = self._positions['misc'] + len(self._bars['misc'])
            elif bar_type == 'file':
                position = self._positions['file'] + (extra_data.get('slot') or 0)
            elif bar_type == 'chunk':
                position = self._positions['chunk'] + len(self._bars['chunk'])
            else:
                raise ValueError("Unknown bar_type")

            bar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                position=position,
                dynamic_ncols=True,
                leave=False if bar_type == 'misc' else True,
            )
            self._bars[bar_type][key] = bar
            return bar

    def update_bar(self, bar_type, key, value):
        with self.lock:
            bar = self._bars[bar_type].get(key)
            if bar:
                delta = value - bar.n
                if delta > 0:
                    bar.update(delta)

    def set_description(self, bar_type, key, desc):
        with self.lock:
            bar = self._bars[bar_type].get(key)
            if bar:
                bar.set_description(desc)
