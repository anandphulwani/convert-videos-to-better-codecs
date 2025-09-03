# region Includes
# helpers/tqdm_manager.py
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
from helpers.call_http_url import call_http_url
from helpers.clear_terminal_below_cursor import clear_terminal_below_cursor
from config import MAX_WORKERS

BAR_TYPE_OTHER = "other"
BAR_TYPE_FILE = "file"
BAR_TYPE_CHUNK = "chunk"

_instance = None

# endregion

class TqdmManager:
