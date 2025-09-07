import os
from config import IN_PROGRESS, DONE_DIR, FINAL_OUTPUT_ROOT, CRF_VALUES

from helpers.copy_and_move_with_progress import move_with_progress

def move_done_if_all_crf_outputs_exist(event_queue):
    for root, _, files in os.walk(IN_PROGRESS):
        for file in files:
            src_file = os.path.join(root, file)
            rel_path = os.path.relpath(src_file, IN_PROGRESS)

            # Check if file exists in all FINAL_OUTPUT_ROOT paths
            if all(os.path.exists(os.path.join(FINAL_OUTPUT_ROOT.format(crf), rel_path)) for crf in CRF_VALUES):
                dst_file = os.path.join(DONE_DIR, rel_path)
                move_with_progress(src_file, dst_file, event_queue)
