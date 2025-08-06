# import os
# import shutil

# from config import ( 
#     LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, 
#     LOGS_DIR, TMP_INPUT, TMP_OUTPUT_ROOT, FINAL_OUTPUT_ROOT, CRF_VALUES
# )
# from helpers.logging_utils import log

# def ensure_dirs():
#     dirs = [LOCKS_DIR, TO_ASSIGN, IN_PROGRESS, DONE_DIR, FAILED_DIR, LOGS_DIR, TMP_INPUT]
#     for d in dirs:
#         os.makedirs(d, exist_ok=True)
#         log(f"Ensured directory exists: {d}", level="debug")
#     for crf in CRF_VALUES:
#         tmp_out = TMP_OUTPUT_ROOT.format(crf)
#         final_out = FINAL_OUTPUT_ROOT.format(crf)
#         # Clear tmp output dir
#         if os.path.exists(tmp_out):
#             shutil.rmtree(tmp_out)
#             log(f"Cleared temp output directory: {tmp_out}")
#         os.makedirs(tmp_out, exist_ok=True)
#         os.makedirs(final_out, exist_ok=True)
#         log(f"Ensured CRF directories: {tmp_out}, {final_out}", level="debug")
