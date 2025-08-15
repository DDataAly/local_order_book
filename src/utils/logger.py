import logging
import os

# Adding %(name)s to add a line specifying which module the log came from
def setup_logger():
    os.makedirs("data", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        filename="data/log.txt",
        filemode="w",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"  
    )