import logging
import os

def setup_logger():
    os.makedirs("data", exist_ok=True)
    console_handler = logging.StreamHandler()
    console_handler.setLevel('DEBUG')
    file_handler = logging.FileHandler('data/run_logs.log')
    file_handler.setLevel('INFO')
    logging.basicConfig(
        level=logging.DEBUG,
        handlers = [console_handler, file_handler],
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"  
    )