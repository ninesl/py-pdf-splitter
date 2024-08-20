import paths
import fitz #PyMuPDF
from pathlib import Path
import pprint as pp
import threading
import time

FILES = {}
THREADS = []

def start_pdf_conversion() -> list[threading.Thread]:
    print(f"{paths.PARENT_DIRECTORY_WSL = }")
    print("Hello world")
    path_dir = Path.cwd() / f"{paths.PARENT_DIRECTORY_WSL}"
    
    country_list = [country for country in path_dir.iterdir()]
    threads: list[threading.Thread] = []
    
    for filepath in country_list:
        threads.append(threading.Thread(target=find_pdfs, daemon=False, args=(filepath,)))

    return threads
        
def find_pdfs(filepath) -> None:
    print("Looking through", filepath.name)
    # files = list(filepath.rglob("*.pdf"))
    for file in range(10):
        if file not in FILES:
            FILES[file] = 0
        FILES[file] += 1
        time.sleep(.5)

    print("Found all pdf files for", filepath.name)

def FILES_checker() -> None:
    def threads_running() -> bool:
        for thread in THREADS:
            if thread.is_alive():
                return True
            thread.handled = True
        return False
    
    while threads_running():
        time.sleep(.1)
        pp.pprint(FILES)

def main() -> None:
    global THREADS
    THREADS = start_pdf_conversion()
    
    for thread in THREADS:
        time.sleep(.25)
        thread.start()
    
    threading.Thread(target=FILES_checker, daemon=True).start()

main()