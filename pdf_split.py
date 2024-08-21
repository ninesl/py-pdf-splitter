import paths
import fitz #PyMuPDF
from pathlib import Path
import pprint as pp
import threading
import os
import time

FILES = set()
THREADS = []

def FILES_checker() -> None:
    def threads_running() -> bool:
        for thread in THREADS:
            if thread.is_alive():
                return True
            thread.handled = True
        return False
    
    while threads_running():
        time.sleep(1)

def get_pdf_threads() -> list[threading.Thread]:
    print(f"{paths.PARENT_DIRECTORY_WSL = }")
    print("Hello world")
    path_dir = Path.cwd() / f"{paths.PARENT_DIRECTORY_WSL}"
    
    country_list = [country for country in path_dir.iterdir()]
    threads: list[threading.Thread] = []

    for path in country_list:
        print(path.parts[-1])
    
    for filepath in country_list:
        threads.append(threading.Thread(target=thread_find_pdfs, args=(filepath,)))

    return threads

def thread_find_pdfs(filepath) -> None:
    print("Looking through", filepath.name)
    files_found = list(filepath.rglob("*.pdf"))
    for file in files_found:
        pdf_to_txt(file)

    print("\nFound all pdf files for", filepath.name)

def pdf_to_txt(file) -> bool:
    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]

    month: str = file.parts[-2]
    year: str = file.parts[-3]
    pdf_code: str = file.parts[-4]
    country_code: str = file.parts[-5]
    day_00: str = pdf_name[-6:-4]

    txt_file_dir: str = f"{paths.PARENT_DIRECTORY_WSL}/{pdf_code}/{year}/{month}/{day_00}"

    pdf_num = 1
    dir_exists: bool = os.path.isdir(txt_file_dir)

    if dir_exists:
        return False
    
    with fitz.open(file) as pdf_file:
        text = "" # concat to this. txt files could be parsed from more than one pdf page
        os.makedirs(txt_file_dir, exist_ok=True)

        for page_num in range(pdf_file.page_count):
            page_text = pdf_file.load_page(page_num).get_text("text")
            text += page_text
            
            if(paths.END_PAGE_TEXT in page_text[-paths.END_PAGE_LENGTH:]): # should be faster than searching whole page
                txt_filename = f"{pdf_name}_{pdf_num}.txt"
                with open(f"{txt_file_dir}/{txt_filename}", "w", encoding="utf-8") as txt_file:
                    txt_file.write(text)
                pdf_num += 1
                text = ""

        # Save any remaining text in case the document doesn't end with the marker
        if text.strip():
            print(f"Writing {pdf_name}_extra")
            with open(f"{txt_file_dir}/{pdf_name}_extra.txt", "w", encoding="utf-8") as extra_txt_file:
                extra_txt_file.write(text)

    print(f"{pdf_code}.", end="")
    return True

def main() -> None:
    global THREADS
    THREADS = get_pdf_threads()
    
    for thread in THREADS:
        thread.start()

main()