import paths
import fitz #PyMuPDF
from pathlib import Path
import pprint as pp
import threading
import os, shutil
import time

THREADS = []

def get_pdf_threads(country_code) -> list[threading.Thread]:
    print(f"{paths.PARENT_DIRECTORY_WSL = }")
    print("Hello world")
    path_dir = Path.cwd() / f"{paths.PARENT_DIRECTORY_WSL}" / f"{country_code}"
    
    country_list = [country for country in path_dir.iterdir()]
    threads: list[threading.Thread] = []

    for path in country_list:
        print(path.parts[-1])
    
    for filepath in country_list:
        threads.append(threading.Thread(target=thread_find_pdfs, args=(filepath,)))

    return threads

PDF_CODE_COUNT = 0
def thread_find_pdfs(filepath) -> None:
    global PDF_CODE_COUNT
    PDF_CODE_COUNT += 1
    print("Looking through", filepath.name, PDF_CODE_COUNT, "pdf_codes found")
    files_found = list(filepath.rglob("*.pdf"))
    for file in files_found:
        pdf_to_txt(file)
    PDF_CODE_COUNT -= 1
    print(filepath.name, PDF_CODE_COUNT, "pdf_codes to go")

DIRS_TO_CHECK = []
def pdf_to_txt(file) -> bool:
    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]

    month: str = file.parts[-2]
    year: str = file.parts[-3]
    pdf_code: str = file.parts[-4]
    country_code: str = file.parts[-5]
    day_00: str = pdf_name[-6:-4]

    txt_file_dir: str = f"{paths.PARENT_DIRECTORY_WSL}/{country_code}/{pdf_code}/{year}/{month}/{day_00}"

    pdf_num = 1
    dir_exists: bool = os.path.isdir(txt_file_dir)

    
    with fitz.open(file, filetype="pdf") as pdf_file:
        text = "" # concat to this. txt files could be parsed from more than one pdf page

        # check if any txt files were missed. hopefully doesn't bork everything
        # will rewrite over pdfs's txt files that are built from >1 pdf page
        if dir_exists:
            file_count: int = len([name for name in os.listdir(txt_file_dir) if os.path.isfile(os.path.join(txt_file_dir, name))])
            if file_count == pdf_file.page_count: #all txt files accounted for.
                return False
            print(f"{pdf_file.page_count = } {file_count = } {txt_file_dir}")
            DIRS_TO_CHECK.append(txt_file_dir)
        
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
    THREADS.extend(get_pdf_threads("USA"))
    THREADS.extend(get_pdf_threads("CAN"))
    
    for thread in THREADS:
        thread.start()

    for thread in THREADS:
        thread.join()

    print("All threads completed")
    pp.pprint(DIRS_TO_CHECK)

main()