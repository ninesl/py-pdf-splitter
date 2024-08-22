import paths
import fitz #PyMuPDF
from pathlib import Path
import pprint as pp
import threading
import os, pathlib

THREADS: list[threading.Thread] = []

def get_pdf_threads(country_code: str) -> list[threading.Thread]:
    print(f"{paths.PARENT_DIRECTORY_WSL = }")
    print("Hello world")
    path_dir: pathlib.PosixPath = Path.cwd() / f"{paths.PARENT_DIRECTORY_WSL}" / f"{country_code}"
    
    country_list: list[pathlib.PosixPath] = [country for country in path_dir.iterdir()]
    threads: list[threading.Thread] = []

    for path in country_list:
        print(path.parts[-1])
    
    for filepath in country_list:
        print(type(filepath))
        # threads.append(threading.Thread(target=thread_find_pdfs, args=(filepath,)))

    return threads

PDF_CODE_COUNT: int = 0
def thread_find_pdfs(filepath: pathlib.PosixPath) -> None:
    global PDF_CODE_COUNT
    PDF_CODE_COUNT += 1
    print("Looking through", filepath.name, PDF_CODE_COUNT, "pdf_codes found")
    files_found: list[pathlib.PosixPath] = list(filepath.rglob("*.pdf"))
    for file in files_found:
        pdf_to_txt(file)
    PDF_CODE_COUNT -= 1
    print("|", filepath.name, PDF_CODE_COUNT, "pdf_codes to go")

def pdf_to_txt(file: pathlib.PosixPath) -> None:
    try:
        if write_parse_pdf(file):
            print(f".", end="")
    except:
        print("\nError parsing.", file)

def write_parse_pdf(file: pathlib.PosixPath) -> bool:
    txt_file_dir: str = find_txt_file_dir(file)

    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]

    dir_exists: bool = os.path.isdir(txt_file_dir)

    pdf_num: int = 1
    with fitz.open(file, filetype="pdf") as pdf_file:
        text: str = "" # concat to this. txt files could be parsed from more than one pdf page

        # check if any txt files were missed. hopefully doesn't bork everything
        # will rewrite over pdfs's txt files that are built from >1 pdf page
        # added -2. Some pdfs have 10 pages but 8 txt files to gen, and so on. -2 seems safe for a cutoff
        if dir_exists:
            file_count: int = len([name for name in os.listdir(txt_file_dir) if os.path.isfile(os.path.join(txt_file_dir, name))])
            if file_count < pdf_file.page_count - 2: #all txt files accounted for.
                return False
        
        os.makedirs(txt_file_dir, exist_ok=True)

        for page_num in range(pdf_file.page_count):
            page_text: str = pdf_file.load_page(page_num).get_text("text")
            text += page_text
            
            if(paths.END_PAGE_TEXT in page_text[-paths.END_PAGE_LENGTH:]): # should be faster than searching whole page
                txt_filename: str = f"{pdf_name}_{pdf_num}.txt"
                with open(f"{txt_file_dir}/{txt_filename}", "w", encoding="utf-8") as txt_file:
                    txt_file.write(text)
                pdf_num += 1
                text = ""

        # Save any remaining text in case the document doesn't end with the marker
        if text.strip():
            print(f"Writing {pdf_name}_extra")
            with open(f"{txt_file_dir}/{pdf_name}_extra.txt", "w", encoding="utf-8") as extra_txt_file:
                extra_txt_file.write(text)

    return True

def find_txt_file_dir(file: pathlib.PosixPath) -> str:
    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]
    month: str = file.parts[-2]
    year: str = file.parts[-3]
    pdf_code: str = file.parts[-4]
    country_code: str = file.parts[-5]
    day_00: str = pdf_name[-6:-4]

    return f"{paths.PARENT_DIRECTORY_WSL}/{country_code}/{pdf_code}/{year}/{month}/{day_00}"

def main() -> None:
    global THREADS
    THREADS.extend(get_pdf_threads("USA"))
    THREADS.extend(get_pdf_threads("CAN"))
    
    for thread in THREADS:
        thread.start()

    for thread in THREADS:
        thread.join()

    print("All threads completed")

# main()