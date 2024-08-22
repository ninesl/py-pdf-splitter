import threading, os
import fitz #PyMuPDF
from pathlib import Path, PosixPath
from paths import PARENT_DIRECTORY_WSL, END_PAGE_TEXT, END_PAGE_LENGTH, get_pdf_path

THREADS: list[threading.Thread] = []

def get_pdf_threads(country_code: str, year:str="*") -> list[threading.Thread]:
    print(f"{country_code = }")
    # print("Hello world")
    path_dir: PosixPath = Path.cwd() / f"{PARENT_DIRECTORY_WSL}" / f"{country_code}"
    
    country_list: list[PosixPath] = [country for country in path_dir.iterdir()]
    threads: list[threading.Thread] = []

    # for path in country_list:
    #     print(path.parts[-1])
    
    for filepath in country_list:
        threads.append(threading.Thread(target=thread_find_pdfs, args=(filepath, year)))

    return threads

PDF_CODE_COUNT: int = 0
def thread_find_pdfs(filepath: PosixPath, year: str = "*") -> None:
    global PDF_CODE_COUNT
    PDF_CODE_COUNT += 1
    print("Looking through", filepath.name, PDF_CODE_COUNT, "pdf_codes found")
    
    # files_found: list[PosixPath] = list(filepath.rglob("*.pdf"))
    
    files_found: list[PosixPath] = []
    for folder in filepath.glob(f"**/{year}"):
        for pdf_file in folder.glob("**/*.pdf"):
            files_found.append(pdf_file)

    for file in files_found:
        # pdf_to_txt(file)
        print(file)
    PDF_CODE_COUNT -= 1
    print("|", filepath.name, PDF_CODE_COUNT, "pdf_codes to go")

def pdf_to_txt(file: PosixPath) -> None:
    try:
        if write_parse_pdf(file):
            print(f".", end="")
    except:
        print("\nError parsing.", file)

def find_txt_file_dir(file: PosixPath) -> str:
    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]
    month: str = file.parts[-2]
    year: str = file.parts[-3]
    pdf_code: str = file.parts[-4]
    country_code: str = file.parts[-5]
    day_00: str = pdf_name[-6:-4]
    
    txt_dir:str = f"{PARENT_DIRECTORY_WSL}/{country_code}/{pdf_code}/{year}/{month}/{day_00}"
    print(txt_dir)
    return txt_dir

def write_parse_pdf(file: PosixPath) -> bool:
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
            if file_count < pdf_file.page_count - 2: # roughly accounts for all txt files accounted for.
                return False
        
        os.makedirs(txt_file_dir, exist_ok=True)

        for page_num in range(pdf_file.page_count):
            page_text: str = pdf_file.load_page(page_num).get_text("text")
            text += page_text
            
            if(END_PAGE_TEXT in page_text[-END_PAGE_LENGTH:]): # should be faster than searching whole page
                txt_filename: str = f"{pdf_name}_{pdf_num}.txt"
                with open(f"{txt_file_dir}/{txt_filename}", "w", encoding="utf-8") as txt_file:
                    txt_file.write(text)
                pdf_num += 1
                text = ""

        # Save any remaining text in case the document doesn't end with the marker
        # if text.strip():
        #     print(f"Writing {pdf_name}_extra")
        #     with open(f"{txt_file_dir}/{pdf_name}_extra.txt", "w", encoding="utf-8") as extra_txt_file:
        #         extra_txt_file.write(text)

    return True

def main() -> None:
    global THREADS
    # THREADS.extend(get_pdf_threads("USA", "2023"))
    # THREADS.extend(get_pdf_threads("CAN", "2023"))
    
    for thread in THREADS:
        thread.start()

    for thread in THREADS:
        thread.join()

    print("All threads completed")

main()

# target specific path. Used in auto scrape

# paths.get_pdf_path(ctry_code:str, pdf_code:str, month:str, day:str, year:str) -> PosixPath:
# path = get_pdf_path("COUNTRY", "PDF_CODE", "MM", "DD", "YYYY")
# pdf_to_txt(path)

# # path = get_pdf_path("USA", "IND", "08", "23", "2014")
# # pdf_to_txt(path)