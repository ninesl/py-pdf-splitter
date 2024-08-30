import threading, os
import fitz #PyMuPDF
from pathlib import Path, PosixPath
from paths import PARENT_DIRECTORY_WSL, END_PAGE_TEXT, END_PAGE_LENGTH, get_pdf_path, get_paths_txt_file
import sys

THREADS: list[threading.Thread] = []

def set_pdf_threads(country_code: str, pdf_code:str="*", year: str = "*", month: str = "*", day: str = "") -> list[threading.Thread]:
    path_dir: PosixPath = Path.cwd() / f"{PARENT_DIRECTORY_WSL}" / f"{country_code}" / f"{pdf_code}" / f"{year}" / f"{month}"
    
    country_list: list[PosixPath] = [country for country in path_dir.iterdir()]
    # threads: list[threading.Thread] = []
    threads: list = []

    # for path in country_list:
    #     print(path.parts[-1])
    
    for file in country_list:
        if(file.is_file()):
            if(day in file.name):
                print(file.name)
                # threads.append(threading.Thread(target=pdf_to_txt, args=(file)))
                pdf_to_txt(file)
        # threads.append(threading.Thread(target=thread_find_pdfs, args=(filepath, pdf_code, year, month, day)))

    return threads

def pdf_to_txt(file: PosixPath) -> None:
    try:
        # if write_parse_pdf(file):
        #     print(f".{file.name}", end="")
        write_parse_pdf(file)
    except:
        print("\nError parsing.", file)

def write_parse_pdf(file: PosixPath) -> bool:
    txt_file_dir: str = find_txt_file_dir(file)

    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]

    dir_exists: bool = os.path.isdir(txt_file_dir)

    pdf_num: int = 1
    pdf_page_count: int = None
    with fitz.open(file, filetype="pdf") as pdf_file:
        text: str = "" # concat to this. txt files could be parsed from more than one pdf page

        # check if any txt files were missed. hopefully doesn't bork everything
        # will rewrite over pdfs's txt files that are built from >1 pdf page
        # added -2. Some pdfs have 10 pages but 8 txt files to gen, and so on. -2 seems safe for a cutoff
        pdf_page_count = pdf_file.page_count
        if dir_exists:
            file_count: int = len([name for name in os.listdir(txt_file_dir) if os.path.isfile(os.path.join(txt_file_dir, name))])
            if file_count > pdf_page_count - 3: # roughly accounts for all txt files accounted for.
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
    print(txt_file_dir, pdf_num - 1, pdf_page_count)
    if pdf_page_count <= 2:
        print("*****************", file.name)
    return True

PDF_CODE_COUNT: int = 0
def thread_find_pdfs(filepath: PosixPath, pdf_code:str="*", year: str = "*", month: str = "*", day: str = "") -> None:
    print(pdf_code, year, month, day)
    global PDF_CODE_COUNT
    PDF_CODE_COUNT += 1
    print("Looking through", filepath.name, PDF_CODE_COUNT, "pdf_codes found")
    
    # files_found: list[PosixPath] = list(filepath.rglob("*.pdf"))
    
    files_found: list[PosixPath] = []
    for file in filepath.glob(f"**/{pdf_code}/{year}/{month}"):
        print("\n***************", file, "\n")
        files_found.append(file)
        # for pdf_file in folder.glob("**/*.pdf"):
        #     files_found.append(pdf_file)

    # for file in files_found:
    #     print("\n", file, "\n")
        # pdf_to_txt(file)

    PDF_CODE_COUNT -= 1
    print("|", filepath.name, PDF_CODE_COUNT, "pdf_codes to go")

def find_txt_file_dir(file: PosixPath) -> str:
    pdf_filename: str = file.parts[-1]
    pdf_name: str = pdf_filename.split('.')[0]
    month: str = file.parts[-2]
    year: str = file.parts[-3]
    pdf_code: str = file.parts[-4]
    country_code: str = file.parts[-5]
    day_00: str = pdf_name[-6:-4]
    
    txt_dir:str = f"{PARENT_DIRECTORY_WSL}/{country_code}/{pdf_code}/{year}/{month}/{day_00}"
    return txt_dir

def set_threads_years(start_year:int, end_year:int) -> None:
    for year in range(start_year, end_year + 1):
        pass
        # THREADS.extend(set_pdf_threads("USA", str(year)))
        # THREADS.extend(set_pdf_threads("CAN", str(year)))
        # THREADS.extend(set_pdf_threads("PR",  str(year)))

def main() -> None:
    global THREADS
    if len(sys.argv) != 4:
        print("Invalid arguments. Please have args be\npy script.py DAY MONTH YEAR")
        return
    
    day: str = sys.argv[1]
    month: str = sys.argv[2]
    year: str = sys.argv[3]
    txt_file: str = get_paths_txt_file(day, month, year)
    print(txt_file)
    
    with open(txt_file) as file:
        for line in file.readlines():
            path_parts = line.strip().split("/")
            country_code = path_parts[-5]
            pdf_code = path_parts[-4]
            print(f"|{country_code}-{pdf_code}| ", end="")
            THREADS.extend(set_pdf_threads(country_code=country_code, pdf_code=pdf_code, 
                                           year=year, month=month.lstrip("0"), day=day.lstrip("0")))
    print()

    # get_paths_from_txt_file()

    for thread in THREADS:
        thread.start()

    for thread in THREADS:
        thread.join()

main()


# target specific path. Used in auto scrape

# paths.get_pdf_path(ctry_code:str, pdf_code:str, month:str, day:str, year:str) -> PosixPath:
# path = get_pdf_path("COUNTRY", "PDF_CODE", "MM", "DD", "YYYY")
# pdf_to_txt(path)

# # pdf_to_txt(path)