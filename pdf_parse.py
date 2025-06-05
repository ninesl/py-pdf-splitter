import threading, os
import fitz #pip install PyMuPDF
from pathlib import Path
from paths import get_paths_txt_file, get_paths_pdf_path, get_pdf_file_name, get_paths_txt_folder, END_PAGE_TEXT, END_PAGE_LENGTH
import sys


def pdf_to_txt(file: Path, write_folder: Path) -> None:
    try:
        if not write_folder.exists():
            os.makedirs(write_folder, exist_ok=True)

        text: str = ""

        file_name: str = file.stem 

        pdf_num: int = 1
        with fitz.open(file, filetype="pdf") as doc: # type: ignore
            text = ""
            for page in doc:
                text += page.get_text()

                # print(f"{END_PAGE_TEXT} {text[-END_PAGE_LENGTH:]} {END_PAGE_TEXT in text[-END_PAGE_LENGTH:]}")
                
                if(END_PAGE_TEXT in text[-END_PAGE_LENGTH:]): # should be faster than searching whole page

                    txt_filename: str = f"{file_name}_{pdf_num}.txt"

                    write_folder.mkdir(parents=True, exist_ok=True)

                    txt_file_full_path: Path = write_folder / txt_filename

                    if not txt_file_full_path.exists():
                        with open(txt_file_full_path, "w", encoding="utf-8") as txt_file:
                            txt_file.write(text)
                            print(".", end="")
                    
                    pdf_num += 1
                    text = ""

        print(f"parsed: {file.name}")
    except:
        print("\nError parsing.", file)

def exec() -> None:
    THREADS: list[threading.Thread] = []

    if len(sys.argv) != 4:
        print("Invalid arguments. Please have args be : DAY MONTH YEAR")
        return

    #hacky way to sanitzie params, cnanot have 2025/05/01 etc, must be 2025/5/01 
    day: str = sys.argv[1]
    if day[0] == "0":
        day = day[1:]

    month: str = sys.argv[2]
    if month[0] == "0":
        month: str = month[1:]

    year: str = sys.argv[3]
    txt_file_path: Path = get_paths_txt_file(day, month, year)
    
    # print(f"{txt_file_path}")
    
    # CTRY_PDF :
    # USA_ABC
    # CAN_XYZ

    with open(txt_file_path) as file:
        for line in file.readlines():
            parts: list[str] = line.strip().split("_")
            country_code: str = parts[0]
            pdf_code: str = parts[1]
            pdf_file_path: Path = get_paths_pdf_path(month, year, country_code, pdf_code)
            pdf_txt_folder: Path = get_paths_txt_folder(pdf_file_path, day)
            pdf_file_name: str = get_pdf_file_name(day, month, year, pdf_code)

            pdf_file: Path = pdf_file_path / pdf_file_name

            print(f"{country_code}_{pdf_code} ", end="")
            print()

            thread: threading.Thread = threading.Thread(target=pdf_to_txt, args=(pdf_file, pdf_txt_folder))
            THREADS.append(thread)
            # THREADS.extend(set_pdf_thread(country_code=country_code, pdf_code=pdf_code, 
                                        #    year=year, month=month.lstrip("0"), day=day.lstrip("0")))

    for thread in THREADS:
        thread.start()

    for thread in THREADS:
        thread.join()
    print("done")

if __name__ == "__main__":
    exec()

# print("hello world")

# target specific path. Used in auto scrape

# paths.get_pdf_path(ctry_code:str, pdf_code:str, month:str, day:str, year:str) -> PosixPath:
# path = get_pdf_path("COUNTRY", "PDF_CODE", "MM", "DD", "YYYY")
# pdf_to_txt(path)

# # pdf_to_txt(path)