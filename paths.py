from pathlib import Path

home = Path.home()
#TODO: needs to be a .env variable
PARENT_DIRECTORY: Path = home / "dev" / "horse_moneyball" / "tracks"

END_PAGE_TEXT: str = "Equibase Company LLC. All Rights Reserved."
END_PAGE_LENGTH: int = len(END_PAGE_TEXT) + 1

def get_paths_txt_file(day:str, month:str, year:str) -> Path:
    d = day.zfill(2)
    txt_path: Path = PARENT_DIRECTORY / "txts" / year / month / f"{d}.txt"
    return txt_path

def get_paths_pdf_path(month:str, year:str, country_code:str, pdf_code:str) -> Path:
    pdf_path: Path = PARENT_DIRECTORY / "pdfs" / country_code / pdf_code / year / month
    return pdf_path

def get_paths_txt_folder(path: Path, day: str) -> Path:
    d = day.zfill(2)
    txt_folder_path: Path = path / d
    return txt_folder_path 

def get_pdf_file_name(day: str, month:str, year:str, pdf_code:str) -> str:
    d = day.zfill(2)
    m = month.zfill(2)
    file_name: str = f"{pdf_code}_{m}{d}{year}.pdf"
    return file_name
