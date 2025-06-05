
.PHONY: run
run:
	uv run pdf_parse.py $(DAY) $(MONTH) $(YEAR)