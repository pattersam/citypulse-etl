#!/bin/bash

pandoc report.md \
    -f gfm \
    --include-in-header tex-header.tex \
    -V linkcolor:blue \
    -V geometry:a4paper \
    -V geometry:margin=2cm \
    -V mainfont="Noto Serif" \
    -V monofont="Noto Mono" \
    --highlight-style pygments.theme \
    --pdf-engine=xelatex \
    -o report.pdf
