# Channel document archiver

Run with the Python interpreter configured in `company-document-archive.json`.
Install `requirements.txt` into that interpreter. Build the optional macOS OCR
helper with `swiftc ocr.swift -o ocr` in this directory.

`ingest.py --request PATH` retains originals, provenance, extracted text and notes.
`reprocess.py --vault PATH` retries missing text without replacing human notes.
OCR output is always marked for review. Run `python3 -m unittest test_ingest.py`.

Channel archive receipts confirm persistence, not agent completion or human review.
Lark HTTP failures expose only HTTP status, resource type and numeric business code.
A fresh real attachment is still required to verify provider download permissions.
