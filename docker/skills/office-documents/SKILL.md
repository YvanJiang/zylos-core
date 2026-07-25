---
name: office-documents
description: Read and edit local PowerPoint, Word, and Excel files with the Python libraries preinstalled in the Core image.
---

# Office Documents

Use the image's Python environment for local Office files:

- PowerPoint: `python-pptx` (`from pptx import Presentation`)
- Word: `python-docx` (`from docx import Document`)
- Excel: `openpyxl` (`from openpyxl import load_workbook, Workbook`)

Always preserve the original file unless the user explicitly asks to overwrite it. Write edited
documents to a new path first, reopen the result with the same library, and verify the expected
slides, paragraphs, tables, worksheets, formulas, and cell values.

For PowerPoint, inspect slide shapes and text runs before editing. For Word, preserve section,
paragraph, run, and table structure. For Excel, load with `data_only=False` when formulas must be
preserved and avoid changing workbook calculation behavior unless requested.
