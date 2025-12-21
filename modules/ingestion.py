import os
import re
import json
import unicodedata
import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
from PIL import Image, ImageEnhance, ImageFilter

class IngestionModule:
    def __init__(self):
        # You can add tesseract configuration paths here if needed
        pass

    def process_file(self, file_path):
        """
        Main entry point: Takes a PDF path, extracts text, and returns structured JSON.
        """
        print(f"[INFO] Processing file: {file_path}")
        raw_text = self._extract_text_from_pdf(file_path)
        
        if not raw_text:
            print("[ERROR] Failed to extract text.")
            return []

        structured_data = self._parse_lab_manual_to_json(raw_text)
        print(f"[INFO] Successfully extracted {len(structured_data)} lab scenarios.")
        return structured_data

    # --- INTERNAL METHODS: OCR & EXTRACTION (From ocr.py) ---
    
    def _ocr_image(self, image: Image.Image):
        try:
            image = image.convert('L')
            image = image.filter(ImageFilter.SHARPEN)
            text = pytesseract.image_to_string(image)
            return text.strip()
        except Exception as e:
            print(f"[WARNING] OCR Error: {e}")
            return ""

    def _extract_text_from_pdf(self, pdf_path: str):
        if not os.path.exists(pdf_path):
            print(f"[ERROR] File not found: {pdf_path}")
            return None

        full_text = ""

        # 1. Try Digital Extraction (Fast)
        try:
            doc = fitz.open(pdf_path)
            for page in doc:
                full_text += page.get_text()
            doc.close()
            
            if full_text and len(full_text.strip()) > 50:
                return full_text.strip()
        except Exception:
            pass # Fallback to OCR

        # 2. Fallback to Optical OCR (Slow but robust)
        print("[INFO] Digital extraction failed. Switching to OCR...")
        try:
            images = convert_from_path(pdf_path)
            full_text = ""
            for i, img in enumerate(images):
                full_text += self._ocr_image(img) + "\n\n"
            return full_text.strip()
        except Exception as e:
            print(f"[ERROR] PDF Processing failed: {e}")
            return None

    # --- INTERNAL METHODS: PARSING (From ocr_to_json.py) ---

    def _sanitize_text(self, text):
        if not text: return text
        mapping = {
            '\u2018': "'", '\u2019': "'", '\u201c': '"', '\u201d': '"',
            '\u2013': '-', '\u2014': '-', '\u00a0': ' ',
        }
        for bad, good in mapping.items():
            text = text.replace(bad, good)
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
        return text.strip()

    def _parse_lab_manual_to_json(self, raw_text):
        """
        Parses raw text into the schema structure:
        [{'question_id': 1, 'tables': [...], 'queries': ['Find students...']}]
        """
        raw_text = self._sanitize_text(raw_text)
        clean_text = raw_text.replace('","', '\n').replace('", "', '\n').replace('"', '')
        
        # Split by "Question X" blocks
        q_chunks = re.split(r'(?=Question \d+)', clean_text)
        datatypes = ['Varchar', 'Integer', 'char', 'char(10)', 'Numeric', 'Date', 'varchar', 'integer', 'Char']
        
        results = []
        
        for chunk in q_chunks:
            if not chunk.strip(): continue
            
            # Extract ID
            q_match = re.search(r'Question (\d+)', chunk)
            q_id = int(q_match.group(1)) if q_match else 0
            
            # Split Schema vs Queries
            sections = re.split(r'\n\s*(?:QUERIES|Queries)\s*\n', chunk, flags=re.IGNORECASE)
            table_section = sections[0]
            query_section = sections[1] if len(sections) > 1 else ""
            
            # Extract Queries
            queries = []
            q_matches = re.findall(r'(\d+\.\s+.*?)(?=\n\s*\d+\.|\Z)', query_section, re.DOTALL)
            for q in q_matches:
                # Clean up query text (remove numbers like "1. ")
                clean_q = re.sub(r'^\d+\.\s*', '', q.strip().replace('\n', ' '))
                queries.append(clean_q)
                
            # Extract Tables (Using your specific logic)
            tables = []
            lines = [l.strip() for l in table_section.split('\n') if l.strip()]
            
            i = 0
            while i < len(lines):
                if lines[i] == "Column name":
                    t_name = "Unknown"
                    t_constraints = []
                    
                    # Backtrack for table name
                    j = i - 1
                    while j >= 0:
                        prev = lines[j]
                        if "following table" in prev.lower():
                            j -= 1; continue
                        if any(w in prev.lower() for w in ["primary key", "combination", "refers to"]):
                            t_constraints.append(prev)
                            j -= 1; continue
                        t_name = prev
                        break
                    
                    i += 3 # Skip header lines
                    
                    # Parse Columns
                    col_lines = []
                    temp_i = i
                    while temp_i < len(lines):
                        if lines[temp_i] == "Column name": break
                        col_lines.append(lines[temp_i])
                        temp_i += 1
                    
                    parsed_cols = []
                    # Identify datatypes to split columns
                    dt_indices = [idx for idx, line in enumerate(col_lines) 
                                  if any(line.lower().startswith(dt.lower()) for dt in datatypes)]
                    
                    for k in range(len(dt_indices)):
                        dt_idx = dt_indices[k]
                        name = col_lines[dt_idx - 1] if dt_idx > 0 else "Unknown"
                        dtype = col_lines[dt_idx]
                        
                        end_idx = dt_indices[k+1] - 1 if k+1 < len(dt_indices) else len(col_lines)
                        const_block = " ".join(col_lines[dt_idx + 1 : end_idx])
                        const_block = re.split(r'\s(?=[A-Z][a-z]+)', const_block)[0].strip()
                        
                        parsed_cols.append({
                            "column_name": name,
                            "datatype": dtype,
                            "constraints": const_block if const_block else None
                        })
                    
                    tables.append({
                        "table_name": t_name,
                        "table_constraints": t_constraints[::-1],
                        "columns": parsed_cols
                    })
                    i = temp_i
                    continue
                i += 1
                
            results.append({"question_id": q_id, "tables": tables, "queries": queries})
            
        return results