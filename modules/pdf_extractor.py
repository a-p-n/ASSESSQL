import pdfplumber
import re
from typing import Dict, List, Any

class PDFExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.grouped_data = {} 

    def process(self) -> Dict[str, Any]:
        all_elements = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                all_elements.extend(self._extract_elements_from_page(page, page_num))

        sorted_stream = sorted(all_elements, key=lambda x: (x['page'], x['top']))

        self._process_stream(sorted_stream)

        return self.grouped_data

    def _extract_elements_from_page(self, page, page_num):
        elements = []
        
        tables = page.find_tables()
        table_bboxes = []

        for table in tables:
            bbox = table.bbox
            table_bboxes.append((bbox[1], bbox[3]))
            
            data = table.extract()
            if data:
                elements.append({
                    'type': 'table',
                    'page': page_num,
                    'top': bbox[1],
                    'data': data
                })

        words = page.extract_words(keep_blank_chars=True)
        current_line = []
        last_top = 0
        
        for w in words:
            if not current_line:
                current_line.append(w)
                last_top = w['top']
            else:
                if abs(w['top'] - last_top) < 5:
                    current_line.append(w)
                else:
                    self._flush_text_line(current_line, elements, page_num, table_bboxes)
                    current_line = [w]
                    last_top = w['top']
        
        if current_line:
            self._flush_text_line(current_line, elements, page_num, table_bboxes)

        return elements

    def _flush_text_line(self, word_list, elements_list, page_num, table_bboxes):
        if not word_list: return

        avg_top = sum(w['top'] for w in word_list) / len(word_list)
        
        for t_top, t_bottom in table_bboxes:
            if t_top <= avg_top <= t_bottom:
                return 

        text_content = " ".join([w['text'] for w in word_list]).strip()
        if text_content:
            elements_list.append({
                'type': 'text',
                'page': page_num,
                'top': avg_top,
                'text': text_content
            })

    def _process_stream(self, stream):
        current_qid = "General"
        self.grouped_data[current_qid] = {"instruction": "", "tables": {}, "queries": []}
        
        last_text_line = "Unknown_Table"
        current_section = "INSTRUCTION"

        question_header_re = re.compile(r"^Question\s+(\d+)", re.IGNORECASE)
        query_re = re.compile(r"^\d+[\.)]")
        ignore_re = re.compile(r"^(Assignment|Page|Lab|Sheet\.)", re.IGNORECASE)

        for element in stream:
            if element['type'] == 'text':
                text = element['text'].strip()
                if not text: continue
                
                q_match = question_header_re.search(text)
                if q_match:
                    current_qid = f"Question {q_match.group(1)}"
                    if current_qid not in self.grouped_data:
                        self.grouped_data[current_qid] = {
                            "instruction": "", 
                            "tables": {}, 
                            "queries": []
                        }
                    current_section = "INSTRUCTION"
                    continue 

                if "QUERIES" in text.upper() and len(text.split()) < 4:
                    current_section = "QUERIES"
                    continue
                    
                if ignore_re.match(text):
                    continue

                if current_section == "INSTRUCTION":
                    if self.grouped_data[current_qid]["instruction"]:
                        self.grouped_data[current_qid]["instruction"] += " " + text
                    else:
                        self.grouped_data[current_qid]["instruction"] = text
                    
                    if len(text.split()) < 6:
                        last_text_line = text

                elif current_section == "QUERIES":
                    if query_re.match(text):
                        self.grouped_data[current_qid]["queries"].append(text)
                    elif self.grouped_data[current_qid]["queries"]:
                        self.grouped_data[current_qid]["queries"][-1] += " " + text

                else:
                    if len(text.split()) < 6 and not self._is_table_header(text):
                        last_text_line = text

            elif element['type'] == 'table':
                current_section = "SCHEMA"
                
                table_name = self._clean_table_name(last_text_line)
                
                instr = self.grouped_data[current_qid]["instruction"]
                if instr.endswith(last_text_line):
                    self.grouped_data[current_qid]["instruction"] = instr[:-len(last_text_line)].strip()
                
                parsed = self._parse_table_schema(table_name, element['data'])
                if parsed:
                    self.grouped_data[current_qid]["tables"].update(parsed)

    def _clean_table_name(self, text: str) -> str:
        clean = text.replace("SQL.", "").replace(":", "").strip()
        parts = clean.split()
        if parts:
            return parts[-1].upper()
        return "UNKNOWN_TABLE"

    def _is_table_header(self, text: str) -> bool:
        lower = text.lower()
        return "column name" in lower or "datatype" in lower or "constraint" in lower

    def _parse_table_schema(self, table_name: str, data: List[List[str]]) -> Dict[str, Any]:
        if not data: return {}
        
        headers = [str(h).lower() for h in data[0] if h]
        if not ("column name" in headers or "datatype" in headers or "constraint" in headers):
            return {}
            
        columns = {}
        for row in data[1:]:
            clean_row = [str(cell).strip() if cell else "" for cell in row]
            
            if len(clean_row) >= 2:
                col_name = clean_row[0].lower()
                col_type = clean_row[1].lower()
                col_const = clean_row[2] if len(clean_row) > 2 else ""
                
                if col_name:
                    columns[col_name] = {
                        "type": col_type,
                        "constraint": col_const
                    }
        
        return {table_name: columns}