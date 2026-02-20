import json
import os
from modules.pdf_extractor import PDFExtractor

def run_test():
    pdf_file = "lab1.pdf"
    
    if not os.path.exists(pdf_file):
        print(f"Error: {pdf_file} not found.")
        return

    extractor = PDFExtractor(pdf_file)
    grouped_data = extractor.process()
    
    print(f"\nSuccessfully extracted {len(grouped_data)} Questions.")

    for question_id, content in grouped_data.items():
        if question_id.upper() == "GENERAL":
            continue
        print(f"\n{'='*20} {question_id.upper()} {'='*20}")

        print("\n[SCHEMA]")
        if content['tables']:
            print(json.dumps(content['tables'], indent=2))
        else:
            print("  (No tables found for this section)")
            
        print("\n[QUERIES]")
        if content['queries']:
            for q in content['queries']:
                print(f"  {q}")
        else:
            print("  (No queries found for this section)")

if __name__ == "__main__":
    run_test()