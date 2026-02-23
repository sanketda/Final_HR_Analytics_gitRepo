import os
import ast
import json

# === Constants ===
DELIMITER = "\n<|CHUNK_SPLIT|>\n"  # Unique delimiter for Langflow splitter

def extract_chunks_from_python_code(code: str, file_name: str):
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    chunks = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start_line = node.lineno - 1
            end_line = max(
                [n.lineno for n in ast.walk(node) if hasattr(n, "lineno")],
                default=start_line
            )
            lines = code.splitlines()[start_line:end_line]
            chunk = "\n".join(lines).strip()
            if chunk:
                
                chunks.append(chunk)
    return chunks

def extract_chunks_from_ipynb(file_path):
    file_name = os.path.basename(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            notebook = json.load(f)
        except json.JSONDecodeError:
            print(f"❌ Skipping corrupt notebook: {file_path}")
            return []

    code_cells = [
        cell["source"] for cell in notebook.get("cells", [])
        if cell.get("cell_type") == "code"
    ]

    all_chunks = []
    for cell in code_cells:
        code = "".join(cell)
        chunks = extract_chunks_from_python_code(code, file_name)
        all_chunks.extend(chunks)

    return all_chunks

def process_codebase_to_chunks_txt(codebase_path, output_txt_path):
    all_chunks = []
    file_count = 0

    for file in os.listdir(codebase_path):
        file_path = os.path.join(codebase_path, file)

        if os.path.isfile(file_path) and file.endswith(".py"):
            print(f"🔍 Processing Python file: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    code = f.read()
                    chunks = extract_chunks_from_python_code(code, file)
                    print(f"   ↳ Found {len(chunks)} chunks.")
                    all_chunks.extend(chunks)
                    file_count += 1
            except Exception as e:
                print(f"❌ Failed to process {file_path}: {e}")

        elif os.path.isfile(file_path) and file.endswith(".ipynb"):
            print(f"🔍 Processing Notebook: {file_path}")
            try:
                chunks = extract_chunks_from_ipynb(file_path)
                print(f"   ↳ Found {len(chunks)} chunks.")
                all_chunks.extend(chunks)
                file_count += 1
            except Exception as e:
                print(f"❌ Failed to process {file_path}: {e}")

    print(f"💾 Writing {len(all_chunks)} chunks to TXT using delimiter.")
    with open(output_txt_path, "w", encoding="utf-8") as txtfile:
        txtfile.write(DELIMITER.join(all_chunks))

    print(f"✅ Done. Processed {file_count} files. Total chunks: {len(all_chunks)}")

# ==== USAGE ====
codebase_path = "E:/Academics/TY/Shyena Tech Yarns/backend"
output_txt_path = "E:/Academics/TY/Shyena Tech Yarns/Langflow/knowledge base/code_chunks.txt"
process_codebase_to_chunks_txt(codebase_path, output_txt_path)
