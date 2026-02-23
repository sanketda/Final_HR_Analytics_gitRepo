import pandas as pd
import os
import json
import re
from langchain_groq import ChatGroq
from dotenv import load_dotenv

# =================== LLM Setup ===================
load_dotenv()  
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

llm = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0.1,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

# =================== LLM Prompt Helper ===================
def analyze_with_llm(rows: str) -> dict:
    prompt_template = """
You are a professional Data Analyst with strong knowledge in MLOps, data quality validation, and cleansing strategies.

You will be given 20 rows of tabular data in the format:
Column Name: <Value>

Your task is to:
1. Infer the correct data type for each column. Choose from: string, integer, float, boolean, date, email, phone, url, latlong.
2. Generate column-specific validation rules (e.g., length limits, corruption checks).
3. Suggest realistic and tailored fix strategies for common issues.

### Output Format:
Return a **valid JSON object only** (no markdown, no explanation), structured as:

{
  "columns": {
    "ColumnName1": "data_type",
    ...
  },
  "fix_strategies": {
    "ColumnName1": {
      "required_missing": "...",
      "invalid_<type>": "...",  // e.g., invalid_phone, invalid_url
      "min_length_violation": "...",
      "max_length_violation": "...",
      "pattern_violation": "...",
      "corrupt_characters": "...",
      "default": "..."
    },
    ...
    "default": {
      "required_missing": "N/A",
      ...
    }
  }
}

### Guidelines:
- Tailor strategies to semantics and data types — do NOT use the same fallback for all columns.
- Include **at least 7 distinct keys per column** in `fix_strategies`.
- Use clear, practical replacements:
  - Name: "Unknown Company"
  - Address: "Address Not Available"
  - Phone: "Phone Not Available"
  - URL: "Missing URL"
  - LatLong: "0.000,0.000"
  - Date: "2024-01-01"
- Special constraints for phone numbers:
  - Must match regex: `^\\+?[1-9][0-9]{9,14}$`
  - Must not contain **any alphabetic characters** (e.g., `phone123!` is invalid).
- Email must match: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$`
- LatLong format: `"latitude,longitude"` where both are valid floats within -90..90 and -180..180.
- URLs should begin with http:// or https://

Return nothing except the JSON object.

Here are the rows:
"""
    prompt = prompt_template + "\n" + rows
    response = llm.invoke(prompt)

    try:
        match = re.search(r"\{[\s\S]*\}", response.content)
        if match:
            json_str = match.group()
            return json.loads(json_str)
        else:
            raise ValueError("No valid JSON found.")
    except json.JSONDecodeError as e:
        print("❌ JSON parsing error:", e)
        print("🔴 Raw LLM Response:", response.content)
        return {}

# =================== Main Function ===================
def generate_schema_from_dataset(csv_path: str, sample_size: int = 20, output_path: str = "generated_schema.json"):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    sample_df = df.head(sample_size)

    # Format sample rows
    sample_rows = ""
    for i in range(len(sample_df)):
        for c in df.columns:
            sample_rows += f"{c}: {sample_df.iloc[i][c]}\n"
        sample_rows += "\n"

    llm_output = analyze_with_llm(sample_rows)
    # print("+++++++++++++++++++++++++LLM Output++++++++++++++++++++++++++++++++++++++")
    # print(json.dumps(llm_output, indent=2))

    # ========== USER INPUT FOR METADATA ==============
    dataset_info = {
        "name": "Google Maps",
        "description": "Google Maps",
        "workspace_path": "/FileStore/corrupted_scraped_data.csv",
        "local_path": "temp_corrupted_scraped_data.csv",
        "version": "1.0"
    }

    # output_config = {
    #     "databricks_path": "/Workspace/Users/prachi.navghare@shyenatechyarns.in/prod_dem_ana/Vedant/cleaned_data.csv",
    #     "local_output_path": "cleaned_data.csv",
    #     "backup_original": False,
    #     "generate_report": True,
    #     "report_path": "data_quality_report.json"
    # }

    # ========== Enhance Validation Rules ==============
    column_types = llm_output.get("columns", {})
    fix_strategies = llm_output.get("fix_strategies", {})

    # Dynamically calculate max_length for each column
    validation_rules = {}
    for col, dtype in column_types.items():
        max_len = sample_df[col].astype(str).apply(len).max()
        validation_rules[col] = {
            "data_type": dtype,
            "required": True,
            "check_corruption": True,
            "min_length": 3,
            "max_length": int(max_len)
        }

    # Custom validation rules for phone, URL, lat/long
    custom_validation_rules = {
        "phone_formats": {
            "patterns": [r"^\+?[1-9][0-9]{9,14}$"]
        },
        "url_validation": {
            "accepted_domains": [".com", ".in", ".org", ".net"]
        },
        "coordinate_validation": {
            "latitude_range": [-90, 90],
            "longitude_range": [-180, 180]
        }
    }

    # ========== Assemble Schema ==============
    full_schema = {
        "dataset_info": dataset_info,
        "validation_rules": validation_rules,
        "fix_strategies": fix_strategies,
        "custom_validation_rules": custom_validation_rules
    }

    # ========== Save Schema ==============
    if output_path:
        with open(output_path, "w") as f:
            json.dump(full_schema, f, indent=2)
        print(f"✅ Schema saved to {output_path}")
    else:
        print("📦 Schema ready (not saved locally):")
        print(json.dumps(full_schema, indent=2))

# ========== Example usage ==========
if __name__ == "__main__":
    generate_schema_from_dataset(
        csv_path=r"",
        sample_size=20,
        output_path="generated_schema.json"
    )
