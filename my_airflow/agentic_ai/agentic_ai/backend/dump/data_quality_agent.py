import os
import re
import json
import base64
import pandas as pd
import requests
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from typing import TypedDict, List, Any, Optional, Dict
from datetime import datetime
from dotenv import load_dotenv

from schema_generator import generate_schema_from_dataset

# ========= CONFIG ==========


load_dotenv()  

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

EMAIL_CONFIG = {
    "sender_email": os.getenv("EMAIL_SENDER"),
    "app_password": os.getenv("EMAIL_PASSWORD"),
    "recipient_list": [os.getenv("EMAIL_RECIPIENT")],
    "smtp_server": os.getenv("SMTP_SERVER"),
    "smtp_port": int(os.getenv("SMTP_PORT"))
}

# ========= SCHEMA CONFIGURATION ==========
class DataQualityConfig:
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """Initialize with either a config file path or config dictionary"""
        if config_path:
            with open(config_path, 'r') as f:
                self.config = json.load(f) #dict
        elif config_dict:
            self.config = config_dict
        else:
            raise ValueError("Either config_path or config_dict must be provided")
        
        self.dataset_info = self.config.get("dataset_info", {})
        self.validation_rules = self.config.get("validation_rules", {})
        self.fix_strategies = self.config.get("fix_strategies", {})
        self.output_config = self.config.get("output_config", {})
        self.custom_validation_rules = self.config.get("custom_validation_rules", {})
        self.data_enhancement = self.config.get("data_enhancement", {})

    def get_column_rules(self, column_name: str) -> dict:
        """Get validation rules for a specific column"""
        return self.validation_rules.get(column_name, {})

    def get_fix_strategy(self, column_name: str, issue_type: str) -> str:
        """Get fix strategy for a specific column and issue type"""
        column_fixes = self.fix_strategies.get(column_name, {})
        return column_fixes.get(issue_type, self.fix_strategies.get("default", {}).get(issue_type, "N/A"))

    def get_configured_columns(self) -> List[str]:
        """Get list of columns configured in validation rules"""
        return list(self.validation_rules.keys())

    def is_column_configured(self, column_name: str) -> bool:
        """Check if a column is configured in validation rules"""
        return column_name in self.validation_rules


# ========= STATE DEFINITION ==========
class AgentState(TypedDict):
    df: Optional[pd.DataFrame]
    issues: Optional[List[dict]]
    df_cleaned: Optional[pd.DataFrame]
    fix_log: Optional[pd.DataFrame]
    fix_json: Optional[dict]
    upload_status: Optional[int]
    upload_response: Optional[Any]
    validation: Optional[str]
    config: Optional[DataQualityConfig]
    original_filename: Optional[str] 

# ========= UTILITY FUNCTIONS ==========


def download_csv_from_dbfs(dbfs_path, local_path):
    """Download CSV from DBFS to local machine"""
    print(f"⬇  Attempting to download: {dbfs_path}")
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/read"
    params = {"path": dbfs_path}
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    # Make the request to get the file content
    res = requests.get(url, headers=headers, params=params)

    if res.status_code == 200:
        # Decode the base64 content
        content = res.json()["data"]
        decoded = base64.b64decode(content).decode("utf-8")

        # Write the decoded content to the local file
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(decoded)

        print(f"✅ CSV downloaded successfully to: {local_path}")
        return True
    else:
        print(f"❌ Failed to download CSV: {res.status_code} → {res.text}")
        return False



def validate_data_type(value, expected_type: str, custom_rules: dict = None) -> bool:
    """Validate if value matches expected data type with custom rules support"""
    if pd.isnull(value):
        return False
    
    try:
        value_str = str(value)
        
        if expected_type == "string":
            return isinstance(value, str) or len(value_str) > 0
        elif expected_type == "integer":
            return str(value).replace('-', '').isdigit()
        elif expected_type == "float":
            float(value)
            return True
        elif expected_type == "boolean":
            return str(value).lower() in ['true', 'false', '1', '0', 'yes', 'no']
        elif expected_type == "date":
            pd.to_datetime(value)
            return True
        elif expected_type == "email":
            return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value_str))
        elif expected_type == "phone":
                # Strip leading/trailing spaces
            value_str = value_str.strip()

            # Strong pre-check: disallow alphabetic characters or mixed types
            if re.search(r"[a-zA-Z]", value_str):
                return False  # Reject if any alphabet

            # Reject if non-phone characters present (not digit, space, +, -, (, ))
            if re.search(r"[^0-9\s\+\-\(\)]", value_str):
                return False

            # Custom rule override (e.g. E.164)
            if custom_rules and "phone_formats" in custom_rules:
                patterns = custom_rules["phone_formats"].get("patterns", [])
                for pattern in patterns:
                    if re.fullmatch(pattern, value_str):
                        return True
                return False

            # Default fallback: extract digits and check length
            digits_only = re.sub(r"[^\d]", "", value_str)
            return 10 <= len(digits_only) <= 15



        elif expected_type == "url":
            # Use custom URL validation if available
            if custom_rules and "url_validation" in custom_rules:
                accepted_domains = custom_rules["url_validation"].get("accepted_domains", [])
                if accepted_domains:
                    return any(domain in value_str for domain in accepted_domains) or value_str.startswith(('http://', 'https://'))
            return value_str.startswith(('http://', 'https://'))
        elif expected_type == "latlong":
            # Use custom coordinate validation if available
            if custom_rules and "coordinate_validation" in custom_rules:
                coord_match = re.match(r'^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$', value_str)
                if coord_match:
                    lat, lon = float(coord_match.group(1)), float(coord_match.group(2))
                    lat_range = custom_rules["coordinate_validation"].get("latitude_range", [-90, 90])
                    lon_range = custom_rules["coordinate_validation"].get("longitude_range", [-180, 180])
                    return lat_range[0] <= lat <= lat_range[1] and lon_range[0] <= lon <= lon_range[1]
                return False
            else:
                return bool(re.match(r'^-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?$', value_str))
        else:
            return True
    except:
        return False

def validate_constraints(value, constraints: dict) -> List[str]:
    """Validate value against constraints"""
    issues = []
    
    if pd.isnull(value):
        if constraints.get("required", False):
            issues.append("required_missing")
        return issues
    
    value_str = str(value)
    
    # Length constraints
    if "min_length" in constraints and len(value_str) < constraints["min_length"]:
        issues.append("min_length_violation")
    # In validate_constraints():
    if "max_length" in constraints and len(value_str) > constraints["max_length"]:
        issues.append("max_length_violation")

    
    # Numeric constraints
    if constraints.get("data_type") in ["integer", "float"]:
        try:
            num_value = float(value)
            if "min_value" in constraints and num_value < constraints["min_value"]:
                issues.append("min_value_violation")
            if "max_value" in constraints and num_value > constraints["max_value"]:
                issues.append("max_value_violation")
        except:
            pass
    
    # Pattern matching
    if "pattern" in constraints:
        if not re.match(constraints["pattern"], value_str):
            issues.append("pattern_violation")
    
    # Allowed values
    if "allowed_values" in constraints:
        if value not in constraints["allowed_values"]:
            issues.append("invalid_value")
    
    # Custom corruption detection
    if constraints.get("check_corruption", False):
        if re.search(r'[^\x20-\x7E]', value_str):
            issues.append("corrupt_characters")
        if sum(c in "!@#$%^&*()_+=[]{};:\"'<>,.?/~`" for c in value_str) > len(value_str) * 0.4:
            issues.append("excessive_special_chars")
    
    return issues

def detect_corruption_for_column(column_name: str, value, config: DataQualityConfig) -> bool:
    """Detect corruption based on column configuration"""
    if pd.isnull(value):
        return False
    
    value_str = str(value)
    column_rules = config.get_column_rules(column_name)
    data_type = column_rules.get("data_type", "string")
    
    # Generic corruption checks
    if column_rules.get("check_corruption", False):
        # Non-ASCII characters
        if re.search(r'[^\x20-\x7E]', value_str):
            return True
        # Excessive special characters
        if sum(c in "!@#$%^&*()_+=[]{};:\"'<>,.?/~`" for c in value_str) > len(value_str) * 0.4:
            return True
    
    # Data type specific corruption checks
    if data_type == "string":
        # Check for patterns that don't match expected string format
        if column_rules.get("pattern") and not re.match(column_rules["pattern"], value_str):
            return True
    elif data_type == "phone":
        # Invalid phone format
        if not validate_data_type(value, "phone", config.custom_validation_rules):
            return True
    elif data_type == "latlong":
        # Invalid coordinate format
        if not validate_data_type(value, "latlong", config.custom_validation_rules):
            return True
    elif data_type == "url":
        # Invalid URL format
        if not validate_data_type(value, "url", config.custom_validation_rules):
            return True
    elif data_type == "email":
        # Invalid email format
        if not validate_data_type(value, "email"):
            return True
    
    return False

# ========= LANGGRAPH NODES ==========
def load_data(state: AgentState) -> AgentState:
    """Load data from CSV file and replace corrupted entries based on configuration"""
    df = None  # Initialize df at the start
    
    try:
        config = state.get("config")
        csv_path = config.dataset_info.get("local_path", "data.csv")

        print(f"🔍 Loading data from: {csv_path}")
        df = pd.read_csv(csv_path, encoding='utf-8', on_bad_lines='skip', engine='python')
        
        # Save original file with _old suffix before cleaning
        original_path = csv_path
        backup_path = original_path.replace('.csv', '_original.csv')
        df.to_csv(backup_path, index=False)
        print(f"💾 Original data backed up to: {backup_path}")
        
        df.dropna(axis=0, how='all', inplace=True)

        print(f"📊 Original data shape: {df.shape}")
        print(f"📊 Available columns: {list(df.columns)}")
        print(f"📊 Configured columns: {config.get_configured_columns()}")

        # Replace corrupted values based on configuration
        def clean_value(col, val):
            if pd.isnull(val):
                return val
            
            # Only process columns that are configured
            if not config.is_column_configured(col):
                return val
            
            # Check for corruption based on column configuration
            if detect_corruption_for_column(col, val, config):
                return "corrupted data"
            
            return val

        # Apply cleaning to all columns
        for col in df.columns:
            if config.is_column_configured(col):
                df[col] = df[col].apply(lambda x: clean_value(col, x))

        print(f"✅ Cleaned corrupted values. Final shape: {df.shape}")
        print(f"✅ First few rows:\n{df.head()}")
        return {**state, "df": df, "original_filename": os.path.basename(csv_path)}

    except Exception as e:
        print(f"❌ Error loading data: {str(e)}")
        sample_data = create_sample_data(state.get("config"))
        print(f"🔧 Using sample data:\n{sample_data}")
        return {**state, "df": sample_data, "original_filename": "sample_data.csv"}
    
    
def create_sample_data(config: DataQualityConfig) -> pd.DataFrame:
    """Create sample data based on schema configuration"""
    sample_data = {}
    
    for column, rules in config.validation_rules.items():
        data_type = rules.get("data_type", "string")
        
        if data_type == "string":
            sample_data[column] = ["Sample A", "Sample B", None, "Sample C"]
        elif data_type == "integer":
            sample_data[column] = [1, 2, 3, None]
        elif data_type == "float":
            sample_data[column] = [1.1, 2.2, 3.3, None]
        elif data_type == "phone":
            sample_data[column] = ["1234567890", "987654321", "invalid", None]
        elif data_type == "email":
            sample_data[column] = ["test@example.com", "invalid-email", None, "user@domain.com"]
        elif data_type == "url":
            sample_data[column] = ["https://example.com", "not-a-url", None, "https://test.com"]
        elif data_type == "latlong":
            sample_data[column] = ["40.7128,-74.0060", "invalid", None, "34.0522,-118.2437"]
        elif data_type == "date":
            sample_data[column] = ["2024-01-01", "2024-02-02", None, "invalid-date"]
        elif data_type == "boolean":
            sample_data[column] = [True, False, None, "invalid"]
        else:
            sample_data[column] = ["Value A", "Value B", None, "Value C"]
    
    return pd.DataFrame(sample_data)

def analyze_data(state: AgentState) -> AgentState:
    """Analyze data quality issues based on schema configuration"""
    df = state.get("df", pd.DataFrame())
    config = state.get("config")
    issues_detected = []
    
    print(f"🔍 Analyzing data with {len(df)} rows")
    print(f"🔍 Analyzing columns: {[col for col in df.columns if config.is_column_configured(col)]}")
    
    for i, row in df.iterrows():
        for col in df.columns:
            if not config.is_column_configured(col):
                continue  # Skip columns not configured in schema
            
            value = row[col]
            column_rules = config.get_column_rules(col)
            
            # Check data type
            expected_type = column_rules.get("data_type", "string")
            if not pd.isnull(value) and not validate_data_type(value, expected_type, config.custom_validation_rules):
                issues_detected.append({
                    "row": i, "column": col, "issue": f"invalid_{expected_type}",
                    "value": value
                })
            
            # Check constraints
            constraint_issues = validate_constraints(value, column_rules)
            for issue in constraint_issues:
                issues_detected.append({
                    "row": i, "column": col, "issue": issue,
                    "value": value
                })
    
    
    return {**state, "issues": issues_detected}


import json

def summarize_issues(state: AgentState) -> AgentState:
    issues = state.get("issues", [])
    print("\n\n")
    print(f"🔍 Found {len(issues)} issues across configured columns")

    column_issues = {}
    for issue in issues:
        col = issue["column"]
        issue_type = issue["issue"]
        if col not in column_issues:
            column_issues[col] = {}
        column_issues[col][issue_type] = column_issues[col].get(issue_type, 0) + 1

    print("📊 Issues by column:")
    for col, issues in column_issues.items():
        print(f"  {col}: {dict(issues)}")

    # Save to JSON file
    output_path = "column_issues_summary.json"
    try:
        with open(output_path, "w") as f:
            json.dump(column_issues, f, indent=2)
        print(f"\n✅ Column issues summary saved to: {output_path}")
    except Exception as e:
        print(f"❌ Failed to save column issues summary: {e}")

    #save column wise issues in json  



    state['fix_json'] = fix_json = {
    "Name": {
        "required_missing": True,
        "data_type_issue": True,
        "min_length_violation": True,
        "max_length_violation": True,
        "pattern_violation": True,
        "corrupt_characters": True,
        "excessive_special_chars": True
    },
    "Address": {
        "required_missing": True,
        "data_type_issue": True,
        "min_length_violation": True,
        "max_length_violation": True,
        "pattern_violation": True,
        "corrupt_characters": True,
        "excessive_special_chars": True
    },
    "Phone": {
        "required_missing": True,
        "data_type_issue": True,
        "min_length_violation": True,
        "max_length_violation": True,
        "pattern_violation": True,
        "corrupt_characters": True,
        "excessive_special_chars": True
    },
    "LatLong": {
        "required_missing": True,
        "data_type_issue": True,
        "min_length_violation": True,
        "max_length_violation": True,
        "pattern_violation": True,
        "corrupt_characters": True,
        "excessive_special_chars": True
    },
    "URL": {
        "required_missing": True,
        "data_type_issue": True,
        "min_length_violation": True,
        "max_length_violation": True,
        "pattern_violation": True,
        "corrupt_characters": True,
        "excessive_special_chars": True
    }
}


    return state


def fix_data(state: AgentState) -> AgentState:
    """Fix data quality issues based on schema configuration"""

    df = state.get("df", pd.DataFrame()).copy()
    issues = state.get("issues", [])
    config = state.get("config")

    fix_json = state.get("fix_json", {})  # You need to make sure this is passed in your state
    fix_log = []

    # print(f"🔧 Fixing {len(issues)} issues")



    for issue in issues:
        row, col, issue_type = issue["row"], issue["column"], issue["issue"]

        if row is None or col not in df.columns:
            continue

        # Retrieve per-column flags
        column_flags = fix_json.get(col, {})

        # Check required_missing flag
        if issue_type == "required_missing":
            if not column_flags.get("required_missing", False):
                continue

        # Check data_type_issue flag for "invalid_*" issues
        if issue_type.startswith("invalid_"):
            if not column_flags.get("data_type_issue", False):
                continue

        if issue_type == "min_length_violation":
            if not column_flags.get("min_length_violation", False):
                continue

        if issue_type == "max_length_violation":
            if not column_flags.get("max_length_violation", False):
                continue

        if issue_type == "corrupt_characters":
            if not column_flags.get("corrupt_characters", False):
                continue

        if issue_type == "pattern_violation":
            if not column_flags.get("pattern_violation", False):
                continue

        old_value = df.at[row, col] if pd.notnull(df.at[row, col]) else None
        fix_value = config.get_fix_strategy(col, issue_type)

        # Apply dynamic fix based on data type if default fix is used
        if fix_value == "N/A":
            column_rules = config.get_column_rules(col)
            data_type = column_rules.get("data_type", "string")

            default_fixes = config.fix_strategies.get("default", {})
            fix_value = default_fixes.get(f"invalid_{data_type}", "N/A")

        df.at[row, col] = fix_value
        fix_log.append({
            "row": row,
            "column": col,
            "issue": issue_type,
            "old_value": old_value,
            "new_value": fix_value
        })

    print(f"✅ Fixed {len(fix_log)} issues")

    return {**state, "df_cleaned": df, "fix_log": pd.DataFrame(fix_log)}





# def apply_data_enhancement(df: pd.DataFrame, config: DataQualityConfig) -> pd.DataFrame:
#     """Apply data enhancement based on configuration"""
#     enhancement_config = config.data_enhancement
    
#     if not enhancement_config:
#         return df
    
#     print("🔧 Applying data enhancements...")
    
#     # Phone standardization
#     if enhancement_config.get("phone_standardization", {}).get("enabled", False):
#         for col in df.columns:
#             if config.is_column_configured(col):
#                 column_rules = config.get_column_rules(col)
#                 if column_rules.get("data_type") == "phone":
#                     df[col] = df[col].apply(lambda x: standardize_phone(x) if pd.notnull(x) else x)
    
#     # Coordinate precision
#     if enhancement_config.get("coordinate_precision", {}).get("enabled", False):
#         decimal_places = enhancement_config["coordinate_precision"].get("decimal_places", 7)
#         for col in df.columns:
#             if config.is_column_configured(col):
#                 column_rules = config.get_column_rules(col)
#                 if column_rules.get("data_type") == "latlong":
#                     df[col] = df[col].apply(lambda x: standardize_coordinates(x, decimal_places) if pd.notnull(x) else x)
    
#     return df

# def standardize_phone(phone_str: str) -> str:
#     """Standardize phone number format"""
#     if pd.isnull(phone_str) or phone_str == "corrupted data":
#         return phone_str
    
#     # Extract digits only
#     digits = re.sub(r'[^\d]', '', str(phone_str))
    
#     # Basic Indian phone number standardization
#     if len(digits) == 10 and digits[0] in '6789':
#         return f"+91{digits}"
#     elif len(digits) == 12 and digits.startswith('91'):
#         return f"+{digits}"
#     elif len(digits) == 13 and digits.startswith('91'):
#         return f"+{digits}"
    
#     return phone_str

# def standardize_coordinates(coord_str: str, decimal_places: int = 7) -> str:
#     """Standardize coordinate precision"""
#     if pd.isnull(coord_str) or coord_str == "corrupted data":
#         return coord_str
    
#     try:
#         coord_match = re.match(r'^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$', str(coord_str))
#         if coord_match:
#             lat, lon = float(coord_match.group(1)), float(coord_match.group(2))
#             return f"{lat:.{decimal_places}f},{lon:.{decimal_places}f}"
#     except:
#         pass
    
#     return coord_str




def delete_from_databricks(path: str) -> None:
    """Delete an existing Databricks workspace file/folder."""
    delete_response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/workspace/delete",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"path": path, "recursive": True}
    )

    if delete_response.status_code == 200:
        print(f"🗑 Deleted existing path: {path}")
    elif delete_response.status_code == 400 and "does not exist" in delete_response.text:
        print(f"ℹ Path did not exist: {path}")
    else:
        print(f"⚠ Failed to delete path ({delete_response.status_code}): {delete_response.text}")





from io import BytesIO

def upload_to_databricks(state: AgentState) -> AgentState:
    df = state.get("df_cleaned", pd.DataFrame())
    original_filename = state.get("original_filename", "cleaned_data.csv")
    ext = os.path.splitext(original_filename)[-1].lower()

    print(f"📤 Uploading {len(df)} rows to Databricks DBFS as {ext}")

    try:
        # Encode content based on extension
        if ext == ".csv":
            content = df.to_csv(index=False).encode("utf-8")
        elif ext in [".xlsx", ".xls"]:
            buffer = BytesIO()
            df.to_excel(buffer, index=False)
            buffer.seek(0)
            content = buffer.read()
        elif ext == ".json":
            content = df.to_json(orient='records', indent=2).encode("utf-8")
        elif ext == ".parquet":
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            content = buffer.read()
        else:
            raise ValueError(f"Unsupported upload format: {ext}")

        # Save a local copy with 'local' suffix
        local_filename = f"{os.path.splitext(original_filename)[0]}_local{ext}"
        with open(local_filename, 'wb') as f:
            f.write(content)
        print(f"✅ Local copy saved as: {local_filename}")

        # Prepare DBFS path
        dbfs_path = f"/FileStore/{original_filename}"

        # Upload to Databricks
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/put",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={
                "path": dbfs_path,
                "overwrite": True,
                "contents": base64.b64encode(content).decode("utf-8")
            }
        )

        if response.status_code == 200:
            print(f"✅ Uploaded to DBFS at: {dbfs_path}")
            return {**state, "upload_status": 200, "upload_response": {"dbfs_path": dbfs_path}}
        else:
            print(f"❌ Upload failed: {response.text}")
            return {**state, "upload_status": response.status_code, "upload_response": response.json()}

    except Exception as e:
        print(f"❌ Upload error: {str(e)}")
        return {**state, "upload_status": 500, "upload_response": {"error": str(e)}}


def validate(state: AgentState) -> AgentState:
    """Validate the data quality workflow results"""
    issues = state.get("issues", [])
    fix_log = state.get("fix_log", pd.DataFrame())
    upload_status = state.get("upload_status", 0)
    config = state.get("config")
    
    # Generate quality metrics
    metrics = generate_quality_metrics(state)
    
    if not issues:
        result = "✅ No issues detected. Data is clean."
    else:
        result = f"🔧 {len(fix_log)} issues were fixed across {len(set(issue['column'] for issue in issues))} columns."
    
    if upload_status == 200:
        result += " ✅ Data uploaded successfully."
    else:
        result += f" ❌ Upload failed with status {upload_status}."
    
    # Add quality metrics to result
    if metrics:
        result += f"\n📊 Quality Metrics: {metrics}"
    
    print(f"📊 Validation: {result}")
    return {**state, "validation": result}


# ========= WORKFLOW CREATION ==========
def create_workflow():
    """Create the data quality workflow"""
    graph = StateGraph(AgentState)
    
    # Add nodes
    graph.add_node("LoadData", load_data)
    graph.add_node("AnalyzeData", analyze_data)
    graph.add_node("FixData", fix_data)
    graph.add_node("UploadToDatabricks", upload_to_databricks)
    graph.add_node("Validate", validate)
    
    graph.add_node("SummarizeIssues", summarize_issues)
    
    # Define edges
    graph.set_entry_point("LoadData")
    graph.add_edge("LoadData", "AnalyzeData")
    graph.add_edge("AnalyzeData", "SummarizeIssues")
    graph.add_edge("SummarizeIssues", "FixData")
    graph.add_edge("FixData", "UploadToDatabricks")
    graph.add_edge("UploadToDatabricks", "Validate")
    graph.add_edge("Validate", END)
    
    return graph.compile()



def generate_quality_metrics(state: AgentState) -> dict:
    """Generate quality metrics based on configuration"""
    df = state.get("df_cleaned", pd.DataFrame())
    config = state.get("config")
    
    if df.empty or not config:
        return {}
    
    metrics = {}
    quality_config = config.config.get("quality_metrics", {})
    
    # Completeness metrics
    if "completeness" in quality_config:
        required_fields = quality_config["completeness"].get("required_fields", [])
        target_percentage = quality_config["completeness"].get("target_percentage", 95.0)
        
        completeness_scores = {}
        for field in required_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness_scores[field] = (non_null_count / len(df)) * 100
        
        metrics["completeness"] = completeness_scores
    
    return metrics

def generate_quality_report(final_state: AgentState, config: DataQualityConfig):
    """Generate a comprehensive quality report"""
    report = {
        "timestamp": datetime.now().isoformat(),
        "dataset_info": config.dataset_info,
        "summary": {
            "total_rows": len(final_state.get("df_cleaned", pd.DataFrame())),
            "total_issues_found": len(final_state.get("issues", [])),
            "total_fixes_applied": len(final_state.get("fix_log", pd.DataFrame())),
            "upload_status": final_state.get("upload_status")
        },
        "column_analysis": {},
        "quality_metrics": generate_quality_metrics(final_state)
    }
    
    # Column-wise analysis
    issues = final_state.get("issues", [])
    for col in config.get_configured_columns():
        col_issues = [issue for issue in issues if issue["column"] == col]
        report["column_analysis"][col] = {
            "total_issues": len(col_issues),
            "issue_types": {}
        }
        
        for issue in col_issues:
            issue_type = issue["issue"]
            report["column_analysis"][col]["issue_types"][issue_type] = \
                report["column_analysis"][col]["issue_types"].get(issue_type, 0) + 1
    
    # Save report
    report_path = config.output_config.get("report_path", "data_quality_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"📄 Quality report saved to: {report_path}")

  # import the function from wherever it's defined





def run_full_data_quality_pipeline(
    filestore_path: str,
    local_path: str,
    schema_output_path: str = "generated_schema.json",
    sample_size: int = 20,
    overrides: dict = None
):
    """
    Run the complete data quality pipeline:
    1. Download CSV from Databricks workspace
    2. Generate schema using LLM
    3. Execute data validation, fixing, enhancement, and upload
    """
    try:
        print("🚀 Running full data quality pipeline...")

        # Step 1: Download raw data from Databricks
        success = download_csv_from_dbfs(filestore_path, local_path)
        if not success:
            raise Exception("❌ Failed to download CSV from Filestore")

        # Step 2: Generate schema config using the downloaded CSV
        generate_schema_from_dataset(
            local_path, sample_size, schema_output_path
        )

        # Load configuration
        config = DataQualityConfig(config_path=schema_output_path, config_dict=overrides)

        print(f"📋 Generated Configuration loaded:")
        print(f"  - Dataset: {config.dataset_info.get('name', 'Unknown')}")
        print(f"  - Configured columns: {config.get_configured_columns()}")
        print(f"  - Total validation rules: {len(config.validation_rules)}")

        # Create and run workflow
        app = create_workflow()
        initial_state: AgentState = {
            "df": None, "issues": None, "df_cleaned": None,
            "fix_log": None, "fix_json" : None, "upload_status": None,
            "upload_response": None, "validation": None,
            "config": config
        }

        final_state = app.invoke(initial_state)

        # Print results 
        print("\n" + "="*50)
        print("📊 WORKFLOW RESULTS")
        print("="*50)

        if final_state.get("df_cleaned") is not None and not final_state["df_cleaned"].empty:
            print(f"\n✅ Cleaned Data Sample ({len(final_state['df_cleaned'])} rows):")
            print(final_state["df_cleaned"].head())

        if final_state.get("fix_log") is not None and not final_state["fix_log"].empty:
            print(f"\n🛠 Fix Log ({len(final_state['fix_log'])} fixes):")
            print(final_state["fix_log"].head(10))

        print(f"\n📤 Upload Status: {final_state.get('upload_status')}")
        print(f"📄 Validation Result: {final_state.get('validation')}")

        # Generate and save report if configured
        if config.output_config.get("generate_report", False):
            generate_quality_report(final_state, config)

        # Save cleaned output locally (optional)
        output_path = config.output_config.get("local_output_path")
        if output_path:
            ext = os.path.splitext(output_path)[-1].lower()
            try:
                df_cleaned = final_state.get("df_cleaned")
                if df_cleaned is not None:
                    if ext == ".csv":
                        df_cleaned.to_csv(output_path, index=False)
                    elif ext in [".xlsx", ".xls"]:
                        df_cleaned.to_excel(output_path, index=False)
                    elif ext == ".json":
                        df_cleaned.to_json(output_path, orient="records", indent=2)
                    elif ext == ".parquet":
                        df_cleaned.to_parquet(output_path, index=False)
                    print(f"💾 Cleaned data saved locally to: {output_path}")
            except Exception as e:
                print(f"❌ Failed to save cleaned data locally: {e}")

        return final_state

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None