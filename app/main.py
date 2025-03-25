import pandas as pd
import pdfplumber
import re
import json
import os
import io
from typing import Dict, List, Optional, Union, Tuple

from fastapi import FastAPI, File, UploadFile, HTTPException, Body, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import magic  # For file type detection

class RegulatoryRuleExtractor:
    def __init__(self):
        """
        Initialize the extractor with class methods
        """
        self.dataset_path = None
        self.pdf_path = None
        self.columns = []
        self.extracted_rules = {}
    
    def _load_columns(self, file_path: str) -> List[str]:
        """
        Load column names from the dataset
        
        :param file_path: Path to Excel or CSV file
        :return: List of column names
        """
        try:
            # Detect file type
            file_mime = magic.Magic(mime=True).from_file(file_path)
            
            if file_mime in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                df = pd.read_excel(file_path)
            elif file_mime == 'text/csv':
                df = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_mime}")
            
            return list(df.columns)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading dataset: {str(e)}")
    
    def extract_rules(self, dataset_bytes: bytes, pdf_bytes: bytes, fuzzy_match: bool = True, context_window: int = 250) -> Dict[str, List[str]]:
        """
        Extract rules from dataset and PDF bytes
        
        :param dataset_bytes: Bytes of dataset file
        :param pdf_bytes: Bytes of PDF file
        :param fuzzy_match: Use fuzzy matching for column names
        :param context_window: Number of characters around the match to extract
        :return: Dictionary of column names and their associated rules
        """
        # Create temporary files
        try:
            # Detect dataset file type
            dataset_mime = magic.Magic(mime=True).from_buffer(dataset_bytes)
            
            # Create temp files
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx' if 'excel' in dataset_mime else '.csv') as dataset_temp, \
                 tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as pdf_temp:
                
                # Write dataset bytes
                dataset_temp.write(dataset_bytes)
                dataset_temp.flush()
                
                # Write PDF bytes
                pdf_temp.write(pdf_bytes)
                pdf_temp.flush()
                
                # Store paths
                self.dataset_path = dataset_temp.name
                self.pdf_path = pdf_temp.name
            
            # Load columns
            self.columns = self._load_columns(self.dataset_path)
            
            rules = {}
            
            # Open PDF
            with pdfplumber.open(self.pdf_path) as pdf:
                for column in self.columns:
                    column_rules = []
                    
                    # Search through all pages
                    for page in pdf.pages:
                        text = page.extract_text()
                        
                        # Different matching strategies
                        if fuzzy_match:
                            # Fuzzy matching - looks for similar column names
                            matches = self._fuzzy_find(text, column)
                        else:
                            # Exact matching
                            matches = self._exact_find(text, column)
                        
                        # Extract rules near matches
                        for match_start, match_end in matches:
                            # Extract a wider context around the match
                            start = max(0, match_start - context_window)
                            end = min(len(text), match_end + context_window)
                            context = text[start:end].strip()
                            
                            # Clean and validate the rule
                            cleaned_rule = self._clean_rule(context)
                            if cleaned_rule:
                                column_rules.append(cleaned_rule)
                    
                    # Remove duplicates and sort
                    column_rules = sorted(set(column_rules), key=len, reverse=True)
                    rules[column] = column_rules
            
            self.extracted_rules = rules
            return rules
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Rule extraction error: {str(e)}")
        
        finally:
            # Clean up temporary files
            if hasattr(self, 'dataset_path') and os.path.exists(self.dataset_path):
                os.unlink(self.dataset_path)
            if hasattr(self, 'pdf_path') and os.path.exists(self.pdf_path):
                os.unlink(self.pdf_path)
    
    def _clean_rule(self, rule: str) -> str:
        """
        Clean and validate extracted rule
        
        :param rule: Raw extracted rule text
        :return: Cleaned rule text
        """
        # Remove excessive whitespace
        rule = re.sub(r'\s+', ' ', rule).strip()
        
        # Remove PDF artifacts
        rule = re.sub(r'\n+', ' ', rule)
        
        # Minimum meaningful length
        if len(rule) < 20:
            return ''
        
        return rule
    
    def _fuzzy_find(self, text: str, column: str) -> List[Tuple[int, int]]:
        """
        Fuzzy matching of column names in text
        
        :param text: Text to search
        :param column: Column name to match
        :return: List of match start and end indices
        """
        matches = []
        lower_text = text.lower()
        lower_column = column.lower()
        
        # Various fuzzy matching approaches
        patterns = [
            # Exact word match
            rf'\b{re.escape(lower_column)}\b',
            # Partial match
            rf'{re.escape(lower_column)}',
            # Variations with common separators
            rf'{re.escape(lower_column.replace(" ", "_"))}',
            rf'{re.escape(lower_column.replace(" ", "-"))}'
        ]
        
        # Try different patterns
        for pattern in patterns:
            for match in re.finditer(pattern, lower_text):
                matches.append((match.start(), match.end()))
        
        return matches
    
    def _exact_find(self, text: str, column: str) -> List[Tuple[int, int]]:
        """
        Exact matching of column names in text
        
        :param text: Text to search
        :param column: Column name to match
        :return: List of match start and end indices
        """
        matches = []
        lower_text = text.lower()
        lower_column = column.lower()
        
        # Find all exact occurrences
        for match in re.finditer(rf'\b{re.escape(lower_column)}\b', lower_text):
            matches.append((match.start(), match.end()))
        
        return matches

    def refine_rules(self, rules: Dict[str, List[str]], column: str, actions: List[Dict]) -> Dict[str, List[str]]:
        """
        Refine rules for a specific column
        
        :param rules: Current rules dictionary
        :param column: Column to refine
        :param actions: List of rule modification actions
        :return: Updated rules dictionary
        """
        if column not in rules:
            raise HTTPException(status_code=404, detail=f"Column {column} not found")
        
        column_rules = rules[column]
        
        for action in actions:
            action_type = action.get('type')
            
            if action_type == 'remove':
                # Remove rule by index
                index = action.get('index')
                if 0 <= index < len(column_rules):
                    del column_rules[index]
            
            elif action_type == 'add':
                # Add new rule
                new_rule = action.get('rule')
                if new_rule:
                    column_rules.append(new_rule)
            
            elif action_type == 'edit':
                # Edit existing rule
                index = action.get('index')
                new_rule = action.get('rule')
                if 0 <= index < len(column_rules) and new_rule:
                    column_rules[index] = new_rule
        
        # Update rules
        rules[column] = column_rules
        return rules

# FastAPI Application
app = FastAPI(
    title="Advanced Regulatory Rule Extraction Service",
    description="API for extracting and refining regulatory rules from datasets and PDFs with flexible input",
    version="1.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Global extractor instance
extractor = RegulatoryRuleExtractor()

@app.post("/extract-rules/")
async def extract_rules(
    dataset: UploadFile = File(...), 
    pdf: UploadFile = File(...),
    fuzzy_match: bool = Query(True, description="Enable fuzzy matching"),
    context_window: int = Query(250, description="Context window for rule extraction")
):
    """
    Extract regulatory rules from uploaded dataset and PDF
    
    Supports:
    - Excel (.xlsx, .xls)
    - CSV files
    - PDF regulatory documents
    
    :param dataset: Uploaded dataset file
    :param pdf: Uploaded PDF regulatory document
    :param fuzzy_match: Enable fuzzy matching
    :param context_window: Context window for rule extraction
    :return: Extracted rules
    """
    try:
        # Read file bytes
        dataset_bytes = await dataset.read()
        pdf_bytes = await pdf.read()
        
        # Validate file types
        dataset_mime = magic.Magic(mime=True).from_buffer(dataset_bytes)
        pdf_mime = magic.Magic(mime=True).from_buffer(pdf_bytes)
        
        # Validate input file types
        valid_dataset_mimes = [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'text/csv'
        ]
        valid_pdf_mimes = ['application/pdf']
        
        if dataset_mime not in valid_dataset_mimes:
            raise HTTPException(status_code=400, detail=f"Invalid dataset file type: {dataset_mime}")
        
        if pdf_mime not in valid_pdf_mimes:
            raise HTTPException(status_code=400, detail=f"Invalid PDF file type: {pdf_mime}")
        
        # Extract rules
        rules = extractor.extract_rules(
            dataset_bytes, 
            pdf_bytes, 
            fuzzy_match=fuzzy_match, 
            context_window=context_window
        )
        
        return JSONResponse(content=rules)
    
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions
        raise http_exc
    except Exception as e:
        # Catch and handle unexpected errors
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/refine-rules/")
async def refine_rules(
    column: str = Body(..., description="Column to refine"), 
    rules: Dict[str, List[str]] = Body(..., description="Current rules dictionary"), 
    actions: List[Dict] = Body(..., description="Rule modification actions")
):
    """
    Refine rules for a specific column
    
    :param column: Column to refine
    :param rules: Current rules dictionary
    :param actions: List of rule modification actions
    :return: Updated rules
    """
    try:
        refined_rules = extractor.refine_rules(rules, column, actions)
        return JSONResponse(content=refined_rules)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-rules/")
async def export_rules(
    rules: Dict[str, List[str]] = Body(..., description="Rules to export"),
    format: str = Query("json", description="Export format", enum=["json", "markdown"])
):
    """
    Export refined rules to a file
    
    :param rules: Rules to export
    :param format: Export format (json or markdown)
    :return: Exported file
    """
    try:
        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format == "json":
            # Export as JSON
            output_path = f"regulatory_rules_{timestamp}.json"
            with open(output_path, "w") as f:
                json.dump(rules, f, indent=2)
            return FileResponse(output_path, media_type="application/json", filename=output_path)
        
        elif format == "markdown":
            # Export as Markdown
            output_path = f"regulatory_rules_report_{timestamp}.md"
            with open(output_path, "w", encoding='utf-8') as f:
                f.write("# Regulatory Rules Analysis Report\n\n")
                f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                for column, column_rules in rules.items():
                    f.write(f"## Column: {column}\n\n")
                    
                    if not column_rules:
                        f.write("*No rules found or retained for this column.*\n\n")
                    else:
                        f.write("### Refined Regulatory Rules:\n")
                        for i, rule in enumerate(column_rules, 1):
                            f.write(f"{i}. {rule}\n")
                        f.write("\n")
            
            return FileResponse(output_path, media_type="text/markdown", filename=output_path)
        
        else:
            raise HTTPException(status_code=400, detail="Unsupported export format")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Additional imports at the end to avoid circular imports
import tempfile
from datetime import datetime

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
