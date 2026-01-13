"""Data cleaning and validation utilities."""

import re
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def map_fields(raw_data: Dict, field_mapping: Dict[str, List[str]]) -> Dict:
    """Map extracted fields to standard field names.
    
    Args:
        raw_data: Raw extracted data
        field_mapping: Dictionary mapping standard names to variations
        
    Returns:
        Dictionary with mapped field names
    """
    mapped_data = {}
    
    for standard_name, variations in field_mapping.items():
        for variation in variations:
            # Try with and without colon
            for key in [variation, variation + ':', variation + '：']:
                if key in raw_data:
                    mapped_data[standard_name] = raw_data[key]
                    break
            if standard_name in mapped_data:
                break
        
        # Set empty string if not found
        if standard_name not in mapped_data:
            mapped_data[standard_name] = ''
    
    return mapped_data


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace in text.
    
    Args:
        text: Input text
        
    Returns:
        Text with normalized whitespace
    """
    if not text:
        return ''
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Collapse multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Remove newlines and tabs
    text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    
    # Collapse again after replacements
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def extract_date(text: str) -> Optional[str]:
    """Extract date from text.
    
    Args:
        text: Input text
        
    Returns:
        Extracted date string or None
    """
    # Pattern for YYYY-MM-DD or YYYY/MM/DD or YYYY.MM.DD
    patterns = [
        r'\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}',
        r'\d{4}年\d{1,2}月\d{1,2}日',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    
    return None


def extract_approval_number(text: str) -> Optional[str]:
    """Extract approval number from text.
    
    Args:
        text: Input text
        
    Returns:
        Extracted approval number or None
    """
    # Pattern for approval numbers (adjust based on actual format)
    patterns = [
        r'[国进]药准字[A-Z]\d{8}',
        r'[A-Z]\d{8}',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    
    return None


def extract_drug_code(text: str) -> Optional[str]:
    """Extract drug code from text.
    
    Args:
        text: Input text
        
    Returns:
        Extracted drug code or None
    """
    # Pattern for drug codes
    pattern = r'\d{12,20}'
    match = re.search(pattern, text)
    
    if match:
        return match.group(0)
    
    return None


def validate_fields(data: Dict, required_fields: List[str]) -> Dict:
    """Validate that required fields are present.
    
    Args:
        data: Data dictionary
        required_fields: List of required field names
        
    Returns:
        Dictionary with validation results
    """
    missing_fields = []
    
    for field in required_fields:
        if not data.get(field):
            missing_fields.append(field)
    
    total_fields = len(required_fields)
    present_fields = total_fields - len(missing_fields)
    completeness = f"{(present_fields / total_fields * 100):.0f}%"
    
    return {
        'is_valid': len(missing_fields) == 0,
        'completeness': completeness,
        'missing_fields': missing_fields,
        'present_fields': present_fields,
        'total_fields': total_fields
    }


def clean_drug_data(data: Dict) -> Dict:
    """Clean and normalize drug data.
    
    Args:
        data: Raw drug data
        
    Returns:
        Cleaned drug data
    """
    cleaned = {}
    
    for key, value in data.items():
        if isinstance(value, str):
            # Normalize whitespace
            value = normalize_whitespace(value)
            
            # Extract structured data if applicable
            if '日期' in key or 'date' in key.lower():
                extracted_date = extract_date(value)
                if extracted_date:
                    value = extracted_date
            
            elif '批准' in key or 'approval' in key.lower():
                extracted_approval = extract_approval_number(value)
                if extracted_approval:
                    value = extracted_approval
        
        cleaned[key] = value
    
    return cleaned


__all__ = [
    'map_fields',
    'normalize_whitespace',
    'extract_date',
    'extract_approval_number',
    'extract_drug_code',
    'validate_fields',
    'clean_drug_data'
]
