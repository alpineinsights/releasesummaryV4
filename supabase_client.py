"""
This module handles the integration with Supabase to fetch company data.
Uses the Supabase Python client for more reliable connections.
"""

import os
from supabase import create_client
from typing import Dict, List, Optional
import pandas as pd
import logging
from dotenv import load_dotenv
from functools import lru_cache
import unicodedata
import re
from difflib import SequenceMatcher

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Initialize Supabase client
@lru_cache(maxsize=1)
def init_client():
    """
    Initialize and cache the Supabase client connection
    """
    try:
        # Use the hardcoded credentials as fallback
        supabase_url = "https://maeistbokyjhewrrisvf.supabase.co"
        supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1hZWlzdGJva3lqaGV3cnJpc3ZmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDMxNTgyMTUsImV4cCI6MjA1ODczNDIxNX0._Fb4I1BvmqMHbB5KyrtlEmPTyF8nRgR9LsmNFmiZSN8"
        
        # Override with values from environment variables if available
        env_url = os.getenv("SUPABASE_URL")
        env_key = os.getenv("SUPABASE_ANON_KEY")
        
        if env_url:
            supabase_url = env_url
        if env_key:
            supabase_key = env_key
                
        # Initialize the client
        client = create_client(supabase_url, supabase_key)
        return client
        
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {str(e)}")
        return None

@lru_cache(maxsize=100)
def get_all_companies() -> List[Dict]:
    """
    Fetches all companies from the Supabase 'universe' table.
    
    Returns:
        List[Dict]: A list of company data dictionaries
    """
    try:
        client = init_client()
        if not client:
            return []
            
        response = client.table('universe').select('*').execute()
        if hasattr(response, 'data'):
            return response.data
        return []
    except Exception as e:
        logger.error(f"Error fetching companies from Supabase: {str(e)}")
        return []

@lru_cache(maxsize=100)
def get_company_names() -> List[str]:
    """
    Returns a list of all company names from Supabase.
    
    Returns:
        List[str]: A list of company names
    """
    companies = get_all_companies()
    return [company["Name"] for company in companies if "Name" in company]

@lru_cache(maxsize=100)
def get_quartrid_by_name(company_name: str) -> Optional[str]:
    """
    Retrieves the Quartr ID for a given company name from Supabase.
    
    Args:
        company_name (str): The company name to look up
        
    Returns:
        str: The Quartr ID if found, None otherwise
    """
    def _normalize(text: str) -> str:
        if not isinstance(text, str):
            return ""
        # Remove accents/diacritics, lowercase, collapse whitespace
        nfkd = unicodedata.normalize('NFKD', text)
        no_accents = ''.join(ch for ch in nfkd if not unicodedata.combining(ch))
        no_punct = re.sub(r"[^A-Za-z0-9\s&'().-]", " ", no_accents)
        collapsed = re.sub(r"\s+", " ", no_punct).strip()
        return collapsed.lower()

    try:
        client = init_client()
        if not client:
            return None

        # 1) Exact match first (fast path)
        response = client.table('universe').select('\"Quartr Id\"').eq('Name', company_name).execute()
        if response.data and len(response.data) > 0:
            quartr_id = response.data[0].get("Quartr Id")
            logger.info(f"Found Quartr ID {quartr_id} for company: {company_name} (exact match)")
            return str(quartr_id)

        # 2) Case-insensitive partial using ilike
        try:
            ilike_pattern = f"%{company_name}%"
            response_ci = client.table('universe').select('Name, \"Quartr Id\"').ilike('Name', ilike_pattern).execute()
            if response_ci.data:
                # Choose best with highest similarity on normalized names
                target_norm = _normalize(company_name)
                best_row = None
                best_score = 0.0
                for row in response_ci.data:
                    row_name = row.get('Name', '')
                    score = SequenceMatcher(None, _normalize(row_name), target_norm).ratio()
                    if score > best_score:
                        best_score = score
                        best_row = row
                if best_row and best_score >= 0.80:
                    quartr_id = best_row.get('Quartr Id')
                    logger.info(f"Found Quartr ID {quartr_id} for company: {company_name} (ilike+fuzzy, score={best_score:.2f}, matched='{best_row.get('Name','')}')")
                    return str(quartr_id)
        except Exception:
            # ilike path may not be supported in some client states; ignore and continue to in-memory fallback
            pass

        # 3) In-memory accent-insensitive fuzzy match as final fallback
        companies = get_all_companies()
        if not companies:
            return None
        target_norm = _normalize(company_name)

        # First try normalized equality
        for row in companies:
            if _normalize(row.get('Name', '')) == target_norm:
                quartr_id = row.get('Quartr Id')
                if quartr_id is not None:
                    logger.info(f"Found Quartr ID {quartr_id} for company: {company_name} (normalized equality='{row.get('Name','')}')")
                    return str(quartr_id)

        # Then best fuzzy
        best_row = None
        best_score = 0.0
        for row in companies:
            row_name = row.get('Name', '')
            score = SequenceMatcher(None, _normalize(row_name), target_norm).ratio()
            if score > best_score:
                best_score = score
                best_row = row
        if best_row and best_score >= 0.80:
            quartr_id = best_row.get('Quartr Id')
            if quartr_id is not None:
                logger.info(f"Found Quartr ID {quartr_id} for company: {company_name} (fallback fuzzy, score={best_score:.2f}, matched='{best_row.get('Name','')}')")
                return str(quartr_id)

        logger.warning(f"Quartr ID not found for '{company_name}' after exact, ilike, and fuzzy fallbacks")
        return None
    except Exception as e:
        logger.error(f"Error fetching Quartr ID for {company_name}: {str(e)}")
        return None

@lru_cache(maxsize=100)
def get_company_by_quartrid(quartrid: str) -> Optional[Dict]:
    """
    Retrieves company data for a given Quartr ID from Supabase.
    
    Args:
        quartrid (str): The Quartr ID to look up
        
    Returns:
        dict: The company data if found, None otherwise
    """
    try:
        client = init_client()
        if not client:
            return None
            
        response = client.table('universe').select('*').eq('\"Quartr Id\"', quartrid).execute()
        if response.data and len(response.data) > 0:
            return response.data[0]
        return None
    except Exception as e:
        logger.error(f"Error fetching company by Quartr ID {quartrid}: {str(e)}")
        return None
