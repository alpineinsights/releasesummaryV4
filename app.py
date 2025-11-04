"""
Financial Insights Analysis Pipeline

This module implements an optimized document processing pipeline for financial analysis:

OPTIMIZATION: Transcript Processing Pipeline
===========================================
- Fast Path: Extract transcript text immediately for Gemini processing
- Slow Path: Generate transcript PDFs in background for sources list
- Synchronization: Wait for PDF generation only when needed for final response

Key Performance Improvements:
- Transcript text extraction decoupled from PDF generation
- Parallel processing of analysis and PDF creation
- Immediate Gemini processing without waiting for PDF uploads
- Background task management for non-blocking operations
"""

import pandas as pd
import os
import tempfile
import uuid
# import google.generativeai as genai # Old import
from google import genai # New SDK import
import time
import logging
from utils import QuartrAPI, AWSS3StorageHandler, TranscriptProcessor
import aiohttp
import asyncio
from typing import List, Dict, Tuple, Any, Optional
import json
import anthropic
import requests
from supabase_client import get_company_names, get_quartrid_by_name, get_all_companies
import io
import re
import threading
import concurrent.futures
import shutil # Import shutil for directory cleanup
import functools # Import functools
from google.genai import types # Import types for config objects

# Try to import PyMuPDF (fitz), but don't fail if it's not available
try:
    import fitz  # PyMuPDF
except ImportError:
    # Log warning instead of failing
    print("Warning: PyMuPDF (fitz) not installed. PDF generation functionality may be limited.")

from anthropic import Anthropic
from datetime import datetime, timezone
from logging_config import setup_logging # Assuming setup_logging exists
from logger import logger  # Import the configured logger
from urllib.parse import urlparse

# Configure logging
logger = logging.getLogger(__name__)

# Load credentials from environment variables (sanitize values)
def _get_clean_env(name: str) -> str:
    try:
        value = os.environ.get(name, "")
    except Exception:
        return ""
    if value is None:
        return ""
    return value.strip().strip('"').strip("'")

try:
    GEMINI_API_KEY = _get_clean_env("GEMINI_API_KEY")
    QUARTR_API_KEY = _get_clean_env("QUARTR_API_KEY")
    PERPLEXITY_API_KEY = _get_clean_env("PERPLEXITY_API_KEY")
    CLAUDE_API_KEY = _get_clean_env("CLAUDE_API_KEY")
except Exception as e:
    print(f"Warning: Error loading secrets from environment variables: {str(e)}.")
    GEMINI_API_KEY = ""
    QUARTR_API_KEY = ""
    PERPLEXITY_API_KEY = ""
    CLAUDE_API_KEY = ""

# Load company data from Supabase (non-cached version)
def load_company_data():
    companies = get_all_companies()
    if not companies:
        logger.error("Failed to load company data from Supabase.")
        return None
    return pd.DataFrame(companies)


# Initialize Gemini model
def initialize_gemini() -> Optional[genai.Client]:
    """Initializes and returns a Gemini API client instance."""
    if not GEMINI_API_KEY:
        logger.error("Gemini API key not found in environment variables")
        return None
    
    try:
        # Initialize the Gemini Client
        client = genai.Client(api_key=GEMINI_API_KEY)
        logger.info("Successfully initialized Gemini Client")
        return client
    except Exception as e:
        logger.error(f"Error initializing Gemini Client: {str(e)}")
        return None

# Initialize Claude client
def initialize_claude():
    if not CLAUDE_API_KEY:
        logger.error("Claude API key not found in environment variables")
        return None
    
    try:
        # Initialize the Claude client with only required parameters
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
        logger.info("Successfully initialized Claude Client")
        return client
    except Exception as e:
        logger.error(f"Error initializing Claude: {str(e)}")
        return None

# Extract valid JSON from Perplexity response
def extract_valid_json(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts and returns only the valid JSON part from a Perplexity response object.
    
    Parameters:
        response (dict): The full API response object.

    Returns:
        dict: The parsed JSON object extracted from the content.
    
    Raises:
        ValueError: If no valid JSON can be parsed from the content.
    """
    # Navigate to the 'content' field
    content = (
        response
        .get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    
    # Find the index of the closing </think> tag
    marker = "</think>"
    idx = content.rfind(marker)
    
    if idx == -1:
        # If marker not found, try parsing the entire content
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning("No </think> marker found and content is not valid JSON")
            # Return the raw content if it can't be parsed as JSON
            return {"content": content}
    
    # Extract the substring after the marker
    json_str = content[idx + len(marker):].strip()
    
    # Remove markdown code fence markers if present
    if json_str.startswith("```json"):
        json_str = json_str[len("```json"):].strip()
    if json_str.startswith("```"):
        json_str = json_str[3:].strip()
    if json_str.endswith("```"):
        json_str = json_str[:-3].strip()
    
    try:
        parsed_json = json.loads(json_str)
        return parsed_json
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse valid JSON from response content: {e}")
        # Return the raw content after </think> if it can't be parsed as JSON
        return {"content": json_str}

# Function to call Perplexity API
async def query_perplexity(query: str, company_name: str, conversation_context=None) -> Tuple[str, List[Dict]]:
    """Call Perplexity API with a Financial News Analyst prompt for the specified company
    
    Args:
        query: The user's query
        company_name: The name of the company
        conversation_context: Passed explicitly to avoid thread issues with st.session_state
    
    Returns:
        Tuple[str, List[Dict]]: The response content and a list of citation objects (citations likely won't be relevant with this prompt)
    """
    if not PERPLEXITY_API_KEY:
        logger.error("Perplexity API key not found")
        return "Error: Perplexity API key not found", []
    
    try:
        logger.info(f"Perplexity API: Starting request for news highlights about {company_name}")
        start_time = time.time()
        
        url = "https://api.perplexity.ai/chat/completions"
        
        # New Perplexity Prompt Structure
        system_prompt = "You are a Financial News Analyst."
        user_message = f"""Provide a concise summary of the key highlights and immediate takeaways from the latest publicly announced financial earnings release for {company_name}. Focus on information readily available in public news summaries and press releases immediately following the announcement.

Information to Extract:
1. Headline Numbers: Key reported metrics like Revenue and EPS. Mention if news sources highlight significant beats/misses vs. analyst expectations (report this neutrally, e.g., "Revenue reported as $X, noted by sources as above/below consensus estimates").
2. Management Commentary Highlights: Briefly list 2-3 key themes emphasized by management as reported in public summaries.
3. Independent qualitative comments: Briefly list 2-3 key topics that news sources highlighted from release.

Constraints:
* Keep the summary concise and factual, based on publicly reported information about the release.
* Maintain a neutral and objective tone.
* Do not include detailed financial breakdowns (like margin analysis, segment details unless a major headline).
* Do not include stock price movements or explicit analyst buy/sell ratings or price targets.
* Focus only on the specified earnings release.
* "Format large monetary amounts as follows:
For amounts ≥ 1 billion in any currency: display in billions with 1-2 decimal places (e.g., $638.0B, €45.2B, £12.5B, ¥1.2T)
For amounts ≥ 1 million but < 1 billion: display in millions with 1-2 decimal places (e.g., $108.0M, €25.7M, £8.3M)
For amounts < 1 million: display the full amount with appropriate currency symbol
Use 'T' for trillions when amounts exceed 1 trillion
Preserve the original currency symbol in all cases
Do NOT apply this formatting to per-share metrics (EPS, dividends per share), ratios, percentages, or other non-monetary values
Apply this formatting to: Net Sales, Revenue, Operating Income, Net Income, Total Assets, Market Cap, and other large monetary figures"
"""
        
        payload = {
            "model": "sonar-reasoning-pro", # Reverted to original model
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            "max_tokens": 4000, # Increased tokens as requested
            "temperature": 0.1, 
            # "web_search_options": {"search_context_size": "high"} # Maybe not needed/different setting for news focus?
        }
        
        headers = {
            "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Revert timeout as well since model is slower
        timeout = aiohttp.ClientTimeout(total=90) 
        
        # Use aiohttp to make the request asynchronously
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.info("Perplexity API: Sending request to API server for news highlights")
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Perplexity API returned error {response.status}: {error_text}")
                        return f"Error: Perplexity API returned status {response.status}", []
                    
                    logger.info("Perplexity API: Received response from server")
                    response_json = await response.json()
                    elapsed = time.time() - start_time
                    logger.info(f"Perplexity API: Response received in {elapsed:.2f} seconds")
                    
                    # Log the raw response for debugging
                    logger.info(f"Perplexity API raw response structure: {list(response_json.keys())}")
                    
                    # Extract citations if present (though less likely to be relevant/used)
                    citations = response_json.get("citations", [])
                    logger.info(f"Perplexity API: Found {len(citations)} citations (may not be used)")
                    
                    # Simplified extraction - just get the text content directly
                    if "choices" in response_json and len(response_json["choices"]) > 0:
                        if "message" in response_json["choices"][0] and "content" in response_json["choices"][0]["message"]:
                            content = response_json["choices"][0]["message"]["content"]
                            
                            # Remove potential </think> tags if the model includes them
                            if "</think>" in content:
                                content = content.split("</think>", 1)[1].strip()
                            
                            # Remove potential markdown fences if present
                            if content.startswith("```json"):
                                content = content[len("```json"):].strip()
                            if content.startswith("```"):
                                content = content[3:].strip()
                            if content.endswith("```"):
                                content = content[:-3].strip()
                            
                            return content, citations
                    
                    # Fallback if we couldn't extract the content using the above method
                    try:
                        # Attempt to parse as JSON first (less likely with this prompt)
                        parsed_content = json.loads(response_json['choices'][0]['message']['content']) # Assume structure if json is expected
                        if isinstance(parsed_content, dict) and 'content' in parsed_content:
                            return parsed_content['content'], citations
                        return str(parsed_content), citations # Return stringified dict if 'content' key missing
                    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
                        # If not JSON or structure mismatch, return the raw content string
                        logger.warning("Perplexity response was not JSON, returning raw content string.")
                        raw_content = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
                        if "</think>" in raw_content:
                                raw_content = raw_content.split("</think>", 1)[1].strip()
                        return raw_content, citations
        except asyncio.TimeoutError:
            logger.error("Perplexity API request timed out after 90 seconds") # Reverted timeout message
            return "Error: Perplexity API request timed out. Please try again later.", []
        except asyncio.CancelledError:
            logger.info("Perplexity API request was cancelled")
            raise  # Re-raise the CancelledError to allow proper cleanup
        
    except asyncio.CancelledError:
        logger.warning("Perplexity API task was cancelled")
        raise  # Re-raise to allow proper cleanup
    except Exception as e:
        logger.error(f"Error calling Perplexity API: {str(e)}")
        return f"Error calling Perplexity API: {str(e)}", []

# Function to call Claude with combined outputs
def query_claude(query: str, company_name: str, gemini_output: str, perplexity_output: str, conversation_context=None) -> str:
    """Call Claude API with combined Gemini and Perplexity outputs for final synthesis"""
    logger.info("Claude API: Starting synthesis process")
    start_time = time.time()
    
    client = initialize_claude()
    if not client:
        # Return a JSON string indicating the error
        return json.dumps({"status": "error", "message": "Error initializing Claude client"}, indent=2)

    try:
        # Define the static JSON structure part of the prompt separately
        json_structure_example = """Structure:

{{
  "status": "success",
  "data": {{
    "executiveSummary": {{
      "title": "Executive Summary",
      "content": "A single concise paragraph summarizing key financial highlights, performance drivers, and outlook. No bullet points or markdown."
    }},
    "profitAndLoss": {{
      "title": "Profit & Loss Analysis",
      "table": [
        {{
          "metric": "Key Metric 1 (e.g., Revenue)",
          "CurrentPeriodValue": "Value (e.g., $XX.XB)",
          "PriorPeriodValue": "Value (e.g., $YY.YB)",
          "PercentageChange": "Z%"
        }},
        {{
          "metric": "Key Metric 2 (e.g., Net Income)",
          "CurrentPeriodValue": "Value",
          "PriorPeriodValue": "Value",
          "PercentageChange": "P%"
        }}
        // Additional relevant P&L items as needed
      ]
    }},
    "segmentPerformance": {{
      "title": "Segment Performance",
      "bullets": [
        "Key finding or data point for Segment A.",
        "Key finding or data point for Segment B."
        // Add more bullets if multiple segments reported with distinct highlights
      ]
    }},
    "geographicPerformance": {{
      "title": "Geographic Performance",
      "bullets": [
        "Key finding or data point for Region X.",
        "Key finding or data point for Region Y."
        // Add more bullets if multiple regions reported with distinct highlights
      ]
    }},
    "cashFlowHighlights": {{
      "title": "Cash Flow & Balance Sheet Highlights",
      "bullets": [
        "Significant cash flow item (e.g., Operating Cash Flow, Free Cash Flow).",
        "Key balance sheet item (e.g., Net Debt, Share Repurchases)."
      ]
    }},
    "forwardOutlook": {{
      "title": "Forward-Looking Guidance / Outlook",
      "bullets": [
        "Summary of company guidance for key metrics (e.g., revenue growth).",
        "Outlook for margins or other financial indicators, if provided."
      ]
    }},
    "conferenceCallHighlights": {{
      "title": "Conference Call Analysis",
      "content": "One or more paragraphs summarizing key themes from the conference call Q&A, management commentary on specific topics, and areas of analyst focus. No bullet points or markdown."
    }},
    "sources": [
      {{
        "name": "[company_name]_[YYYYMMDD]_report.pdf",
        "url": "https://www.example.com/link/to/document1.pdf",
        "category": "Company data"
      }},
      {{
        "name": "[company_name]_[YYYYMMDD]_slides.pdf",
        "url": "https://www.example.com/link/to/document2.pdf",
        "category": "Company data"
      }},
      {{
        "name": "[company_name]_[YYYYMMDD]_transcript.pdf",
        "url": "https://www.example.com/link/to/document3.pdf",
        "category": "Company data"
      }}
      // Additional source documents if applicable
    ]
  }}
}}
"""
        # Create prompt for Claude (NEW JSON OUTPUT PROMPT)
        prompt = (
            f"""**Role:** You are a Senior Financial Analyst acting as a final review editor.

**Objective:** Synthesize a final, polished earnings summary for **{company_name}** regarding their latest release.

**CRITICAL FOCUS:** The entire summary MUST be about **{company_name}**. Do NOT under any circumstances generate content about a different company, regardless of perceived similarities in the input texts. Your focus is solely on **{company_name}**.

**Input Materials:** You will work with two pieces of input:
1. **Primary Analysis:** A detailed draft report based on the company's official financial documents (report, slides, transcript) for **{company_name}**. This is your foundational text.
2. **Supplemental Briefing:** A concise summary of highlights and key takeaways derived from public news sources about the earnings release for **{company_name}**.

**Task:** Review the 'Primary Analysis' for **{company_name}** and enhance it by carefully integrating relevant information *only* from the 'Supplemental Briefing' for **{company_name}**. The goal is a single, cohesive, finalized report for **{company_name}**.

---

**MANDATORY OUTPUT FORMAT:**

Return a single valid **JSON object** with the following structure.
❗ Do not wrap the output in backticks, quotes, or markdown code blocks.
❗ Do not escape quotes or line breaks.
❗ Do not insert comments or narrative explanations.

"""
            + json_structure_example # This line ensures the JSON structure is included
            + f"""

---

**Output Rules:**

- Omit any section completely if no content is available — do not include empty fields or placeholders.
- Bullet points must appear only inside `"bullets"` arrays as plain strings.
- Tables must be consistent JSON arrays of objects under `"table"`.
- Use plain text only — do not bold, italicize, or use markdown (e.g., `**`, `•`, etc.).
- Always return `"status": "success"` at the top level.
- Do not include N/A or "None" as values.

---

**Integration Guidelines:**

- **Primary Analysis is authoritative** — do not change financial figures from the **{company_name}** 'Primary Analysis'.
- Use content from the **Supplemental Briefing** only if it adds real strategic insight for **{company_name}**.
- DO NOT include:
  - Stock price movements
  - Analyst ratings or speculative statements
  - Attributions like "According to the briefing"
- Maintain a professional, neutral, and objective tone, focused on **{company_name}**.

---

**Final Output:**

Return only the clean JSON object as specified for **{company_name}**.
Do not include markdown, comments, narrative explanations, or any extra wrapping.
Format large monetary amounts as follows:
For amounts ≥ 1 billion in any currency: display in billions with 1-2 decimal places (e.g., $638.0B, €45.2B, £12.5B, ¥1.2T)
For amounts ≥ 1 million but < 1 billion: display in millions with 1-2 decimal places (e.g., $108.0M, €25.7M, £8.3M)
For amounts < 1 million: display the full amount with appropriate currency symbol
Use 'T' for trillions when amounts exceed 1 trillion
Preserve the original currency symbol in all cases
Do NOT apply this formatting to per-share metrics (EPS, dividends per share), ratios, percentages, or other non-monetary values
Apply this formatting to: Net Sales, Revenue, Operating Income, Net Income, Total Assets, Market Cap, and other large monetary figures"

---

--- START PRIMARY ANALYSIS ---
{gemini_output}
--- END PRIMARY ANALYSIS ---

--- START SUPPLEMENTAL BRIEFING ---
{perplexity_output}
--- END SUPPLEMENTAL BRIEFING ---
"""
        )

        logger.info(f"Claude API: Sending request with prompt length {len(prompt)} characters for company: {company_name}")
        api_start_time = time.time()
        
        # Call Claude API with the updated model name
        message = client.messages.create(
            model="claude-3-7-sonnet-20250219", # Reverted to Sonnet 3.7
            max_tokens=4096, # Max for Sonnet 3.5
            temperature=0.1, # Keep low temperature for factual synthesis
            system="You are a Senior Financial Analyst synthesizing an earnings report.", # Updated system prompt
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        api_time = time.time() - api_start_time
        logger.info(f"Claude API: Received response in {api_time:.2f} seconds")
        
        response_text = message.content[0].text
        total_time = time.time() - start_time
        logger.info(f"Claude API: Total processing time: {total_time:.2f} seconds")
        
        return response_text
        
    except Exception as e:
        logger.error(f"Error calling Claude API: {str(e)}")
        # Return a JSON string indicating the error
        return json.dumps({"status": "error", "message": f"Error calling Claude API: {str(e)}"}, indent=2)

# Function to process company documents
async def process_company_documents(company_id: str, company_name: str, storage_handler_instance: AWSS3StorageHandler, event_type: str = "all") -> Tuple[List[Dict], Optional[Dict]]:
    """Process documents from the single most recent company event.
    
    Returns:
        Tuple of (processed_files_list, transcript_text_dict)
        - processed_files_list: List of file info dicts for reports/slides 
        - transcript_text_dict: Dict with transcript text and metadata, or None
    """
    processed_files_output = []
    selected_files_details = [] # Store details of the selected documents from the latest event
    transcript_text_data = None  # Store extracted transcript text
    # found_types dictionary is no longer needed

    # Ensure storage_handler_instance is valid
    if not storage_handler_instance or not storage_handler_instance.s3_client:
        logger.error("Invalid or uninitialized storage_handler_instance passed to process_company_documents.")
        return [], None

    try:
        async with aiohttp.ClientSession() as session:
            # Initialize API and handlers
            quartr_api = QuartrAPI() # Assumes QuartrAPI is defined elsewhere (e.g., utils.py)
            storage_handler = storage_handler_instance # Use the passed-in instance
            transcript_processor = TranscriptProcessor() # Assumes defined elsewhere (e.g., utils.py)

            # ===== QUARTR API V3 INTEGRATION =====
            # Step 1: Get events from v3 API
            logger.info(f"[V3] Step 1: Fetching events for company: {company_name} (ID: {company_id})")
            events_data = await quartr_api.get_events_v3(company_id, session, limit=100)
            
            events = events_data.get('events', [])
            if not events:
                logger.warning(f"No events found for company: {company_name} (ID: {company_id})")
                return [], None

            logger.info(f"[V3] Retrieved {len(events)} events from v3 API")
            
            # Filter out future events (only keep events that have already occurred)
            now = datetime.now(timezone.utc)
            past_events = []
            for event in events:
                event_date_str = event.get('date', '')
                if event_date_str:
                    try:
                        # Parse the event date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
                        event_date = datetime.fromisoformat(event_date_str.replace('Z', '+00:00'))
                        if event_date <= now:
                            past_events.append(event)
                        else:
                            logger.debug(f"Skipping future event: {event.get('title')} ({event_date_str})")
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Could not parse event date '{event_date_str}': {e}")
                        # Include events with unparseable dates to be safe
                        past_events.append(event)
            
            logger.info(f"[V3] Filtered to {len(past_events)} past events (excluded {len(events) - len(past_events)} future events)")
            events = past_events
            
            # Sort events by date (descending) - ensures events[0] is the latest
            events.sort(key=lambda x: x.get('date', ''), reverse=True)

            # --- EVENT SELECTION LOGIC (same as before, works with v3 structure) --- 
            selected_event = None
            event_index = 0
            
            # Check if the latest event contains "AGM" in the title
            if events:
                latest_event = events[0]
                latest_event_title = latest_event.get('title', 'Unknown Event')  # v3 uses 'title'
                
                if 'AGM' in latest_event_title.upper():
                    logger.info(f"Latest event '{latest_event_title}' contains 'AGM' - looking for next available event")
                    # Use the next available event if it exists
                    if len(events) > 1:
                        selected_event = events[1]
                        event_index = 1
                        logger.info(f"Selected next available event at index {event_index}")
                    else:
                        logger.warning("No next event available after AGM event, proceeding with AGM event")
                        selected_event = latest_event
                        event_index = 0
                else:
                    # Use the latest event as normal
                    selected_event = latest_event
                    event_index = 0
            
            if not selected_event:
                logger.warning(f"No events available for processing for company: {company_name} (ID: {company_id})")
                return [], None
            
            # Extract event info (v3 uses 'date' and 'title')
            event_date = selected_event.get('date', '').split('T')[0]
            event_title = selected_event.get('title', 'Unknown Event')
            selected_event_id = selected_event.get('id')
            
            logger.info(f"Selected event for processing: {event_title} ({event_date}) [ID: {selected_event_id}] at index {event_index}")

            # Step 2: Fetch audio and documents for the selected event using v3 API
            logger.info(f"[V3] Step 2: Fetching audio and documents for event ID: {selected_event_id}")
            
            # Fetch audio and documents in parallel
            audio_task = quartr_api.get_audio_v3(company_id, [selected_event_id], session)
            documents_task = quartr_api.get_documents_v3(company_id, [selected_event_id], session)
            
            audio_data, documents_data = await asyncio.gather(audio_task, documents_task)
            
            logger.info(f"[V3] Retrieved {len(audio_data)} audio files and {len(documents_data)} documents")
            
            # Step 3: Map documents to the selected event
            event_map = quartr_api.map_documents_to_events([selected_event], audio_data, documents_data)
            
            if selected_event_id not in event_map:
                logger.warning(f"Selected event {selected_event_id} not found in mapped results")
                return [], None
            
            event_with_docs = event_map[selected_event_id]
            documents = event_with_docs.get('documents', {})
            
            logger.info(f"[V3] Mapped documents: report={bool(documents.get('report'))}, "
                       f"slides={bool(documents.get('slides'))}, "
                       f"transcript={bool(documents.get('transcript'))}, "
                       f"audio={bool(documents.get('audio'))}")
            
            # Build selected_files_details from the mapped documents
            # Process slides
            if documents.get('slides'):
                selected_files_details.append({
                    'url': documents['slides'],
                    'type': 'slides',
                    'event_date': event_date,
                    'event_title': event_title
                })
                logger.info(f"[V3] Found slides for event: {event_title} ({event_date})")
            
            # Process report
            if documents.get('report'):
                selected_files_details.append({
                    'url': documents['report'],
                    'type': 'report',
                    'event_date': event_date,
                    'event_title': event_title
                })
                logger.info(f"[V3] Found report for event: {event_title} ({event_date})")
            
            # Process transcript (URL only for now - will be processed later)
            transcript_url = documents.get('transcript')
            if transcript_url:
                selected_files_details.append({
                    'url': transcript_url,
                    'type': 'transcript',
                    'event_date': event_date,
                    'event_title': event_title,
                    'transcript_data_source': None  # v3 provides direct file URLs
                })
                logger.info(f"[V3] Found transcript for event: {event_title} ({event_date})")
            # --- END EVENT SELECTION AND DOCUMENT MAPPING ---

            logger.info(f"Selected {len(selected_files_details)} documents from the selected event.")

            # Now, process ONLY the selected documents (from the selected event)
            # Separate processing for files (reports/slides) vs transcript text
            for file_detail in selected_files_details:
                file_type = file_detail['type']
                # Handle cases where URL might be None (relevant for transcript processing)
                original_url = file_detail.get('url')
                event_date = file_detail['event_date']
                event_title = file_detail['event_title']

                try:
                    if file_type == 'slides' or file_type == 'report':
                        # Process reports and slides as files (unchanged logic)
                        if not original_url: # Should not happen based on logic above, but safe check
                             logger.warning(f"Skipping {file_type} for {event_title} due to missing URL.")
                             continue
                        async with session.get(original_url) as response:
                            if response.status == 200:
                                content_to_upload = await response.read()
                                original_filename = original_url.split('/')[-1]
                                if '?' in original_filename:
                                    original_filename = original_filename.split('?')[0]
                                s3_filename = storage_handler.create_filename(
                                    company_name, event_date, event_title, file_type, original_filename
                                )
                                content_type = response.headers.get('content-type', 'application/pdf')
                                
                                # Upload to S3 and generate presigned URL
                                success = await storage_handler.upload_file(
                                    content_to_upload, s3_filename, content_type
                                )
                                if success:
                                    presigned_url = storage_handler.get_presigned_url(s3_filename)
                                    if presigned_url:
                                        logger.info(f"Generated presigned URL for {s3_filename}")
                                        processed_files_output.append({
                                            'filename': s3_filename,
                                            'type': file_type,
                                            'event_date': event_date,
                                            'event_title': event_title,
                                            'url': presigned_url
                                        })
                                    else:
                                        logger.error(f"Failed to generate presigned URL for {s3_filename}")
                                else:
                                     logger.error(f"Failed to upload {file_type} ({s3_filename}) to S3.")
                            else:
                                logger.warning(f"Failed to download {file_type} from {original_url}, status: {response.status}")
                                continue # Skip this file

                    elif file_type == 'transcript':
                         # Fast text extraction for transcripts (NEW OPTIMIZED PATH)
                         logger.info(f"Starting fast transcript text extraction for {event_title}")
                         transcript_data_source = file_detail.get('transcript_data_source')
                         
                         # Extract text quickly without PDF generation
                         extracted_data = await extract_transcript_text_fast(
                             original_url, transcript_data_source, session
                         )
                         
                         if extracted_data and extracted_data.get('text'):
                             # Store transcript text data with metadata for Gemini processing
                             transcript_text_data = {
                                 'text': extracted_data['text'],
                                 'company_name': company_name,
                                 'event_title': event_title,
                                 'event_date': event_date,
                                 'source': extracted_data.get('source', 'unknown'),
                                 'original_url': original_url
                             }
                             logger.info(f"Successfully extracted transcript text (fast) for {event_title}, length: {len(extracted_data['text'])}")
                         else:
                             # Log based on original URL if available, otherwise just event title
                             url_for_log = original_url if original_url else "transcript data source"
                             logger.warning(f"Failed to extract transcript text (fast) for {event_title} from {url_for_log}")

                except Exception as e:
                    # Log based on original URL if available
                    url_for_log = original_url if original_url else "transcript data source"
                    logger.error(f"Error processing selected {file_type} for {event_title} (Source: {url_for_log}): {str(e)}", exc_info=True)

            logger.info(f"Finished processing. Uploaded {len(processed_files_output)} documents to S3 from the selected event.") # Updated log
            if transcript_text_data:
                logger.info(f"Successfully extracted transcript text for fast processing")
            return processed_files_output, transcript_text_data

    except Exception as e:
        logger.error(f"Error processing company documents: {str(e)}", exc_info=True)
        return [], None

# Function to download files from storage to temporary location (using presigned URLs)
async def download_files_from_s3(file_infos: List[Dict]) -> Tuple[List[str], Optional[str]]:
    """Download files from presigned URLs to temporary location and return local paths and temp dir path."""
    temp_dir = tempfile.mkdtemp()
    local_files = []
    download_tasks = []
    
    logger.info(f"Attempting to download {len(file_infos)} files from presigned URLs...")
    
    async with aiohttp.ClientSession() as session:
        for file_info in file_infos:
            presigned_url = file_info.get('url')
            # Use S3 filename (key) stored previously for local naming
            s3_filename = file_info.get('filename') 
            if not presigned_url or not s3_filename:
                logger.warning(f"Skipping file download due to missing URL or filename in info: {file_info}")
                continue

            try:
                # Create a safe local filename based on the S3 key
                safe_local_filename = s3_filename.replace('/', '_').replace('\\', '_')[:200]
                local_path = os.path.join(temp_dir, safe_local_filename)
                
                logger.info(f"Scheduling download from presigned URL for {s3_filename} to {local_path}")
                # Create a task to download and save the file
                download_tasks.append(download_and_save(session, presigned_url, local_path))

            except Exception as e:
                logger.error(f"Error preparing download for S3 key {s3_filename} from URL {presigned_url}: {str(e)}")
        
        # Run download tasks concurrently
        download_results = await asyncio.gather(*download_tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(download_results):
            s3_key_for_log = file_infos[i].get('filename', 'unknown') # Get corresponding key for logging
            if isinstance(result, Exception):
                logger.error(f"Error downloading file {s3_key_for_log} (index {i}): {str(result)}", exc_info=False) # Maybe avoid traceback spam
            elif isinstance(result, str) and os.path.exists(result) and os.path.getsize(result) > 0: # download_and_save returns path on success
                local_files.append(result)
                logger.info(f"Successfully downloaded {s3_key_for_log} to {result}")
            else: # Download failed (returned None or empty file)
                logger.error(f"Failed to download or save file {s3_key_for_log} (index {i}). URL: {file_infos[i].get('url')}")

    if not local_files:
         logger.warning(f"Failed to download any files. Cleaning up temp directory: {temp_dir}")
         try:
             shutil.rmtree(temp_dir)
         except Exception as cleanup_err:
             logger.error(f"Error cleaning up empty temp directory {temp_dir}: {cleanup_err}")
         return [], None # Return empty list and None for dir path

    logger.info(f"Downloaded {len(local_files)} files to temporary directory: {temp_dir}")
    return local_files, temp_dir

# Helper for downloading a single file from URL
async def download_and_save(session: aiohttp.ClientSession, url: str, local_path: str) -> Optional[str]:
    """Downloads content from URL and saves to local_path. Returns path on success, None on failure."""
    try:
        async with session.get(url) as response:
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            content = await response.read()
            # Ensure directory exists (though created by download_files_from_s3)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(content)
            # Verify file was written successfully
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                 return local_path
            else:
                logger.error(f"Failed to write or file empty after download: {local_path}")
                return None
    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP error downloading {url}: {e.status} {e.message}")
        return None
    except Exception as e:
        logger.error(f"Error downloading {url} to {local_path}: {str(e)}", exc_info=True)
        return None

# Function to query Gemini with file context using File API (Async Version)
async def query_gemini(client: genai.Client, query: str, file_paths: List[str], company_name: str, conversation_context: List = None, transcript_text_data: Optional[Dict] = None) -> str:
    """Query Gemini model with context from files using the File API (Async Version)
    
    Args:
        client: Gemini API client
        query: User's query
        file_paths: List of local file paths for reports/slides
        company_name: Name of the company being analyzed
        conversation_context: Previous conversation history
        transcript_text_data: Optional dict containing transcript text and metadata for direct inclusion
    
    Returns:
        Analysis response text from Gemini
    """
    logger.info("--- ENTERING async query_gemini function ---")
    
    uploaded_files_for_cleanup = []
    result = "Error: Default error message if result is not assigned."
    try:
        # --- Process ALL provided files --- 
        files_to_process = file_paths # Use all files passed in
        logger.info(f"Gemini API: Processing {len(files_to_process)} files.")
        # logger.info(f"Gemini API: Files selected for processing: {files_to_process}") # Can be verbose

        logger.info(f"Gemini API: Starting analysis with {len(files_to_process)} documents using File API (async)")
        start_time = time.time()
        
        if not client:
            logger.error("Gemini client is not initialized")
            result = "Error: Gemini client not initialized"
            logger.info(f"--- EXITING async query_gemini function (Client None) ---")
            return result
        
        # Build conversation history
        conversation_history = ""
        if conversation_context:
            conversation_history = "\n\nPREVIOUS CONVERSATION CONTEXT:\n"
            for entry in conversation_context:
                conversation_history += f"Question: {entry['query']}\n"
                conversation_history += f"Answer: {entry['summary']}\n\n"

        # Build transcript text section if available
        transcript_text_section = ""
        if transcript_text_data and transcript_text_data.get('text'):
            transcript_text = transcript_text_data['text']
            event_title = transcript_text_data.get('event_title', 'Unknown Event')
            event_date = transcript_text_data.get('event_date', 'Unknown Date')
            transcript_text_section = f"""

CONFERENCE CALL TRANSCRIPT:
Event: {event_title}
Date: {event_date}

{transcript_text}

--- END TRANSCRIPT ---
"""
            logger.info(f"Including transcript text directly in prompt, length: {len(transcript_text)}")
        else:
            logger.info("No transcript text data available for direct inclusion")

        # Upload files asynchronously
        loop = asyncio.get_running_loop()
        upload_tasks = []
        for file_path in files_to_process:
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                logger.warning(f"Skipping invalid file: {file_path}")
                continue
            logger.info(f"Gemini API: Creating upload task for: {file_path}")
            upload_func_with_args = functools.partial(
                client.files.upload, 
                file=file_path
            )
            upload_tasks.append(loop.run_in_executor(None, upload_func_with_args))

        # Run upload tasks concurrently
        logger.info(f"Gemini API: Awaiting {len(upload_tasks)} file uploads...")
        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        # --- Process results and implement explicit polling --- 
        active_files = []
        polling_tasks = [] # We might not need this list if polling inline
        files_being_polled = [] # Keep track of files needing polling

        # Keep track of original paths corresponding to upload_results
        paths_for_results = [fp for fp in files_to_process if os.path.exists(fp) and os.path.getsize(fp) > 0]
        
        for i, res in enumerate(upload_results):
            original_path = paths_for_results[i] if i < len(paths_for_results) else "unknown_path"
            if isinstance(res, Exception):
                logger.error(f"Error during initial upload task for {original_path}: {res}", exc_info=False) # Avoid traceback spam maybe?
                continue # Skip this file
            elif res: # Successfully got a File object back
                gemini_file = res
                uploaded_files_for_cleanup.append(gemini_file) # Add for cleanup regardless of state
                logger.info(f"Successfully initiated upload for {original_path} as {gemini_file.uri} - Initial State: {gemini_file.state.name}")
                if gemini_file.state.name == "ACTIVE":
                     logger.info(f"File {gemini_file.name} ({original_path}) is already ACTIVE.")
                     active_files.append(gemini_file)
                elif gemini_file.state.name == "PROCESSING":
                     logger.info(f"File {gemini_file.name} ({original_path}) is PROCESSING. Will poll.")
                     files_being_polled.append(gemini_file)
                else: # FAILED or unspecified state from initial upload response
                     logger.error(f"File {gemini_file.name} ({original_path}) has unexpected initial state: {gemini_file.state.name}.")
                     # Optionally try fetching details, but primarily mark as failed for this run
                     # Consider not adding to active_files

        # --- Explicit Polling Loop --- 
        if files_being_polled:
            logger.info(f"Starting explicit polling for {len(files_being_polled)} files...")
            polling_start_time = time.time()
            max_polling_time = 300 # 5 minutes total polling timeout
            poll_interval = 5 # Check every 5 seconds
            
            while files_being_polled and (time.time() - polling_start_time) < max_polling_time:
                logger.info(f"Polling check: {len(files_being_polled)} files remaining.")
                still_polling = [] 
                for file_to_poll in files_being_polled:
                    try:
                        # Run client.files.get in executor to avoid blocking async loop
                        get_func_with_args = functools.partial(client.files.get, name=file_to_poll.name)
                        polled_file = await loop.run_in_executor(None, get_func_with_args)
                        
                        if polled_file.state.name == "ACTIVE":
                            logger.info(f"Polling successful for file {polled_file.name}. State: ACTIVE.")
                            active_files.append(polled_file)
                            # Do not add back to still_polling
                        elif polled_file.state.name == "FAILED":
                            logger.error(f"Polling detected file {polled_file.name} FAILED processing.")
                            # Do not add back to still_polling
                        elif polled_file.state.name == "PROCESSING":
                            logger.debug(f"File {polled_file.name} still PROCESSING...")
                            still_polling.append(file_to_poll) # Keep polling this one
                        else:
                            logger.warning(f"File {polled_file.name} has unexpected state during polling: {polled_file.state.name}")
                            still_polling.append(file_to_poll) # Keep polling uncertain states?
                    except Exception as poll_err:
                        logger.error(f"Error polling file {file_to_poll.name}: {poll_err}")
                        # Decide if we should keep trying or give up on this file
                        # For now, let's keep polling it unless it's clearly failed.
                        still_polling.append(file_to_poll)
                
                files_being_polled = still_polling
                if files_being_polled:
                    await asyncio.sleep(poll_interval)
            
            # After loop, check if any files timed out
            if files_being_polled:
                logger.error(f"Polling timed out for {len(files_being_polled)} files after {max_polling_time} seconds.")
                for timed_out_file in files_being_polled:
                     logger.error(f" - Timed out waiting for: {timed_out_file.name}")
        
        # --- End Polling --- 

        uploaded_file_objects = active_files
        if not uploaded_file_objects:
            logger.warning("Gemini context file processing resulted in zero usable ACTIVE files.")
            result = "Error: No documents could be processed successfully for context. Check logs."
            logger.info(f"--- EXITING async query_gemini function (No Active Files) ---")
            return result

        # --- Prepare contents list (Reverting to flat list structure like legacy) --- 
        contents = list(uploaded_file_objects) # Start with list of File objects
        
        # NEW GEMINI PROMPT (Keep the detailed one)
        prompt = f"""Role: You are a Senior Financial Analyst.

Objective: Generate a comprehensive, neutral, and objective summary of the latest release for the specified publicly traded company: {company_name}, covering the latest fiscal period. This summary is intended for a professional investor audience.

Input Materials: You will be provided with the following documents pertaining to this specific earnings release.

Formatting Constraints (Strict):
* Your output must begin immediately with the structured report (starting with **Executive Summary**) and contain **no preamble, intro, or setup**.
* Do **not** include any sentences like "I will now summarize…" or "Based on the documents provided…"
* Your role is not to explain your task — just output the final structured report as if it were being published in a professional investor briefing.
* Do not include meta-comments or acknowledgments. Output only the factual report in the required structure.

Deliverable Sections:
1. Executive Summary: Begin with a brief paragraph summarizing the key highlights of the release – overall performance, significant beats/misses (if mentioned relative to company's own outlook or prior periods), and major themes.
2. Profit & Loss (P&L) Analysis:
Detail the key P&L items sequentially, only showing items if they are available.
Example (for non-financial companies): 
Revenue / Sales
Gross Profit / Gross Margin (%)
Operating Expenses (briefly, if significant changes)
EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization) / EBITDA Margin (%)
EBIT (Earnings Before Interest and Taxes) / Operating Profit / EBIT Margin (%)
Net Income / Net Earnings (Attributable to shareholders)
Earnings Per Share (EPS) - Specify basic and diluted if available.
For each item, report the value for the current period.
Include period-over-period variations (e.g., Year-over-Year (YoY), Quarter-over-Quarter (QoQ) if relevant). Clearly label the comparison period.
Crucially: Distinguish between 'Reported' (as published) figures and 'Adjusted' / 'Organic' / 'Like-for-Like' (LFL) / 'Constant Currency' figures whenever the company provides them. Clearly state the basis for any adjustments (e.g., "Adjusted Net Income excludes restructuring costs").
Present this P&L data in a clear tabular format comparing the current period to the relevant prior period(s), including both absolute values and percentage changes. Include separate columns or rows for Reported vs. Adjusted/Organic figures if applicable.
3. Segment Performance (If Applicable): If the company reports results by business segment or geographic region, briefly summarize the performance (e.g., revenue growth, profitability trends) of the key segments. Highlight any notable divergences.
4. Cash Flow & Balance Sheet Highlights: Briefly summarize key movements and metrics, such as:
Operating Cash Flow (OCF)
Capital Expenditures (CapEx)
Free Cash Flow (FCF) - Specify the company's definition if provided.
Net Debt position and evolution.
Significant changes in working capital.
5. Forward-Looking Guidance / Outlook: Summarize the company's guidance for future periods (e.g., next quarter, full year) as provided in the release materials. Detail the specific metrics guided (e.g., Revenue, Margin, EPS) and the ranges given. Note any changes compared to previous guidance.
6. Conference Call Analysis:
Key Discussion Topics: Identify the 3-5 main themes or questions raised repeatedly by analysts during the Q&A.
Positive/Reassuring Points: Highlight specific comments or data points from the call that were likely perceived positively by analysts taking part in the call (e.g., strong order book, successful integration, confident outlook on specific issues).
Areas of Concern/Scrutiny: Identify topics or questions where management faced significant scrutiny, seemed less confident, or where underlying concerns remain (e.g., competitive pressure, margin headwinds, execution risks). Provide detailed context.

Constraints & Tone:
If relevant data is not provided to structure a comment in any of the above-mentioned sections, do not show the section and do not mention that the data was not available
Maintain a strictly professional, neutral, objective, and balanced tone throughout the summary.
Avoid any laudatory language, hype, or overly critical phrasing. Stick to factual reporting and analysis based only on the documents provided.
Do not include personal opinions or predictions beyond summarizing the company's own statements/guidance.
Exclude commentary on Environmental, Social, and Governance (ESG) factors unless they were presented in the earnings materials as having a direct, material financial impact or outlook implication within this specific reporting period or guidance.
Deliverable Format: Present the summary in a well-structured format using clear headings for each section outlined above (starting with section 1, Executive Summary). Use tables for financial data comparison as specified.
Format large monetary amounts as follows:
For amounts ≥ 1 billion in any currency: display in billions with 1-2 decimal places (e.g., $638.0B, €45.2B, £12.5B, ¥1.2T)
For amounts ≥ 1 million but < 1 billion: display in millions with 1-2 decimal places (e.g., $108.0M, €25.7M, £8.3M)
For amounts < 1 million: display the full amount with appropriate currency symbol
Use 'T' for trillions when amounts exceed 1 trillion
Preserve the original currency symbol in all cases
Do NOT apply this formatting to per-share metrics (EPS, dividends per share), ratios, percentages, or other non-monetary values
Apply this formatting to: Net Sales, Revenue, Operating Income, Net Income, Total Assets, Market Cap, and other large monetary figures"

User's Original Query (for context if needed, but prioritize the objective above): '{query}'
{conversation_history}{transcript_text_section}"""
        contents.append(prompt) # Append the prompt string directly to the list
        
        logger.info(f"Gemini API: Preparing to call generate_content with {len(uploaded_file_objects)} ACTIVE file(s) (async)")
        api_start_time = time.time()
        # Define safety settings list
        safety_settings_list = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ]
        # Create config dictionary including safety settings AND disabling AFC
        generation_config_dict = {
            "temperature": 0.1,
            "max_output_tokens": 8192,
            "safety_settings": safety_settings_list,
            # Explicitly disable Automatic Function Calling
            "automatic_function_calling": types.AutomaticFunctionCallingConfig(disable=True)
        }
        
        logger.debug("Gemini API: Calling generate_content via loop.run_in_executor with partial")
        generate_func_with_args = functools.partial(
            client.models.generate_content,
            model='gemini-2.0-flash', # Change model name here
            contents=contents, # Pass the flat list directly
            config=generation_config_dict # Pass the combined config
        )
        response = await asyncio.wait_for(
            loop.run_in_executor(None, generate_func_with_args), 
            timeout=600.0
        )
        logger.debug("Gemini API: executor call for generate_content completed within timeout.")
        api_time = time.time() - api_start_time
        logger.info(f"Gemini API: Received response in {api_time:.2f} seconds")
        total_time = time.time() - start_time
        logger.info(f"Gemini API: Total async processing time: {total_time:.2f} seconds")
        
        # --- Process Response (Updated Check) --- 
        # Check if response.text exists and is non-empty
        if not response or not response.text:
            logger.warning("Gemini response was empty or None.")
            # Add more detailed checks if needed later based on response object structure
            # E.g., check response.prompt_feedback if it exists
            prompt_feedback_info = ""
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                prompt_feedback_info = f" Prompt Feedback: {response.prompt_feedback}"
                if response.prompt_feedback.block_reason:
                    block_reason = response.prompt_feedback.block_reason.name
                    logger.warning(f"Content potentially blocked due to: {block_reason}")
                    result = f"Error: The request may have been blocked by safety filters ({block_reason}).{prompt_feedback_info}"
                    logger.info(f"--- EXITING async query_gemini function (Blocked Response?) ---")
                    return result

            result = f"Error: Received an empty response from Gemini.{prompt_feedback_info}"
            logger.info(f"--- EXITING async query_gemini function (Empty Response?) ---")
            return result
        
        logger.info("Gemini API: Successfully generated content (async).")
        result = response.text
        logger.info(f"--- EXITING async query_gemini function (Success) ---")
        return result

    except Exception as e:
        logger.error(f"Critical error within async query_gemini function: {str(e)}", exc_info=True)
        # Ensure 'result' is assigned the error message in the except block
        result = f"An error occurred during the Gemini analysis process: {str(e)}"
        logger.info(f"--- EXITING async query_gemini function (Exception) ---")
        return result # Return the error string

    finally:
        # 'result' should always be defined by the time finally block is reached
        # (either from try block success or except block assignment)
        # Added a check just in case, though it shouldn't be necessary with the fix above.
        if 'result' not in locals():
             result = "Error: Unknown state in query_gemini finally block."
             logger.error("Variable 'result' was not defined before finally block in query_gemini!")
        logger.info(f"--- Reached finally block in async query_gemini. Final result (start): {result[:100]}...")
        # Cleanup Uploaded Files (Async)
        if uploaded_files_for_cleanup:
            logger.info(f"Starting cleanup of {len(uploaded_files_for_cleanup)} uploaded Gemini files (async finally)...")
            cleanup_start_time = time.time()
            delete_tasks = []
            for f in uploaded_files_for_cleanup:
                logger.info(f"Creating delete task for file: {f.name}")
                # Use functools.partial for delete, remove request_options
                delete_func_with_args = functools.partial(
                    client.files.delete, 
                    name=f.name
                )
                delete_tasks.append(loop.run_in_executor(None, delete_func_with_args))
            
            # Await all delete tasks concurrently
            delete_results = await asyncio.gather(*delete_tasks, return_exceptions=True)
            successful_deletes = 0
            for i, del_res in enumerate(delete_results):
                file_name_to_delete = uploaded_files_for_cleanup[i].name if i < len(uploaded_files_for_cleanup) else "unknown"
                if isinstance(del_res, Exception):
                     logger.error(f"Error deleting temporary file {file_name_to_delete}: {del_res}")
                else:
                     logger.info(f"Delete task completed successfully for file {file_name_to_delete}")
                     successful_deletes += 1

            cleanup_duration = time.time() - cleanup_start_time
            logger.info(f"Gemini file cleanup finished in {cleanup_duration:.2f} seconds. Successful deletes: {successful_deletes}/{len(uploaded_files_for_cleanup)}")
        logger.info(f"--- EXITING async query_gemini function (Finally Block Complete) ---")

# Add new fast text extraction function after the existing functions
async def extract_transcript_text_fast(transcript_url: Optional[str], transcripts_data: Optional[Dict], session: aiohttp.ClientSession) -> Optional[Dict]:
    """Fast transcript text extraction without PDF generation.
    
    Supports both v1 and v3 Quartr API formats.
    
    Returns:
        Dict with 'text' and metadata, or None if extraction fails
    """
    try:
        # Determine the best raw transcript URL to fetch
        raw_transcript_url = None

        # V3 API SUPPORT: If transcript_url is provided and looks like a direct file URL, use it
        if transcript_url and ('files.quartr.com' in transcript_url or transcript_url.endswith('.json')):
            raw_transcript_url = transcript_url
            logger.info("[V3] Using direct transcript URL from v3 API for fast extraction.")
        
        # V1 API SUPPORT: Prioritize URLs from the transcripts dictionary if provided
        elif transcripts_data: # Check if the dictionary exists (v1 format)
             # Check primary transcriptUrl within the dict first (less common in practice)
             if 'transcriptUrl' in transcripts_data and transcripts_data['transcriptUrl']:
                 raw_transcript_url = transcripts_data['transcriptUrl']
                 logger.info("[V1] Using transcriptUrl from transcripts dict for fast extraction.")
             # Check finishedLiveTranscriptUrl as the main target
             elif 'liveTranscripts' in transcripts_data and isinstance(transcripts_data.get('liveTranscripts'), dict) and \
                  'finishedLiveTranscriptUrl' in transcripts_data['liveTranscripts'] and transcripts_data['liveTranscripts']['finishedLiveTranscriptUrl']:
                 raw_transcript_url = transcripts_data['liveTranscripts']['finishedLiveTranscriptUrl']
                 logger.info("[V1] Using finishedLiveTranscriptUrl from transcripts dict for fast extraction.")
             # Check liveTranscriptUrl as a fallback if finished is missing
             elif 'liveTranscripts' in transcripts_data and isinstance(transcripts_data.get('liveTranscripts'), dict) and \
                  'liveTranscriptUrl' in transcripts_data['liveTranscripts'] and transcripts_data['liveTranscripts']['liveTranscriptUrl']:
                 raw_transcript_url = transcripts_data['liveTranscripts']['liveTranscriptUrl']
                 logger.info("[V1] Using liveTranscriptUrl from transcripts dict as fallback for fast extraction.")

        # 2. If no URL found from dict, check the primary transcript_url argument (v1 app URLs)
        if not raw_transcript_url and transcript_url:
            # If it's an app URL, attempt to resolve via API
            if 'app.quartr.com' in transcript_url:
                logger.info("No raw URL in dict, attempting API lookup for app URL for fast extraction.")
                try:
                    document_id = transcript_url.split('/')[-2]
                    if document_id.isdigit():
                        api_lookup_url = f"https://api.quartr.com/public/v1/transcripts/document/{document_id}"
                        headers = {"X-Api-Key": QUARTR_API_KEY}
                        logger.info(f"Attempting API lookup for fast extraction: {api_lookup_url}")
                        async with session.get(api_lookup_url, headers=headers) as response:
                            if response.status == 200:
                                transcript_api_data = await response.json()
                                if transcript_api_data and 'transcript' in transcript_api_data:
                                    text = transcript_api_data['transcript'].get('text', '')
                                    if text:
                                        formatted_text = TranscriptProcessor.format_transcript_text(text)
                                        logger.info(f"Successfully extracted transcript text via API lookup (fast), length: {len(formatted_text)}")
                                        return {
                                            'text': formatted_text,
                                            'source': 'api_lookup'
                                        }
                                logger.warning(f"API lookup successful but no transcript text found for {api_lookup_url}")
                            else:
                                response_text = await response.text()
                                logger.error(f"API lookup for transcript failed: Status {response.status}, Response: {response_text}")
                    else:
                         logger.warning(f"Could not extract valid document ID from app URL: {transcript_url}")
                except IndexError:
                     logger.warning(f"Could not parse document ID from app URL path: {transcript_url}")
                except Exception as api_err:
                     logger.error(f"Error during transcript API lookup for {transcript_url}: {api_err}")
                # If API lookup fails or URL format is wrong, fall through
            else:
                # If primary URL is not app URL and not found in dict, use it directly
                raw_transcript_url = transcript_url
                logger.info("Using primary transcript_url directly for fast extraction.")

        # 3. Fetch and process from the determined raw_transcript_url (if any)
        if raw_transcript_url:
            logger.info(f"Fetching transcript from determined URL for fast extraction: {raw_transcript_url}")
            try:
                headers = {"X-Api-Key": QUARTR_API_KEY} if 'api.quartr.com' in raw_transcript_url else {}
                async with session.get(raw_transcript_url, headers=headers) as response:
                    if response.status == 200:
                        try:
                            # Assume JSON/JSONL first
                            transcript_data = await response.json()
                            text = None
                            if isinstance(transcript_data, dict):
                                # Handle different possible JSON structures
                                if 'transcript' in transcript_data and isinstance(transcript_data['transcript'], dict):
                                    text = transcript_data['transcript'].get('text', '')
                                elif 'text' in transcript_data: # Simpler structure
                                    text = transcript_data['text']
                            
                            if text:
                                formatted_text = TranscriptProcessor.format_transcript_text(text)
                                logger.info(f"Successfully extracted transcript text (fast), length: {len(formatted_text)}")
                                return {
                                    'text': formatted_text,
                                    'source': 'json_url'
                                }
                        except json.JSONDecodeError:
                            # Try as plain text
                            text = await response.text()
                            if text:
                                formatted_text = TranscriptProcessor.format_transcript_text(text)
                                logger.info(f"Successfully extracted transcript as plain text (fast), length: {len(formatted_text)}")
                                return {
                                    'text': formatted_text,
                                    'source': 'text_url'
                                }
                    else:
                        logger.error(f"Failed to fetch transcript from {raw_transcript_url}, status: {response.status}")
            except Exception as e:
                logger.error(f"Error fetching transcript from {raw_transcript_url}: {e}")

        logger.warning("Fast transcript extraction failed - no valid source found")
        return None

    except Exception as e:
        logger.error(f"Error in fast transcript extraction: {e}")
        return None

# Add background PDF generation function
async def generate_transcript_pdf_background(transcript_text: str, company_name: str, event_title: str, event_date: str, 
                                           storage_handler: AWSS3StorageHandler) -> Optional[Dict]:
    """Generate transcript PDF in background and upload to S3.
    
    Returns:
        Dict with 'filename', 'type', 'event_date', 'event_title', 'url' or None if failed
    """
    try:
        loop = asyncio.get_running_loop()
        
        # Generate PDF in executor to avoid blocking
        pdf_content = await loop.run_in_executor(
            None, 
            TranscriptProcessor.create_pdf, 
            company_name, event_title, event_date, transcript_text
        )
        
        if pdf_content:
            # Create filename
            s3_filename = storage_handler.create_filename(
                company_name, event_date, event_title, 'transcript', 'transcript.pdf'
            )
            
            # Upload to S3
            success = await storage_handler.upload_file(
                pdf_content, s3_filename, 'application/pdf'
            )
            
            if success:
                presigned_url = storage_handler.get_presigned_url(s3_filename)
                if presigned_url:
                    logger.info(f"Successfully generated transcript PDF in background: {s3_filename}")
                    return {
                        'filename': s3_filename,
                        'type': 'transcript',
                        'event_date': event_date,
                        'event_title': event_title,
                        'url': presigned_url
                    }
                else:
                    logger.error(f"Failed to generate presigned URL for background transcript PDF: {s3_filename}")
            else:
                logger.error(f"Failed to upload background transcript PDF to S3: {s3_filename}")
        else:
            logger.error("Failed to generate PDF content in background")
        
        return None
        
    except Exception as e:
        logger.error(f"Error generating transcript PDF in background: {e}")
        return None

# --- Main execution logic --- 
async def run_analysis(company_name: str, query: str, conversation_context: List = None):
    """Runs the full analysis pipeline (now async)"""
    logger.info(f"--- ENTERING run_analysis for {company_name} ---")
    if conversation_context is None: conversation_context = []

    gemini_client = initialize_gemini()
    claude_client = initialize_claude() # Initialize Claude

    # Instantiate AWSS3StorageHandler here
    storage_handler = AWSS3StorageHandler()
    if not storage_handler.s3_client: # Check if S3 client initialized successfully
        logger.critical("S3 storage handler initialization failed")
        return "Error: Failed to initialize S3 storage handler."

    if not gemini_client: # Simplistic check, add claude_client if needed
        logger.critical("Gemini client initialization failed")
        return "Error: Failed to initialize AI clients."

    logger.info(f"Starting analysis pipeline for {company_name}")

    quartr_id = get_quartrid_by_name(company_name)
    if not quartr_id: return f"Error: Company '{company_name}' not found or missing Quartr ID."
    logger.info(f"Found Quartr ID: {quartr_id}")

    # Initialize results/placeholders
    gemini_output = "Error: Gemini analysis did not run or failed."
    claude_response = "Error: Claude synthesis did not run or failed."
    final_response = "Error: Analysis could not be completed."
    perplexity_output = "Error: Perplexity task did not complete."
    perplexity_citations = []
    processed_files_info = []
    transcript_text_data = None
    local_files = []
    temp_download_dir = None
    perplexity_task = None
    transcript_pdf_task = None
    # storage_handler is already initialized above

    try:
        # --- Start Perplexity Concurrently --- 
        logger.debug("Creating Perplexity task...")
        perplexity_task = asyncio.create_task(
            query_perplexity(query, company_name, conversation_context)
        )
        logger.info("Started Perplexity task concurrently.")

        # 2. Fetch and Process Documents 
        logger.info("Processing company documents...")
        processed_files_info, transcript_text_data = await process_company_documents(quartr_id, company_name, storage_handler_instance=storage_handler, event_type="all")
        logger.info(f"Document processing completed: {len(processed_files_info)} files, transcript: {'Yes' if transcript_text_data else 'No'}")
        
        # Start background PDF generation for transcript if text was extracted
        transcript_pdf_task = None
        if transcript_text_data and transcript_text_data.get('text'):
            logger.info("Starting background transcript PDF generation task")
            transcript_pdf_task = asyncio.create_task(
                generate_transcript_pdf_background(
                    transcript_text_data['text'],
                    transcript_text_data['company_name'],
                    transcript_text_data['event_title'],
                    transcript_text_data['event_date'],
                    storage_handler
                )
            )
        
        if not processed_files_info and not transcript_text_data: 
             logger.error(f"No documents found for {company_name}")
             if perplexity_task and not perplexity_task.done(): 
                 perplexity_task.cancel()
             return "Error: No documents found for this company."

        # 3. Download Documents from S3 (using presigned URLs from processed_files_info)
        local_files = []
        temp_download_dir = None
        if processed_files_info:
            logger.info("Downloading documents from S3...")
            local_files, temp_download_dir = await download_files_from_s3(processed_files_info)
            logger.info(f"Downloaded {len(local_files)} files from S3")
        else:
            logger.info("Skipping S3 download (transcript-only processing)")
            
        if not local_files and not transcript_text_data:
             logger.error("Document download failed and no transcript available")
             if perplexity_task: perplexity_task.cancel()
             if transcript_pdf_task: transcript_pdf_task.cancel()
             return "Error: Failed to download documents from storage and no transcript available."
        
        # 4. Run Gemini Analysis (Awaiting the async version)
        logger.info("Starting Gemini document analysis...")
        gemini_start = time.time()
        gemini_output = await query_gemini(gemini_client, query, local_files, company_name, conversation_context, transcript_text_data)
        gemini_duration = time.time() - gemini_start
        logger.info(f"Gemini analysis completed in {gemini_duration:.2f} seconds")

        # --- Wait for Perplexity Task --- 
        logger.info("Waiting for Perplexity task to complete...")
        if perplexity_task:
            try:
                perplexity_output, perplexity_citations = await asyncio.wait_for(perplexity_task, timeout=95.0)
                logger.info(f"Perplexity task completed. Citations: {len(perplexity_citations)}")
            except asyncio.TimeoutError:
                 logger.error("Perplexity task timed out externally (wait_for).", exc_info=False)
                 perplexity_output = "Error: Perplexity task timed out."
            except asyncio.CancelledError:
                 logger.warning("Perplexity task was cancelled.")
                 perplexity_output = "Error: Perplexity task was cancelled."
            except Exception as e_perp:
                 logger.error(f"Error awaiting Perplexity task: {e_perp}", exc_info=True)
                 perplexity_output = f"Error: Perplexity task failed: {e_perp}"
        else:
            logger.warning("Perplexity task missing?")
            perplexity_output = "Error: Perplexity task missing."

        # Error handling before Synthesis
        if gemini_output.startswith("Error") and perplexity_output.startswith("Error"):
            logger.error("Both Gemini and Perplexity failed.")
            # Cancel transcript PDF task if still running
            if transcript_pdf_task and not transcript_pdf_task.done():
                logger.info("Cancelling transcript PDF task due to upstream failures")
                transcript_pdf_task.cancel()
            
            # Prepare sources even if both upstream processes failed, to include in the error response
            source_list_json_bp_failed = []
            has_doc_sources_bp_failed = any(fi.get('filename') for fi in processed_files_info)
            if has_doc_sources_bp_failed:
                for file_info in processed_files_info:
                    if 'filename' in file_info:
                        display_url_bp_failed = storage_handler.get_public_url(file_info['filename'])
                        path_part_bp_failed = urlparse(file_info['filename']).path
                        filename_from_path_bp_failed = os.path.basename(path_part_bp_failed.split('?')[0])
                        source_name_bp_failed = filename_from_path_bp_failed or f"{file_info.get('type','doc')}_{file_info.get('event_date','ND')}.pdf"
                        source_list_json_bp_failed.append({"name": source_name_bp_failed, "url": display_url_bp_failed, "category": "Company data"})
            return json.dumps({
                "status": "error", 
                "message": "Error: Both document analysis and web search failed.",
                "sources": source_list_json_bp_failed
            }, indent=2)
        
        # 5. Synthesize with Claude (Run synchronous function in thread)
        if not claude_client:
             logger.error("Claude client not initialized, skipping synthesis.")
             claude_response = json.dumps({"status": "error", "message": "Claude client not available for synthesis."}, indent=2) # Return JSON error
        else:
            logger.info("Starting final synthesis with Claude")
            claude_start = time.time()
            try:
                loop = asyncio.get_running_loop() 
                # Debug: Log input lengths for Claude synthesis
                logger.debug(f"Claude synthesis inputs - Gemini: {len(gemini_output)} chars, Perplexity: {len(perplexity_output)} chars")

                claude_response_raw = await loop.run_in_executor(
                    None, # Default executor
                    query_claude, 
                    query, 
                    company_name, 
                    gemini_output, 
                    perplexity_output, 
                    conversation_context
                )
                # Strip markdown fences from Claude's response before further processing
                if claude_response_raw.strip().startswith("```json"):
                    claude_response = claude_response_raw.strip()[7:-3].strip() # Remove ```json and ```
                elif claude_response_raw.strip().startswith("```"):
                    claude_response = claude_response_raw.strip()[3:-3].strip() # Remove ``` and ```
                else:
                    claude_response = claude_response_raw.strip()

                claude_duration = time.time() - claude_start
                logger.info(f"Claude synthesis completed in {claude_duration:.2f} seconds")
            except Exception as e_claude:
                logger.error(f"Error during Claude synthesis (run_in_executor): {e_claude}", exc_info=True)
                claude_response = json.dumps({"status": "error", "message": f"Error during Claude synthesis: {e_claude}"}, indent=2)
        
        # Prepare sources in JSON format - wait for transcript PDF if needed
        source_list_json = []
        
        # Add regular file sources (reports/slides)
        has_doc_sources = any(fi.get('filename') for fi in processed_files_info)
        if has_doc_sources:
            for file_info in processed_files_info:
                if 'filename' in file_info:
                    display_url = storage_handler.get_public_url(file_info['filename'])
                    path_part = urlparse(file_info['filename']).path
                    filename_from_path = os.path.basename(path_part.split('?')[0])
                    source_name = filename_from_path or f"{file_info.get('type','doc')}_{file_info.get('event_date','ND')}.pdf" # Default name
                    source_list_json.append({"name": source_name, "url": display_url, "category": "Company data"})
        
        # Wait for transcript PDF generation to complete and add to sources
        if transcript_pdf_task:
            logger.info("Waiting for background transcript PDF generation to complete...")
            try:
                transcript_pdf_info = await asyncio.wait_for(transcript_pdf_task, timeout=120.0)  # 2 minute timeout for PDF generation
                if transcript_pdf_info:
                    display_url = storage_handler.get_public_url(transcript_pdf_info['filename'])
                    path_part = urlparse(transcript_pdf_info['filename']).path
                    filename_from_path = os.path.basename(path_part.split('?')[0])
                    source_name = filename_from_path or f"transcript_{transcript_pdf_info.get('event_date','ND')}.pdf"
                    source_list_json.append({"name": source_name, "url": display_url, "category": "Company data"})
                    logger.info(f"Successfully added transcript PDF to sources: {source_name}")
                else:
                    logger.warning("Background transcript PDF generation returned None")
            except asyncio.TimeoutError:
                logger.error("Background transcript PDF generation timed out after 2 minutes")
            except Exception as e:
                logger.error(f"Error waiting for background transcript PDF generation: {e}")
        
        logger.info(f"Total sources prepared: {len(source_list_json)}")

        # Determine final response based on success/failure of steps
        # Check if Claude response is already a JSON error string from query_claude or synthesis exception
        is_claude_error_json = False
        try:
            parsed_temp_claude_error = json.loads(claude_response)
            if isinstance(parsed_temp_claude_error, dict) and parsed_temp_claude_error.get("status") == "error":
                is_claude_error_json = True
        except json.JSONDecodeError:
            pass # Not a JSON error string, proceed to parse as main response

        if not is_claude_error_json and not claude_response.startswith("Error:"): # Make sure it's not a simple string error from old logic
             try:
                 parsed_claude_json = json.loads(claude_response)
                 # Inject sources into the parsed JSON
                 if 'data' in parsed_claude_json and isinstance(parsed_claude_json['data'], dict):
                     parsed_claude_json['data']['sources'] = source_list_json
                 elif 'data' not in parsed_claude_json: # If 'data' key is missing, create it for sources
                     parsed_claude_json['data'] = {'sources': source_list_json}
                 elif 'data' in parsed_claude_json and not isinstance(parsed_claude_json['data'], dict):
                     # Data key exists but is not a dict. This is problematic.
                     # Log this and potentially overwrite or handle as an error.
                     logger.warning("Claude JSON has 'data' field but it is not a dictionary. Overwriting with sources.")
                     parsed_claude_json['data'] = {'sources': source_list_json} # Risky, but follows prompt structure desire
                 else: # This case should ideally not be hit if data exists and is a dict
                      parsed_claude_json['data'] = {'sources': source_list_json} # Fallback to ensure sources are there
                 
                 final_response = json.dumps(parsed_claude_json, indent=2)

             except json.JSONDecodeError as e:
                 logger.error(f"Failed to parse Claude response as JSON: {e}")
                 logger.error(f"Claude response (after stripping) was: {claude_response[:500]}")
                 if not gemini_output.startswith("Error"):
                     final_response = json.dumps({
                         "status": "error",
                         "message": "Claude synthesis failed to produce valid JSON. Falling back to Gemini output.",
                         "details": f"Claude raw output (stripped, truncated): {claude_response[:500]}...",
                         "gemini_output_markdown": gemini_output,
                         "sources": source_list_json
                     }, indent=2)
                 else:
                     final_response = json.dumps({
                         "status": "error",
                         "message": "Both Claude and Gemini analysis failed. Claude output was not valid JSON.",
                         "details": f"Claude raw output (stripped, truncated): {claude_response[:500]}...",
                         "sources": source_list_json
                     }, indent=2)
        elif is_claude_error_json:
            # Claude_response is already a JSON error string from query_claude or synthesis exception
            # We might want to ensure sources are part of this error JSON if possible
            try:
                error_json_from_claude = json.loads(claude_response)
                if isinstance(error_json_from_claude, dict) and 'sources' not in error_json_from_claude:
                    error_json_from_claude['sources'] = source_list_json # Add sources if not present
                final_response = json.dumps(error_json_from_claude, indent=2)
            except json.JSONDecodeError: # Should not happen if is_claude_error_json is true
                final_response = claude_response # Fallback to the raw error string

        elif not gemini_output.startswith("Error"):
             logger.warning("Claude synthesis failed (simple error string or other), falling back to Gemini output.")
             final_response = json.dumps({
                 "status": "error",
                 "message": "Claude synthesis failed. Displaying Gemini output.",
                 "claude_error_details": claude_response, # Include Claude's error string
                 "gemini_output_markdown": gemini_output,
                 "sources": source_list_json
             }, indent=2)
        else:
             logger.error("Both Gemini and Claude failed (simple error strings or other issues).")
             final_response = json.dumps({
                 "status": "error",
                 "message": "Both document analysis and synthesis failed.",
                 "claude_error_details": claude_response,
                 "gemini_error_details": gemini_output,
                 "sources": source_list_json
             }, indent=2)

        # The old sources_section logic is removed as sources are injected into the JSON directly.

        # Log the final JSON output before returning
        logger.info(f"FINAL JSON OUTPUT: {final_response}")
        logger.info(f"--- EXITING run_analysis (Success) for {company_name} ---")
        return final_response

    except Exception as e_main:
        print(f"!!! PRINT: Error during main analysis block for {company_name}: {e_main}") 
        logger.error(f"Error during main analysis block for {company_name}: {e_main}", exc_info=True)
        if perplexity_task and not perplexity_task.done():
             try:
                 logger.info("Attempting to cancel Perplexity task due to main analysis error...")
                 perplexity_task.cancel()
                 await asyncio.sleep(0.1) # Give a moment for cancellation to register
             except Exception as e_cancel:
                 logger.error(f"Error cancelling Perplexity task: {e_cancel}")
        
        if transcript_pdf_task and not transcript_pdf_task.done():
             try:
                 logger.info("Attempting to cancel transcript PDF task due to main analysis error...")
                 transcript_pdf_task.cancel()
                 await asyncio.sleep(0.1) # Give a moment for cancellation to register
             except Exception as e_cancel:
                 logger.error(f"Error cancelling transcript PDF task: {e_cancel}")
        
        # Prepare sources for main exception
        source_list_json_exception = []
        # Check if processed_files_info is available in this scope, might need to be careful
        if 'processed_files_info' in locals() and processed_files_info: # Check if defined
            has_doc_sources_exception = any(fi.get('filename') for fi in processed_files_info)
            if has_doc_sources_exception:
                for file_info in processed_files_info:
                    if 'filename' in file_info and 'storage_handler' in locals() and storage_handler: # Check for storage_handler too
                        display_url_exception = storage_handler.get_public_url(file_info['filename'])
                        path_part_exception = urlparse(file_info['filename']).path
                        filename_from_path_exception = os.path.basename(path_part_exception.split('?')[0])
                        source_name_exception = filename_from_path_exception or f"{file_info.get('type','doc')}_{file_info.get('event_date','ND')}.pdf"
                        source_list_json_exception.append({"name": source_name_exception, "url": display_url_exception, "category": "Company data"})

        final_response = json.dumps({
            "status": "error",
            "message": f"An unexpected error occurred during analysis: {str(e_main)}",
            "sources": source_list_json_exception
        }, indent=2)
        logger.info(f"--- EXITING run_analysis (Main Exception) for {company_name} ---")
        return final_response

    finally:
        logger.info(f"--- Reached finally block in run_analysis for {company_name} ---")
        if temp_download_dir and os.path.exists(temp_download_dir):
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, shutil.rmtree, temp_download_dir)
                logger.info(f"Cleaned up temporary download directory: {temp_download_dir}")
            except Exception as cleanup_err:
                logger.error(f"Error cleaning up temp directory {temp_download_dir}: {cleanup_err}")
        logger.info(f"--- EXITING run_analysis (Finally Block Complete) for {company_name} ---")
