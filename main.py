from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import os
import asyncio
import aiohttp
import time
import logging
import json
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any, Tuple
from supabase_client import get_quartrid_by_name
from logger import logger
from urllib.parse import urlparse

# Import the core logic function from app.py
from app import run_analysis 

# Load environment variables
load_dotenv()

# Configure logging
logger.info("Starting FastAPI Financial Insights Application")

# Initialize FastAPI app
app = FastAPI(
    title="Financial Insights API",
    description="API for generating financial insights about companies using the multi-LLM pipeline from app.py",
    version="1.0.1" # Increment version
)

# Input model
class QueryRequest(BaseModel):
    company_name: str
    query: str
    conversation_context: Optional[List[Dict[str, str]]] = None

# Response model
class QueryResponse(BaseModel):
    answer: str
    processing_time: float
    # sources: Optional[Dict[str, List[Dict[str, str]]]] = None # Commented out for now

# Main endpoint for financial insights
@app.post("/api/insights")
async def get_financial_insights(request: QueryRequest):
    start_time = time.time()
    logger.info(f"Received request for company: {request.company_name}, query: {request.query}")
    
    try:
        # Call the consolidated run_analysis function from app.py
        # Note: run_analysis handles company ID lookup, document processing, LLM calls, etc.
        analysis_result = await run_analysis(
            company_name=request.company_name,
            query=request.query,
            conversation_context=request.conversation_context
        )

        # Calculate processing time
        processing_time = time.time() - start_time
        logger.info(f"Request processed in {processing_time:.2f} seconds")
        
        # Add processing time to the analysis result
        if isinstance(analysis_result, dict):
            analysis_result["processing_time"] = processing_time
            # Log the final JSON output being returned
            logger.info(f"FINAL JSON OUTPUT: {json.dumps(analysis_result, indent=2)}")
            return analysis_result
        else:
            # Fallback for any edge cases where run_analysis still returns a string
            response_data = {
                "answer": analysis_result, 
                "processing_time": processing_time
            }
            logger.info(f"FINAL JSON OUTPUT (Fallback): {json.dumps(response_data, indent=2)}")
            return response_data
    
    except Exception as e:
        # Catch any unexpected errors during the endpoint execution itself
        logger.error(f"Unhandled exception in /api/insights endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Root endpoint
@app.get("/")
async def root():
    return {
        "greeting": "Hello, World!",
        "message": "Welcome to the Financial Insights API!"
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"} 
