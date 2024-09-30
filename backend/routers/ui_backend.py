# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import pandas as pd
import networkx as nx
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.lc_graph import process_documents, save_triples_to_csvs
from vectorstore.search import SearchHandler
from langchain_nvidia_ai_endpoints import ChatNVIDIA
from dotenv import load_dotenv
import logging
load_dotenv()
import logging
import sys

def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create console handler and set level to INFO
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to console handler
    console_handler.setFormatter(formatter)

    # Add console handler to logger
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()

# Get the data directory path from environment variables
DATA_DIR = os.getenv("DATA_DIR") 


# Initialize a FastAPI router
router = APIRouter()

# Define a Pydantic model for the request body
class DirectoryRequest(BaseModel):
    directory: str
    model_id: str
# Define an endpoint to get available models
@router.get("/get-models/")
async def get_models():
    models = ChatNVIDIA.get_available_models()
    available_models = [model.id for model in models if model.model_type == "chat" and "instruct" in model.id]
    return {"models": available_models}

# Define an endpoint to process documents
@router.post("/process-documents/")
async def process_documents_endpoint(request: DirectoryRequest, background_tasks: BackgroundTasks):
    directory = request.directory
    model_id = request.model_id
    llm = ChatNVIDIA(model=model_id)
    logger.info(f"Received request to process documents from directory: {directory}")
    logger.info(f"Using model: {model_id}")

    # Save progress updates in a temporary file
    progress_file = "progress.txt"
    with open(progress_file, "w") as f:
        f.write("0")

    def update_progress(completed_futures, total_futures):
        progress = completed_futures / total_futures
        with open(progress_file, "w") as f:
            f.write(str(progress))
        logger.info(f"Processing progress: {progress:.2%}")
    # Define a background task for processing documents
    def background_task():
        logger.info("Starting background task for document processing")
        documents, _ = process_documents(directory, llm, update_progress=update_progress, triplets=False)

        logger.info(f"Processed {len(documents)} documents")
        logger.info("Initializing SearchHandler for document insertion")
        search_handler = SearchHandler("hybrid_demo3", use_bge_m3=True, use_reranker=True)
        logger.info("Inserting processed documents into search index")
        search_handler.insert_data(documents)
        logger.info("Document insertion completed")

    background_tasks.add_task(background_task)
    logger.info("Background task for document processing has been queued")
    return {"message": "Processing started"}
# Define an endpoint to get the progress of the background task
@router.get("/progress/")
async def get_progress():
    try:
        with open("progress.txt", "r") as f:
            progress = f.read()
        return {"progress": progress}
    except FileNotFoundError:
        return {"progress": "0"}

# Define an endpoint to check if the GraphML file exists
# @router.get("/check-file-exists/")
# async def check_file_exists():
#     graphml_path = os.path.join(DATA_DIR, "knowledge_graph.graphml")
#     if os.path.exists(graphml_path):
#         return JSONResponse(content={"file_exists": True})
#     else:
#         return JSONResponse(content={"file_exists": False})
# @router.get("/check-file-exists/")
# async def check_file_exists():
#     # Highlight[START]
#     # Instead of checking for a GraphML file, we'll check for the processed documents
#     documents_csv_path = os.path.join(DATA_DIR, "documents.csv")
#     if os.path.exists(documents_csv_path):
#         return JSONResponse(content={"file_exists": True})
#     else:
#         return JSONResponse(content={"file_exists": False})