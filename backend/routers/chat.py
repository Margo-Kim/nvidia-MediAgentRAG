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
import json
import networkx as nx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from langchain_nvidia_ai_endpoints import ChatNVIDIA
from langchain.chains import GraphQAChain
from vectorstore.search import SearchHandler
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.graphs.networkx_graph import NetworkxEntityGraph
import logging
import requests
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Define a Pydantic model for the chat request body
class ChatRequest(BaseModel):
    user_input: str
    use_kg: bool
    model_id: str

# # Load the knowledge graph path from environment variables
DATA_DIR = os.getenv("DATA_DIR")
print("DATA_DIR:", DATA_DIR)
KG_GRAPHML_PATH = os.path.join(DATA_DIR, "knowledge_graph.graphml")

LLAMA_70B_URL = os.getenv("LLAMA_70B_URL", "http://localhost:8000")

# Function to call Llama 3 70B
def call_llama_70b(prompt: str, max_tokens: int = 64) -> Dict[str, Any]:
    url = f"{LLAMA_70B_URL}/v1/chat/completions"
    payload = {
        "model": "meta/llama3-70b-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens
    }
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error calling Llama 3 70B: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error calling Llama 3 70B: {str(e)}")


# Define an endpoint to get available models
@router.get("/get-models/")
async def get_models():
    models = ChatNVIDIA.get_available_models()
    available_models = [model.id for model in models if model.model_type == "chat" and "instruct" in model.id]
    available_models.append("meta/llama3-70b-instruct")
    return {"models": available_models}


# @router.post("/chat/")
# async def chat_endpoint(request: ChatRequest):
#     response_data = {"user_input": request.user_input}
#     llm = ChatNVIDIA(model=request.model_id)
    
#     prompt_template = ChatPromptTemplate.from_messages([
#         ("system", "You are a helpful AI assistant named Envie. You will reply to questions based on the context provided. If something is out of context, politely decline to respond."),
#         ("user", "{input}")
#     ])
#     chain = prompt_template | llm | StrOutputParser()

#     search_handler = SearchHandler("hybrid_demo3", use_bge_m3=True, use_reranker=True)
    
#     try:
#         search_results = search_handler.search_and_rerank(request.user_input, k=5)
#         context = "Here are the relevant passages from the knowledge base:\n\n" + "\n".join(item.text for item in search_results)
#         response_data["context"] = context
#     except Exception as e:
#         logger.error(f"Error performing hybrid search: {str(e)}")
#         response_data["context"] = "No relevant information found in the knowledge base."

#     full_response = llm.invoke(f"Context: {response_data['context']}\n\nUser query: {request.user_input}")
#     response_data["assistant_response"] = full_response if isinstance(full_response, str) else full_response.content

#     return response_data

@router.post("/chat/")
async def chat_endpoint(request: ChatRequest):
    response_data = {"user_input": request.user_input}
    
    search_handler = SearchHandler("hybrid_demo3", use_bge_m3=True, use_reranker=True)
    
    try:
        search_results = search_handler.search_and_rerank(request.user_input, k=5)
        context = "Here are the relevant passages from the knowledge base:\n\n" + "\n".join(item.text for item in search_results)
        response_data["context"] = context
    except Exception as e:
        logger.error(f"Error performing hybrid search: {str(e)}")
        response_data["context"] = "No relevant information found in the knowledge base."

    if request.model_id == "meta/llama3-70b-instruct":
        try:
            llama_response = call_llama_70b(f"Context: {response_data['context']}\n\nUser query: {request.user_input}")
            response_data["assistant_response"] = llama_response["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Error using Llama 3 70B: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error using Llama 3 70B: {str(e)}")
    else:
        llm = ChatNVIDIA(model=request.model_id)
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", "You are a helpful AI assistant named Envie. You will reply to questions based on the context provided. If something is out of context, politely decline to respond."),
            ("user", "{input}")
        ])
        chain = prompt_template | llm | StrOutputParser()
        full_response = llm.invoke(f"Context: {response_data['context']}\n\nUser query: {request.user_input}")
        response_data["assistant_response"] = full_response if isinstance(full_response, str) else full_response.content

    return response_data

# Add a new endpoint to check Llama 3 70B status
@router.get("/check-llama-70b-status/")
async def check_llama_70b_status():
    try:
        response = requests.get(f"{LLAMA_70B_URL}/v1/models", timeout=5)
        response.raise_for_status()
        return {"status": "available"}
    except requests.RequestException as e:
        logger.error(f"Error checking Llama 3 70B status: {str(e)}")
        raise HTTPException(status_code=503, detail="Llama 3 70B is not available")