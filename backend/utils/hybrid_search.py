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
from langchain_community.document_loaders import DirectoryLoader, UnstructuredFileLoader, TextLoader
from langchain_community.document_loaders import PyPDFLoader
from pypdf import PdfReader
from langchain.docstore.document import Document
from langchain_nvidia_ai_endpoints import ChatNVIDIA
import concurrent.futures
import os
import logging
from tqdm import tqdm
from langchain_community.document_loaders import DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
import multiprocessing
import csv
import streamlit as st
# from utils.preprocessor import extract_triples
import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt_tab')
nltk.download('averaged_perceptron_tagger_eng')

import logging
import sys

logger = logging.getLogger(__name__)


# function to process a single document (will run many of these processes in parallel)
def process_document(doc, llm):
    try:
        return extract_triples(doc, llm)
    except Exception as e:
        print(f"Error processing document: {e}")
        return []


def read_pdf(file_path):
    try:
        with open(file_path, 'rb') as file:
            pdf = PdfReader(file)
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n"
        return text
    except Exception as e:
        logger.error(f"Error reading PDF {file_path}: {str(e)}")
        return ""

def custom_document_loader(file_path):
    _, extension = os.path.splitext(file_path)
    try:
        if extension.lower() == '.pdf':
            logger.info(f"Loading PDF: {file_path}")
            text = read_pdf(file_path)
            if text:
                return [Document(page_content=text, metadata={"source": file_path})]
            else:
                logger.warning(f"No content extracted from PDF: {file_path}")
                return []
        elif extension.lower() == '.txt':
            logger.info(f"Loading text file: {file_path}")
            return TextLoader(file_path).load()
        else:
            logger.info(f"Loading file with UnstructuredFileLoader: {file_path}")
            return UnstructuredFileLoader(file_path).load()
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {str(e)}")
        return []



def process_documents(directory, llm, update_progress=None, triplets=False, chunk_size=500, chunk_overlap=100):    
    logger.info(f"Processing documents from directory: {directory}")
    
    # Highlight[START]
    documents = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            docs = custom_document_loader(file_path)
            documents.extend(docs)
    
    logger.info(f"Loaded {len(documents)} raw documents")
    # Highlight[END]

    if not documents:
        logger.warning("No documents were loaded successfully.")
        return []

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    split_documents = text_splitter.split_documents(documents)
    logger.info(f"Split into {len(split_documents)} document chunks")

    if update_progress:
        update_progress(1, 1)  # Indicate completion


    return split_documents, [] 



import pandas as pd

def save_triples_to_csvs(triples):
    data_directory = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_directory, exist_ok=True)

    # Define paths for the CSV files
    entities_csv_path = os.path.join(data_directory, 'entities.csv')
    relations_csv_path = os.path.join(data_directory, 'relations.csv')
    triples_csv_path = os.path.join(data_directory, 'triples.csv')
    # Create the triples DataFrame
    triples_df = pd.DataFrame(triples, columns=['subject', 'subject_type', 'relation', 'object', 'object_type'])

    # Create the relations DataFrame
    relations_df = pd.DataFrame({'relation_id': range(len(triples_df['relation'].unique())), 'relation_name': triples_df['relation'].unique()})

    # Get unique entities (subjects and objects) from triples_df
    entities = pd.concat([triples_df['subject'], triples_df['object']]).unique()

    entities_df = pd.DataFrame({
    'entity_name': entities,
    'entity_type': [
        triples_df.loc[triples_df['subject'] == entity, 'subject_type'].iloc[0]
        if entity in triples_df['subject'].values
        else triples_df.loc[triples_df['object'] == entity, 'object_type'].dropna().iloc[0]
             if not triples_df.loc[triples_df['object'] == entity, 'object_type'].empty
             else 'Unknown'
        for entity in entities
        ]
    })
    entities_df = entities_df.reset_index().rename(columns={'index': 'entity_id'})

    # Merge triples_df with entities_df for subject
    triples_with_ids = triples_df.merge(entities_df[['entity_id', 'entity_name']], left_on='subject', right_on='entity_name', how='left')
    triples_with_ids = triples_with_ids.rename(columns={'entity_id': 'entity_id_1'}).drop(columns=['entity_name', 'subject', 'subject_type'])

    # Merge triples_with_ids with entities_df for object
    triples_with_ids = triples_with_ids.merge(entities_df[['entity_id', 'entity_name']], left_on='object', right_on='entity_name', how='left')
    triples_with_ids = triples_with_ids.rename(columns={'entity_id': 'entity_id_2'}).drop(columns=['entity_name', 'object', 'object_type'])

    # Merge triples_with_ids with relations_df to get relation IDs
    triples_with_ids = triples_with_ids.merge(relations_df, left_on='relation', right_on='relation_name', how='left').drop(columns=['relation', 'relation_name'])

    # Select necessary columns and ensure correct data types
    result_df = triples_with_ids[['entity_id_1', 'relation_id', 'entity_id_2']].astype({'entity_id_1': int, 'relation_id': int, 'entity_id_2': int})

   
    entities_df.to_csv(entities_csv_path, index=False)
    relations_df.to_csv(relations_csv_path, index=False)
    result_df.to_csv(triples_csv_path, index=False)

if __name__ == "__main__":
    llm = ChatNVIDIA(model="ai-mixtral-8x7b-instruct")
    pdf_directory = "/Users/margokim/Downloads/knowledge_graph_rag/frontend/pages"  # Replace with your actual directory path
    pdf_files = [f for f in os.listdir(pdf_directory) if f.endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in {pdf_directory}")
    else:
        for pdf_file in pdf_files:
            pdf_path = os.path.join(pdf_directory, pdf_file)
            print(f"Testing PDF: {pdf_path}")
            content = read_pdf(pdf_path)
            if content:
                print(f"Successfully read PDF. First 500 characters:\n{content[:500]}...")
            else:
                print(f"Failed to read PDF or PDF is empty.")
    
    # Test document processing
    print("\nTesting document processing:")
    documents = process_documents(pdf_directory, None)  # Pass None for llm as it's not used in our simplified version
    print(f"Processed {len(documents)} document chunks")
    
    print(documents)


    