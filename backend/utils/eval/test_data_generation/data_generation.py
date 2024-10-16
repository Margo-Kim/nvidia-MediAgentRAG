# Import necessary libraries
from langchain.schema import Document
from ragas.testset.generator import TestsetGenerator
from ragas.testset.evolutions import simple, reasoning, multi_context
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
import json
import nest_asyncio
nest_asyncio.apply()
import pdfplumber

# Load the PDF file and extract its text content
pdf_file_path = "/raid/nvidia-project/nvidia-mediAgent/nvidia-MediAgentRAG/frontend/pages/document_13180007-DS-15.pdf"

documents = []

# Extract text from the PDF
with pdfplumber.open(pdf_file_path) as pdf:
    for page in pdf.pages:
        content = page.extract_text()
        if content:
            documents.append(Document(page_content=content))

# Ensure valid document content before processing
valid_documents = [doc for doc in documents if doc.page_content.strip()]

if len(valid_documents) == 0:
    raise ValueError("No valid document content found to process.")

# Initialize RAGAS components for test data generation
generator_llm = ChatOpenAI(model="gpt-3.5-turbo")
critic_llm = ChatOpenAI(model="gpt-3.5-turbo")
embeddings = OpenAIEmbeddings()

generator = TestsetGenerator.from_langchain(
    generator_llm,
    critic_llm,
    embeddings
)

# Define the distribution of question types
distributions = {
    simple: 0.5,
    multi_context: 0.4,
    reasoning: 0.1
}

# Generate synthetic question-answer pairs
num_samples = 5  # Adjust as needed
testset = generator.generate_with_langchain_docs(valid_documents, num_samples, distributions)

# Step 6: Convert testset to a DataFrame for analysis
test_df = testset.to_pandas()

# Save the generated dataset to a CSV file 
output_csv_path = "generated_testset_document_13180007-DS-15.csv"
test_df.to_csv(output_csv_path, index=False)

# -------------------------------------------------------------------------------------------------------

# code for local deployment (in progress)

# # Import necessary libraries
# from openai import OpenAI
# from langchain.schema import Document
# from ragas.testset.generator import TestsetGenerator
# from ragas.testset.evolutions import simple, reasoning, multi_context
# from langchain_openai import ChatOpenAI, OpenAIEmbeddings
# import nest_asyncio
# import pdfplumber
# from unittest.mock import AsyncMock
# from langchain_nvidia_ai_endpoints import NVIDIAEmbeddings
# import json
# nest_asyncio.apply()

# # Load the PDF file and extract its text content
# pdf_file_path = "/raid/nvidia-project/nvidia-mediAgent/nvidia-MediAgentRAG/frontend/pages/document_13180007-DS-14.pdf"

# documents = []

# # Extract text from the PDF
# with pdfplumber.open(pdf_file_path) as pdf:
#     for page in pdf.pages:
#         content = page.extract_text()
#         if content:
#             documents.append(Document(page_content=content))

# # Ensure valid document content before processing
# valid_documents = [doc for doc in documents if doc.page_content.strip()]

# if len(valid_documents) == 0:
#     raise ValueError("No valid document content found to process.")

# # Use the local `meta/llama-3.1-8b-instruct` model for generating and critiquing
# generator_llm = ChatOpenAI(model="meta/llama-3.1-8b-instruct", base_url="http://localhost:8000/v1", api_key="not-used")
# critic_llm = ChatOpenAI(model="meta/llama-3.1-8b-instruct", base_url="http://localhost:8000/v1", api_key="not-used")
# embeddings = NVIDIAEmbeddings(base_url="http://localhost:8001/v1", model="nvidia/nv-embedqa-e5-v5")


# # Initialize the testset generator with the mock embeddings
# generator = TestsetGenerator.from_langchain(
#     generator_llm,
#     critic_llm,
#     embeddings
# )

# # Define the distribution of question types
# distributions = {
#     simple: 0.5,
#     multi_context: 0.4,
#     reasoning: 0.1
# }

# # Generate synthetic question-answer pairs
# num_samples = 10  # Adjust as needed
# testset = generator.generate_with_langchain_docs(valid_documents, num_samples, distributions)

# # Convert testset to a DataFrame for analysis
# test_df = testset.to_pandas()

# # Save the generated dataset to a CSV file
# output_csv_path = "generated_testset.csv"
# test_df.to_csv(output_csv_path, index=False)

# # Print the first few rows of the generated dataset
# print(test_df.head())
