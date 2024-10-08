# Import necessary libraries
from langchain.schema import Document
from ragas.testset.generator import TestsetGenerator
from ragas.testset.evolutions import simple, reasoning, multi_context
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
import json
import nest_asyncio
nest_asyncio.apply()

# Load your dataset
json_file_path = "backend/utils/eval/test_data_generation/processed_first_discharge_note_diff.json"

with open(json_file_path, 'r') as f:
    json_data = json.load(f)

# Step 2: Convert JSON data into the appropriate Document format
documents = []
if isinstance(json_data, dict):
    extracted_info = json_data.get("processed_data", {}).get("extracted_info", {})
    # Join all values from `extracted_info` to create a single content string
    content = "\n".join(str(value) for value in extracted_info.values())
    if content:
        documents.append(Document(page_content=content))
# else:
#     for record in json_data:
#         extracted_info = record.get("processed_data", {}).get("extracted_info", {})
#         # Join all values from `extracted_info` to create a single content string
#         content = "\n".join(str(value) for value in extracted_info.values())
#         if content:
#             documents.append(Document(page_content=content))


# Step 3: Initialize RAGAS components for test data generation
generator_llm = ChatOpenAI(model="gpt-4o-mini")
critic_llm = ChatOpenAI(model="gpt-4o")
embeddings = OpenAIEmbeddings()

generator = TestsetGenerator.from_langchain(
    generator_llm,
    critic_llm,
    embeddings
)

# Step 4: Define the distribution of question types
distributions = {
    simple: 0.5,
    multi_context: 0.4,
    reasoning: 0.1
}
# Step 5: Generate synthetic question-answer pairs
num_samples = 10  # Adjust as needed
testset = generator.generate_with_langchain_docs(documents, num_samples, distributions)

# Step 6: Convert testset to a DataFrame for analysis
test_df = testset.to_pandas()

# Display the first few rows of the generated DataFrame
print(test_df.head())

# Analyze the distribution of question types in the dataset
# print("Question Type Distribution:")
# print(test_df['question_type'].value_counts())

# Step 7: Save the generated dataset to a CSV file for further use
output_csv_path = "backend/utils/eval/test_data_generation/generated_testset.csv"
test_df.to_csv(output_csv_path, index=False)
