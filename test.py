import requests
import json

def get_embedding(text, input_type="query"):
    url = "http://localhost:8001/v1/embeddings"
    
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    
    data = {
        "input": [text],
        "model": "nvidia/nv-embedqa-e5-v5",
        "input_type": input_type
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}, {response.text}"

# Example usage
text = "Hello world"
result = get_embedding(text)
print(json.dumps(result, indent=2))

# If you want to use it for document embedding instead of query
# result = get_embedding(text, input_type="document")