import pandas as pd
from ragas import evaluate
from ragas.metrics import (
    answer_relevancy,
    faithfulness,
    context_recall,
    context_precision,
)

def load_data(file_path):
    """Load the dataset for evaluation."""
    return pd.read_csv(file_path)

def prepare_evaluation_data(df):
    """Prepare the data in the format required by RAGAS."""
    return {
        "question": df['question'].tolist(),
        "context": df['context'].tolist(),
        "answer": df['rag_answer'].tolist(),
        "ground_truth": df['ground_truth'].tolist()
    }

def run_evaluation(evaluation_data):
    """Run the RAGAS evaluation."""
    return evaluate(
        evaluation_data,
        metrics=[
            context_precision,
            faithfulness,
            answer_relevancy,
            context_recall,
        ]
    )

def analyze_results(result):
    """Analyze and print the evaluation results."""
    print("Evaluation Results:")
    print(result)
    
    # Convert to DataFrame for more detailed analysis if needed
    result_df = result.to_pandas()
    print("\nDetailed Metrics:")
    print(result_df.describe())

def main():
    # Configuration
    data_file_path = "generated_testset.csv"

    # Main process
    df = load_data(data_file_path)
    evaluation_data = prepare_evaluation_data(df)
    result = run_evaluation(evaluation_data)
    analyze_results(result)

if __name__ == "__main__":
    main()