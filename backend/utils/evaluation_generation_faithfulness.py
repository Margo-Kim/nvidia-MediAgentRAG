import os

# Set OpenAI API Key
os.environ["OPENAI_API_KEY"] = ""

from datasets import Dataset 
from ragas.metrics import faithfulness
from ragas.metrics import answer_relevancy
from ragas import evaluate

# Function to evaluate faithfulness
def evaluate_faithfulness(questions, answers, contexts):
    """
    Evaluate the faithfulness metric.

    Parameters:
        questions (list): A list of questions.
        answers (list): A list of answers generated by the model.
        contexts (list of list): A list of context information related to each question.

    Returns:
        pandas.DataFrame: A DataFrame containing the faithfulness scores.
    """
    data_samples = {
        'question': questions,
        'answer': answers,
        'contexts': contexts,
    }
    dataset = Dataset.from_dict(data_samples)
    score = evaluate(dataset, metrics=[faithfulness])
    return score.to_pandas()

# Example usage
questions = ['When was the first super bowl?', 'Who won the most super bowls?']
answers = ['The first superbowl was held on Jan 15, 1967', 'The most super bowls have been won by The New England Patriots']
contexts = [
    ['The First AFL–NFL World Championship Game was an American football game played on January 15, 1967, at the Los Angeles Memorial Coliseum in Los Angeles,'],
    ['The Green Bay Packers...Green Bay, Wisconsin.', 'The Packers compete...Football Conference']
]

faithfulness_score = evaluate_faithfulness(questions, answers, contexts)
print(faithfulness_score)