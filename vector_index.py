import json
from pathlib import Path
from typing import List, Dict
import os
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document

load_dotenv()


class ModelVectorIndex:
    def __init__(self, models_path: str = "fs_cache/models"):
        self.models_path = Path(models_path)
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

    def load_model_files(self) -> List[Dict]:
        """Load all JSON model files from the models directory"""
        models = []
        for file_path in self.models_path.glob("*.json"):
            with open(file_path, "r") as f:
                model = json.load(f)
                models.append(model)
        return models

    def create_model_document(self, model: Dict) -> Document:
        """Create a searchable document from model metadata"""
        # Convert the entire model to a string representation
        content = json.dumps(model, indent=2)

        # Create metadata for filtering
        metadata = {
            "table_name": model["name"],
            "database": model["database"],
            "column_count": len(model["columns"]),
            "table_description": model["properties"]["description"],
        }

        return Document(page_content=content, metadata=metadata)

    def build_index(self) -> FAISS:
        """Build FAISS index from all model files"""
        # Load all models
        models = self.load_model_files()

        # Convert models to documents
        documents = [self.create_model_document(model) for model in models]

        # Create and return the FAISS index
        return FAISS.from_documents(documents, self.embeddings)

    def save_index(self, index: FAISS, path: str = "fs_cache/vector_index"):
        """Save the FAISS index to disk"""
        index.save_local(path)

    def load_index(self, path: str = "fs_cache/vector_index") -> FAISS:
        """Load the FAISS index from disk"""
        return FAISS.load_local(
            path, self.embeddings, allow_dangerous_deserialization=True
        )


def main():
    vector_index = ModelVectorIndex()

    # Build and save the index
    print("Building vector index...")
    index = vector_index.build_index()

    print("Saving index to disk...")
    vector_index.save_index(index)

    # Example search
    print("\nTesting search functionality...")
    loaded_index = vector_index.load_index()

    # Example queries
    test_queries = []

    for query in test_queries:
        print(f"\nQuery: {query}")
        results = loaded_index.similarity_search_with_score(query, k=2)
        for doc, score in results:
            print(f"\nTable: {doc.metadata['table_name']}")
            print(f"Database: {doc.metadata['database']}")
            print(f"Relevance Score: {score}")
            print("-" * 50)


if __name__ == "__main__":
    main()
