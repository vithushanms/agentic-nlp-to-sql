import json
from pathlib import Path
import openai
from vector_index import ModelVectorIndex
from dotenv import load_dotenv
import os

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")


class SemanticRelationshipGenerator:
    def __init__(self, models_path: str = "fs_cache/models"):
        self.models_path = Path(models_path)
        self.vector_index = ModelVectorIndex()
        self.index = self.vector_index.load_index()

    def get_model_files(self):
        """Get all model files from the models directory"""
        return list(self.models_path.glob("*.json"))

    def generate_relationship_prompt(self, current_model, related_models):
        """Generate a prompt for the LLM to create relationships"""
        return f"""
Based on the following database models, generate potential relationships in JSON format.
Focus on identifying meaningful business relationships between these tables.

Current Model:
{json.dumps(current_model)}

Related Models:
{json.dumps(related_models)}

Generate relationships in the following format:
{{
    "name": "RelationshipName",
    "models": ["Table1", "Table2"],
    "joinType": "ONE_TO_MANY" or "MANY_TO_MANY",
    "condition": "Table1.column = Table2.column"
}}

Consider:
1. Primary and foreign key relationships
2. Business logic connections
3. Common fields that could be used for joins
4. The nature of the relationship (one-to-many, many-to-many)

Return only the JSON object, no additional text.
"""

    def process_models(self):
        """Process each model and generate relationships"""
        model_files = self.get_model_files()
        all_relationships = []

        for model_file in model_files:
            try:
                with open(model_file, "r", encoding="utf-8") as f:
                    current_model = json.load(f)

                    # Search for related models using vector similarity
                    query = f"Find tables related to {current_model['name']}"
                    try:
                        results = self.index.similarity_search_with_score(query, k=3)
                    except Exception as e:
                        print(f"Error in vector search for {model_file.name}: {str(e)}")
                        # Try to rebuild the index
                        self.index = self.vector_index.build_index()
                        results = self.index.similarity_search_with_score(query, k=3)

                    related_models = []
                    for doc, _ in results:
                        try:
                            related_model = json.loads(doc.page_content)
                            related_models.append(related_model)
                            # print(related_model)
                        except json.JSONDecodeError:
                            print(
                                f"Warning: Could not parse related model from vector store for {model_file.name}"
                            )
                            continue

                    if not related_models:
                        print(f"Warning: No related models found for {model_file.name}")
                        continue

                    # Generate relationships using LLM
                    prompt = self.generate_relationship_prompt(
                        current_model, related_models
                    )

                    try:
                        response = openai.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[
                                {
                                    "role": "system",
                                    "content": "You are a database relationship expert who generates accurate and meaningful table relationships.",
                                },
                                {"role": "user", "content": prompt},
                            ],
                            temperature=0.3,
                            max_tokens=500,
                            timeout=30,
                        )

                        response_content = response.choices[0].message.content.strip()

                        # Debug logging
                        print(f"\nResponse for {model_file.name}:")
                        print(response_content)

                        # Check for empty response
                        if not response_content:
                            print(
                                f"Warning: Empty response received for {model_file.name}"
                            )
                            continue

                        # Try to clean the response if it's not valid JSON
                        try:
                            # Parse the response - handle both single object and array
                            relationships = json.loads(response_content)
                        except json.JSONDecodeError:
                            # Try to clean the response by removing any markdown formatting
                            cleaned_content = response_content.strip("`").strip()
                            if cleaned_content.startswith("json"):
                                cleaned_content = cleaned_content[4:].strip()
                            try:
                                relationships = json.loads(cleaned_content)
                            except json.JSONDecodeError as e:
                                print(
                                    f"Error parsing LLM response for {model_file.name}: {str(e)}\n"
                                    f"Response content: {response_content[:200]}..."  # Show first 200 chars
                                )
                                continue

                        if isinstance(relationships, list):
                            all_relationships.extend(relationships)
                        else:
                            all_relationships.append(relationships)

                        print(f"Successfully processed {model_file.name}")

                    except openai.APITimeoutError:
                        print(f"Timeout processing {model_file.name}, retrying...")
                        continue
                    except json.JSONDecodeError as e:
                        print(
                            f"Error parsing LLM response for {model_file.name}: {str(e)}"
                        )
                        continue
                    except Exception as e:
                        print(
                            f"Error processing {model_file.name} with OpenAI: {str(e)}"
                        )
                        continue

            except json.JSONDecodeError:
                print(f"Error: Could not parse {model_file.name}")
                continue
            except Exception as e:
                print(f"Unexpected error processing {model_file.name}: {str(e)}")
                continue

        return all_relationships

    def save_relationships(
        self, relationships, output_path: str = "fs_cache/relationships"
    ):
        """Save generated relationships to JSON files"""
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        for idx, relationship in enumerate(relationships):
            with open(output_path / f"semantic_relationship_{idx+1}.json", "w") as f:
                json.dump(relationship, f, indent=4)


def main():
    generator = SemanticRelationshipGenerator()
    relationships = generator.process_models()
    generator.save_relationships(relationships)
    print(f"Generated {len(relationships)} relationships")


if __name__ == "__main__":
    main()
