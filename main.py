import os
import argparse
from pathlib import Path
from dotenv import load_dotenv
from ingestion import DatabaseIngestion
from vector_index import ModelVectorIndex
from semantic_relationship import SemanticRelationshipGenerator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Database Model and Relationship Generator"
    )

    # Required arguments
    parser.add_argument(
        "--context",
        type=str,
        required=True,
        help="Business context for semantic understanding (e.g., 'This is an e-commerce platform...')",
    )

    # Database tables argument
    parser.add_argument(
        "--tables",
        type=str,
        nargs="+",
        required=True,
        help="List of database tables in format 'database:table' (e.g., 'db1:users db1:orders')",
    )

    # Optional arguments
    parser.add_argument(
        "--test-queries",
        type=str,
        nargs="+",
        help="Test queries for vector search (e.g., 'Find tables related to customers')",
    )

    return parser.parse_args()


def parse_tables(table_args):
    """Convert table arguments from 'database:table' format to list of tuples"""
    tables = []
    for table_arg in table_args:
        try:
            db_name, table_name = table_arg.split(":")
            tables.append((db_name, table_name))
        except ValueError:
            raise ValueError(
                f"Invalid table format: {table_arg}. Expected format: 'database:table'"
            )
    return tables


def main():
    # Load environment variables
    load_dotenv(override=True)

    # Parse command line arguments
    args = parse_args()

    # Create cache directories
    base_path = Path("fs_cache")
    base_path.mkdir(parents=True, exist_ok=True)

    # Parse tables
    database_tables = parse_tables(args.tables)

    try:
        # Step 1: Database Ingestion
        print("\n=== Step 1: Database Ingestion ===")
        ingestion = DatabaseIngestion(database_tables, args.context)
        ingestion.process()
        print("✓ Database ingestion completed")

        # Step 2: Build Vector Index
        print("\n=== Step 2: Building Vector Index ===")
        vector_index = ModelVectorIndex()
        index = vector_index.build_index()
        vector_index.save_index(index)
        print("✓ Vector index built and saved")

        # Test vector search if queries provided
        if args.test_queries:
            print("\nTesting vector search with sample queries:")
            loaded_index = vector_index.load_index()
            for query in args.test_queries:
                print(f"\nQuery: {query}")
                results = loaded_index.similarity_search_with_score(query, k=2)
                for doc, score in results:
                    print(f"Table: {doc.metadata['table_name']}")
                    print(f"Database: {doc.metadata['database']}")
                    print(f"Relevance Score: {score}")
                    print("-" * 50)

        # Step 3: Generate Semantic Relationships
        print("\n=== Step 3: Generating Semantic Relationships ===")
        relationship_generator = SemanticRelationshipGenerator()
        relationships = relationship_generator.process_models()
        relationship_generator.save_relationships(relationships)
        print(f"✓ Generated {len(relationships)} relationships")

    except Exception as e:
        print(f"Error during processing: {str(e)}")
    finally:
        if ingestion:
            ingestion.close()


if __name__ == "__main__":
    main()
