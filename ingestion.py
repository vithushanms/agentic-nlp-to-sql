import os
import json
from dotenv import load_dotenv
from pathlib import Path
import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
import openai

load_dotenv()

# Configure OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")


class DatabaseIngestion:
    def __init__(self, database_tables=None, business_context=None):
        db_type = os.getenv("DATASOURCE_TYPE", "mysql").lower()
        self.database_tables = database_tables or []
        self.business_context = business_context

        if db_type == "mysql":
            self.connection_url_template = URL.create(
                "mysql+mysqlconnector",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT")),
                database=None,
            )
        elif db_type == "postgresql":
            self.connection_url_template = URL.create(
                "postgresql",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT")),
                database=None,
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        # Create cache directories if they don't exist
        self.base_path = Path("fs_cache")
        self.models_path = self.base_path / "models"
        self.relationships_path = self.base_path / "relationships"

        self.models_path.mkdir(parents=True, exist_ok=True)
        self.relationships_path.mkdir(parents=True, exist_ok=True)

    def connect_to_database(self, database_name):
        # Create a new connection URL for the specific database
        connection_url = self.connection_url_template.set(database=database_name)

        # Create new engine and inspector for this database
        self.engine = create_engine(connection_url)
        self.inspector = inspect(self.engine)

    def get_tables(self):
        if self.database_tables:
            # Return only the specified tables for the current database
            current_db = self.engine.url.database
            return [table for db, table in self.database_tables if db == current_db]
        return self.inspector.get_table_names()

    def get_columns(self, table_name):
        columns = []
        try:
            # First try the normal inspector method
            columns = self._get_columns_from_inspector(table_name)
        except Exception:
            # If that fails (e.g., for views), try using information_schema
            columns = self._get_columns_from_information_schema(table_name)
        return columns

    def _get_columns_from_inspector(self, table_name):
        columns = []
        for column in self.inspector.get_columns(table_name):
            col_info = {
                "COLUMN_NAME": column["name"],
                "DATA_TYPE": str(column["type"]),
                "IS_NULLABLE": "YES" if column.get("nullable") else "NO",
                "COLUMN_KEY": (
                    "PRI" if self.is_primary_key(table_name, column["name"]) else ""
                ),
            }
            columns.append(col_info)
        return columns

    def _get_columns_from_information_schema(self, table_name):
        columns = []
        query = text(
            """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_KEY
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = :database 
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """
        )

        with self.engine.connect() as connection:
            result = connection.execute(
                query, {"database": self.engine.url.database, "table_name": table_name}
            )
            for row in result:
                col_info = {
                    "COLUMN_NAME": row.COLUMN_NAME,
                    "DATA_TYPE": row.DATA_TYPE,
                    "IS_NULLABLE": row.IS_NULLABLE,
                    "COLUMN_KEY": row.COLUMN_KEY or "",
                }
                columns.append(col_info)
        return columns

    def is_primary_key(self, table_name, column_name):
        try:
            # First try the normal inspector method
            pk_columns = self.inspector.get_pk_constraint(table_name)[
                "constrained_columns"
            ]
            return column_name in pk_columns
        except Exception:
            # If that fails, try using information_schema
            query = text(
                """
                SELECT COUNT(*) as is_pk
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = :database
                AND TABLE_NAME = :table_name
                AND COLUMN_NAME = :column_name
                AND CONSTRAINT_NAME = 'PRIMARY'
            """
            )

            with self.engine.connect() as connection:
                result = connection.execute(
                    query,
                    {
                        "database": self.engine.url.database,
                        "table_name": table_name,
                        "column_name": column_name,
                    },
                ).scalar()
                return bool(result)

    def get_relationships(self):
        relationships = []
        try:
            # First try the normal inspector method
            for table_name in self.get_tables():
                for fk in self.inspector.get_foreign_keys(table_name):
                    relationship = {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": fk["constrained_columns"][0],
                        "REFERENCED_TABLE_NAME": fk["referred_table"],
                        "REFERENCED_COLUMN_NAME": fk["referred_columns"][0],
                    }
                    relationships.append(relationship)
        except Exception:
            # If that fails, try using information_schema
            query = text(
                """
                SELECT 
                    TABLE_NAME,
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = :database
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """
            )

            with self.engine.connect() as connection:
                result = connection.execute(
                    query, {"database": self.engine.url.database}
                )
                for row in result:
                    relationship = {
                        "TABLE_NAME": row.TABLE_NAME,
                        "COLUMN_NAME": row.COLUMN_NAME,
                        "REFERENCED_TABLE_NAME": row.REFERENCED_TABLE_NAME,
                        "REFERENCED_COLUMN_NAME": row.REFERENCED_COLUMN_NAME,
                    }
                    relationships.append(relationship)

        return relationships

    def generate_column_description(self, table_name, column_name, data_type):
        """Generate column description using OpenAI GPT"""
        if not self.business_context:
            return f"Column {column_name} in table {table_name}"

        prompt = f"""
Based on this business context:
{self.business_context}

Generate a brief (max 10-15 words) business description for this database column:
Table: {table_name}
Column: {column_name}
Data Type: {data_type}
Description should explain the business purpose of this column.
"""

        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a technical documentation expert who writes clear, concise database column descriptions.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=50,
                temperature=0.3,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(
                f"Warning: Could not generate description for {column_name}: {str(e)}"
            )
            return f"Column {column_name} in table {table_name}"

    def generate_table_description(self, table_name):
        """Generate table description using OpenAI GPT"""
        if not self.business_context:
            return f"Table containing {table_name} data"

        prompt = f"""
Based on this business context:
{self.business_context}

Generate a brief (max 10-15 words) technical description for this database table:
Table Name: {table_name}

Description should explain the business purpose of this table.
"""

        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a technical documentation expert who writes clear, concise database table descriptions.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=50,
                temperature=0.0,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(
                f"Warning: Could not generate description for table {table_name}: {str(e)}"
            )
            return f"Table containing {table_name} data"

    def generate_model_json(self, table_name, columns):
        model = {
            "name": table_name,
            "database": self.engine.url.database,
            "columns": [],
            "refreshTime": datetime.datetime.now().isoformat(),
            "properties": {
                "description": self.generate_table_description(table_name),
                "displayName": table_name,
                "database": self.engine.url.database,
            },
        }

        for column in columns:
            column_name = column["COLUMN_NAME"]
            data_type = column["DATA_TYPE"]

            column_info = {
                "name": column_name,
                "type": data_type.upper(),
                "notNull": 1 if column["IS_NULLABLE"] == "NO" else 0,
                "properties": {
                    "description": self.generate_column_description(
                        table_name, column_name, data_type
                    ),
                    "displayName": column_name,
                },
            }

            if column["COLUMN_KEY"] == "PRI":
                model["primaryKey"] = column_name

            model["columns"].append(column_info)

        return model

    def generate_relationship_json(self, relationship):
        return {
            "name": f"{relationship['TABLE_NAME']}_{relationship['REFERENCED_TABLE_NAME']}_Relation",
            "models": [
                relationship["TABLE_NAME"],
                relationship["REFERENCED_TABLE_NAME"],
            ],
            "joinType": "ONE_TO_MANY",
            "condition": f"{relationship['REFERENCED_TABLE_NAME']}.{relationship['REFERENCED_COLUMN_NAME']} = {relationship['TABLE_NAME']}.{relationship['COLUMN_NAME']}",
        }

    def process(self):
        if not self.database_tables:
            # If no specific tables provided, use default behavior
            self.connect_to_database(os.getenv("DB_NAME"))
            tables = self.get_tables()
            self._process_tables(tables)
        else:
            # Process each database and its tables
            processed_dbs = set()
            for db_name, table in self.database_tables:
                if db_name not in processed_dbs:
                    self.connect_to_database(db_name)
                    processed_dbs.add(db_name)

                columns = self.get_columns(table)
                model_json = self.generate_model_json(table, columns)

                with open(self.models_path / f"{db_name}_{table}.json", "w") as f:
                    json.dump(model_json, f, indent=4)

            # Process relationships after all tables are processed
            self._process_relationships()

    def _process_relationships(self):
        # Process relationships for each database
        processed_dbs = set()
        for db_name, _ in self.database_tables:
            if db_name not in processed_dbs:
                self.connect_to_database(db_name)
                processed_dbs.add(db_name)

                relationships = self.get_relationships()
                for idx, rel in enumerate(relationships):
                    # Only process relationships if both tables are in our filter
                    if self._is_relationship_relevant(rel):
                        relationship_json = self.generate_relationship_json(rel)
                        with open(
                            self.relationships_path
                            / f"{db_name}_relationship_{idx+1}.json",
                            "w",
                        ) as f:
                            json.dump(relationship_json, f, indent=4)

    def _is_relationship_relevant(self, relationship):
        # Check if both tables in the relationship are in our filtered list
        table_pairs = [(db, table) for db, table in self.database_tables]
        return any(
            (db, relationship["TABLE_NAME"]) in table_pairs
            for db in {pair[0] for pair in table_pairs}
        ) and any(
            (db, relationship["REFERENCED_TABLE_NAME"]) in table_pairs
            for db in {pair[0] for pair in table_pairs}
        )

    def close(self):
        if hasattr(self, "engine"):
            self.engine.dispose()


if __name__ == "__main__":
    # Example business context
    semantic_prompts = """
    
"""

    database_tables = []

    try:
        ingestion = DatabaseIngestion(database_tables, semantic_prompts)
        ingestion.process()
        print("Ingestion completed successfully!")
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
    finally:
        ingestion.close()
