import sqlite3

def create_dynamic_view(db_path, view_name, base_table, definitions_table):
    """
    Create a dynamic SQL view in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database
        view_name (str): Name of the view to create
        base_table (str): Name of the base table to query
        columns (list): List of columns to include in the view
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the view
        relevant_fields_sql = f"SELECT 'Variable Name', Relevant FROM {definitions_table} WHERE Relevant = 1"
        cursor.execute(relevant_fields_sql)
        rows = cursor.fetchall()

        print(f"View '{view_name}' created successfully.")

        # Close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error creating view: {e}")

def main():
    db_path = "C:\\Projects\\Hackathon\\Hackathon25\\cms_synthetic_claims\\cms_synthetic_claims.db"
    claims_table = "raw_cms_claims"
    definitions_table = "raw_claim_definitions"
    create_dynamic_view(db_path=db_path, view_name="relevant_claims_view", base_table=claims_table, definitions_table=definitions_table)

if __name__ == "__main__":
    main()