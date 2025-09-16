import sqlite3

import sqlite3


# Generate a New SQL Lite Table that Eliminates Non-Relevant Fields
# Based on the "Relevant" Column in the Definitions Table
# We need to Map the Claims Code Table -> Definitions Code Table as they do not perfectly match in all cases 


#TASKS:
    # Figure out how to get the list of columns from the definitions table where Relevant = 1 and then use that to build the processed claims table -- based on list of col, create new table  -- Robert 
    # Map the claims code to the definitions code where they do not match perfectly (e.g., Claim ID -> ClaimID, Claim Amount -> Claim_Amount, etc.) -- Robert
    # Spit out New Sql Table into Databse (With Versioning? ) -- Robert 
    # Dynamically split cleaned dataset into 20% Test and 80% Train datasets for ML purposes -- Michelle 
    # Optional Generate and Tag Synthetic Fraud Data -- Manny 
    # Match in Beneficiary Data to Claims Data for Enriched Dataset (FROM CMS) -- Joel , ED

def create_dynamic_view(db_path, view_name, base_table,):
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
        relevant_fields_sql = f"Select * From raw_cms_claims ORDER BY BENE_ID"

        cursor.execute(relevant_fields_sql)
        raw_claims = cursor.fetchall()
        # Get column names from the cursor description
        column_names = [description[0] for description in cursor.description]

        # Convert raw claims to list of dictionaries
        claims_dict_list = []
        for row in raw_claims:
            claim_dict = dict(zip(column_names, row))
            claims_dict_list.append(claim_dict)

        # Now you have a list of dictionaries where each dictionary represents a claim
        # with column names as keys and row values as values
        print(f"Converted {len(claims_dict_list)} claims to dictionaries")


        # Group claims by BENE_ID
        claims_by_bene_id = {}
        
        for claim in claims_dict_list:
            bene_id = claim.get('BENE_ID')
            
            if bene_id not in claims_by_bene_id:
                claims_by_bene_id[bene_id] = []
            else:
                claims_by_bene_id[bene_id].append(claim)
        
        # Print summary of grouped claims
        print(f"Grouped claims into {len(claims_by_bene_id)} unique BENE_IDs")
        for bene_id, claims in claims_by_bene_id.items():
            print(f"BENE_ID {bene_id}: {len(claims)} claims")


        # Close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error creating view: {e}")





def main():
    db_path = "C:\\Projects\\Hackathon\\Hackathon25\\cms_synthetic_claims\\cms_synthetic_claims.db"
    claims_table = "raw_cms_claims"
    create_dynamic_view(db_path=db_path, view_name="relevant_claims_view", base_table=claims_table)

if __name__ == "__main__":
    main()