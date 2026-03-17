#!/usr/bin/env python3
"""Query the 100 most recent siretisations and output JSON."""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.database import DatabaseManager

def main():
    data = DatabaseManager.execute_query("""
        SELECT s.ID as domain_id, d.domaine, s.nom_societe, s.ThG_siret,
               s.ThG_MR_score, s.code_postal, s.ville, s.adresse,
               s.created_at, s.updated_at
        FROM APP_domaine_SIRETISATION s
        JOIN APP_domaine d ON s.ID = d.ID
        WHERE s.ThG_MR_score > 0
        ORDER BY s.updated_at DESC
        LIMIT 100
    """)
    result = []
    for row in data:
        r = dict(row)
        if r.get('created_at'):
            r['created_at'] = r['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if r.get('updated_at'):
            r['updated_at'] = r['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        result.append(r)
    print(json.dumps(result, ensure_ascii=False))

if __name__ == '__main__':
    main()
