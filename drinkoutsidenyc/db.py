import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from etl_functions import delete_collection, build_collection

# Use a service account
cred = credentials.Certificate('./drinkoutsidenyc-f825926aaa1f.json')
firebase_admin.initialize_app(cred)

db = firestore.client()

def main():
    # Set reference
    coll_ref = db.collection('establishments')

    # Drop collection
    delete_collection(coll_ref, 500)

    # Implicitly create collection and load data
    build_collection(db)

if __name__ == '__main__':
    main()