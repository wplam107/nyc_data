import math
from datetime import datetime

from soql_queries import dt_to_string, before_query#, after_query
from models import Establishment, LastUpdate, LatestDate


####################
# Helper functions #
####################
def counter(docs):
    count = 0
    for doc in docs:
        count += 1

    return count

def get_batches(count):
    batches = 1
    if count > 500:
        batches = math.ceil(count / 500)

    return batches


#########################
# Functions for loading #
#########################
def delete_collection(coll_ref, batch_size, total=0):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        # print(f'Deleting doc {doc.id} => {doc.to_dict()}')
        doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        total += deleted
        return delete_collection(coll_ref, batch_size, total)
    else:
        print('Total Deleted:', total)

def build_collection(db, limit=50000):
    # Query Socrata
    now = dt_to_string(datetime.now())
    results = before_query(now, limit)
    count = len(results)

    # Determine batches
    batch_count = get_batches(count)
    batches = [
        range(500 * i, 500 * (i+1)) if i != batch_count - 1
        else range(500 * i, count)
        for i in range(0, batch_count)
    ]

    # Load batches
    total = 0
    batch_number = 0
    for batch in batches:
        batch_ref = db.batch()
        for i in batch:
            r = results[i]
            doc = Establishment(r)
            ref = db.collection(doc.collection).document(doc._id)
            batch_ref.set(ref, dict(doc))

        res = batch_ref.commit()
        total += len(res)
        batch_number += 1

    print('Batches Loaded:', batch_number)
    print('Total Writes:', total)