from pymongo import MongoClient
import pymongo
import datetime


date = datetime.date.today()
netq_collec = f'symphonyorder_netquantity_{date}'
# filtered_collec = f'neworders_{date}'
cumulative_collec = f"cumulative_{date}"
all_list_client_collec = f"client"

connection = MongoClient('localhost', 27017)

try:
    cumulative_db = connection['Cumulative_symphonyorder']
    all_list_db = connection['all_list']
    # new_db = connection['symphonyorder_filtered']

except Exception:
    print("ERROR: Mongo Connection Error123")

try:
    netq_db = connection['symphonyorder_netquantity']
    netq_db[netq_collec].drop()
    print('Netq Collec Deleted')
except:
    pass

client = all_list_db[all_list_client_collec].distinct("client")
client.remove("All")

def savedata(post):
    try:
        client = MongoClient()
        db = client['symphonyorder_netquantity']
        collec = f"symphonyorder_netquantity_{date}"
        db.create_collection(collec)
        print(f"Created New Collection '{collec}'")
        db[collec].insert_one(post)
        #print(post)
    except Exception:
        new_match=match = db[collec].find_one({ "$and" : [{"clientID":post['clientID']},{"symbol":post['symbol']}] })
        if new_match:
            # print(new_match)
            if(new_match["quantity"]!=post["quantity"]):
                db[collec].update({'_id': new_match['_id']}, {"$set": {"quantity":post["quantity"]}})
        
        else:
            db[collec].insert_one(post)
            print("new Quantities Added")

def check_data():

    while True:
        for client_id in client:
            new_client = cumulative_db[cumulative_collec].find_one({"clientID": client_id})
            if new_client:
                
                match=cumulative_db[cumulative_collec].aggregate([{"$match":{"clientID":client_id}},{"$group":{"_id":"$symbol","quantity":{"$sum":"$quantity"}}} ])
                for i in match:
                    # print(client_id," ",i)
                    post={
                        "clientID":client_id,
                        "symbol":i["_id"],
                        "quantity":i["quantity"]
                    }
                    # print(post)
                    savedata(post)


check_data()
