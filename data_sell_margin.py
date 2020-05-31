from pymongo import MongoClient
import pymongo
import datetime
import time
from net_sell_margin2 import margin 

date = datetime.date.today()
netq_collec = f'symphonyorder_netquantity_{date}'
netmargin_collec=f'net_sell_margin_{date}'

connection = MongoClient('localhost', 27017)

try:
    netq_db = connection['symphonyorder_netquantity']

except Exception:
    print("ERROR: Mongo Connection Error123")

try:
    net_margin_db = connection['net_sell_margin']
    net_margin_db[netmargin_collec].drop()
    print('net_margin_collec deleted')
except:
    pass


def check_data():
    id_dict = {}
    count = netq_db[netq_collec].count()
    
    #print(count)
    #while True:

    neworders = netq_db[netq_collec].find().sort([('_id', -1)]).limit(count)
    for order in neworders:

        order_id = order['_id']
        clientID = order['clientID']
        quantity = order['quantity']

        if order_id not in id_dict.keys():
            id_dict.update({order_id: quantity})
            
            try:
                print("run")
                post={"clientID":clientID,
                        "quantity":quantity,
                        }
                margin(post,date)
                #print(post)
            except Exception as e:
                print(e)
                continue

        else:
            # print(match)
            if (quantity != id_dict[order_id]):
                print(quantity)
                try:
                    post = {
                        "clientID": clientID,
                        "quantity": quantity,
                    }
                    # print(quantity-,'+',id_dict[order_id])
                    margin(post, date)
                    id_dict.update({order_id: quantity})
                except Exception as e:
                    print (e)
                    continue
    #time.sleep(60)

while True:
    check_data()
    time.sleep(30)
    net_margin_db[netmargin_collec].drop()
