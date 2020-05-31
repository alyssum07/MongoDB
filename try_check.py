from pymongo import MongoClient
import pymongo
import datetime
import time
import winsound
import playsound

date = datetime.date.today()
raw_collec = f'final_response_{date}'

connection = MongoClient('localhost', 27017)

try:
    db = connection['final_response']
except Exception:
    print("ERROR: Mongo Connection Error123")

def sound():
                
    if(status=="Rejected"):
        playsound.playsound("alarm-sound (5).mp3")
        time.sleep(0.5)
    elif(status=="Cancelled"):
        playsound.playsound("Wake-up-sounds (5).mp3")
        time.sleep(0.5)
    else:
        pass

def check_data():
    global status
    id_list=[]
    count = db[raw_collec].count()
    print(count)
    while True:
        neworders = db[raw_collec].find().sort([('_id', -1)]).limit(count)
    
        for order in neworders:
            order_id=order['_id']
            if order_id not in id_list:
                id_list.append(order_id)
                status = order['orderStatus']
                print(status)
                sound()

           
check_data()
