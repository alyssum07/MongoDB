from pymongo import MongoClient
import datetime
import time
import csv

def margin_rem(sum_q,amt):
    r_margin=(((abs(sum_q)/20)*5000)/amt)*100
    return r_margin

def max_margin(match_Id,quant):
    values.setdefault(match_Id,[]).append(abs(quant))
    max_margin_dic.update(values)
    print(max_margin_dic)

def amount():
    reader = csv.reader(open('clientAmt.txt', 'r'))
    for row in reader:
        Id, amt = row
        id_dic[Id] = int(amt)

id_dic={}
max_margin_dic={}
values={}

def margin(post,date):
    try:
        client = MongoClient()
        db = client['net_sell_margin']
        collec = f"net_sell_margin_{date}"
        db.create_collection(collec)
        print(f"Created New margin Collection '{collec}'")
        #db[collec].insert(post)
        #print(post)

    except Exception:
        pass

    finally:
        #print(post)
        match = db[collec].find_one({"clientID":post['clientID']})
        if match:
            #print(match)
            match_Id=match['clientID']
            amount()
            # if(match_Id not in id_dic.keys()):
            #     reader = csv.reader(open('clientAmt.txt', 'r'))
            #     for row in reader:
            #         Id, amt = row
            #         id_dic[Id] = amt
            #     # print("client's Id ",match_Id)
            #     # amt=int(input("enter above client's amount "))
            #     # id_dic.update({match_Id:amt})

            
            if(post['quantity']<=0):
                new_quantity=match['quantity']+post['quantity']
                max_margin(match_Id,post['quantity'])
                rem=margin_rem(new_quantity,id_dic[match_Id])
                db[collec].update({'_id': match['_id']}, {"$set": {"quantity":new_quantity,"Margin remaining(%)":rem}})


            #sum_q=sum(max_margin_dic[match_Id])
            #print(sum_q)
            Max=max(max_margin_dic[match_Id])
            # rem=margin_rem(sum_q,id_dic[match_Id])
            Final_max=margin_rem(Max,id_dic[match_Id])
            db[collec].update({'_id': match['_id']}, {"$set": {"Max Margin Used(%)":Final_max}})
            
        else:
            #print(post)
            post_Id=post['clientID']
            if(post['quantity']<=0):
                max_margin(post_Id,post['quantity'])
            db[collec].insert(post)
            print("value added in margin")
            

#print(id_dic)

    
