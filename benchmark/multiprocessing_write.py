import os
path_this = os.path.dirname(os.path.abspath(__file__))
n_workers = max(1,os.cpu_count()-3) if os.cpu_count()>32 else max(1,os.cpu_count()//2-3)

def worker(name,day_from,day_to):
    import sqless
    db = sqless.DB(f"{path_this}/test.sqlite")
    table = db['dairy']
    items = {item['key']:item['cnt'] for item in table.iter(f"key >= D{day_from} and key <= D{day_to}")}
    payloads = {
        f"D{day}": {'day':day, 'writer':name,'cnt':items.get(f"D{day}",0)+1}
        for day in range(day_from,day_to+1)
    }
    table.upsert(payloads)

#==========================================#
# Multiprocessing Tool
#==========================================#
from multiprocessing import Pool
import tqdm
def worker_wrapper(p):
    try:
        data = p['f'](**p.get('kwargs', {}))
        p['suc']=True
        p['data']=data
    except Exception as e:
        p['suc']=False
        p['data']=f"error: {e}"
    return p
def batch(payloads, pool_size=n_workers, chunksize=None, desc=None):
    if chunksize is None: chunksize = min(max(1,int(len(payloads)/8/pool_size)),100)
    with Pool(pool_size) as pool:
        for ret in tqdm.tqdm(pool.imap_unordered(worker_wrapper, payloads, chunksize=chunksize), total=len(payloads), desc=desc):
            yield ret



if __name__ == '__main__':
    import time
    for ret in batch([
        {'key':'worker_01','f':worker,'kwargs':{'name':'worker_01','day_from':1100,'day_to':1600}},
        {'key':'worker_02','f':worker,'kwargs':{'name':'worker_02','day_from':1100,'day_to':1600}},
        {'key':'worker_03','f':worker,'kwargs':{'name':'worker_03','day_from':1200,'day_to':1700}},
        {'key':'worker_04','f':worker,'kwargs':{'name':'worker_04','day_from':1200,'day_to':1700}},
        {'key':'worker_05','f':worker,'kwargs':{'name':'worker_05','day_from':1300,'day_to':1800}},
        {'key':'worker_06','f':worker,'kwargs':{'name':'worker_06','day_from':1300,'day_to':1800}},
        {'key':'worker_07','f':worker,'kwargs':{'name':'worker_07','day_from':1400,'day_to':1900}},
        {'key':'worker_08','f':worker,'kwargs':{'name':'worker_08','day_from':1400,'day_to':1900}},
        {'key':'worker_09','f':worker,'kwargs':{'name':'worker_09','day_from':1500,'day_to':2000}},
        {'key':'worker_10','f':worker,'kwargs':{'name':'worker_10','day_from':1500,'day_to':1000}},
        {'key':'worker_11','f':worker,'kwargs':{'name':'worker_11','day_from':2100,'day_to':2600}},
        {'key':'worker_12','f':worker,'kwargs':{'name':'worker_12','day_from':2100,'day_to':2600}},
        {'key':'worker_13','f':worker,'kwargs':{'name':'worker_13','day_from':2200,'day_to':2700}},
        {'key':'worker_14','f':worker,'kwargs':{'name':'worker_14','day_from':2200,'day_to':2700}},
        {'key':'worker_15','f':worker,'kwargs':{'name':'worker_15','day_from':2300,'day_to':2800}},
        {'key':'worker_16','f':worker,'kwargs':{'name':'worker_16','day_from':2300,'day_to':2800}},
        {'key':'worker_17','f':worker,'kwargs':{'name':'worker_17','day_from':2400,'day_to':2900}},
        {'key':'worker_18','f':worker,'kwargs':{'name':'worker_18','day_from':2400,'day_to':2900}},
        {'key':'worker_19','f':worker,'kwargs':{'name':'worker_19','day_from':2500,'day_to':3000}},
        {'key':'worker_20','f':worker,'kwargs':{'name':'worker_20','day_from':2500,'day_to':3000}},
        {'key':'worker_21','f':worker,'kwargs':{'name':'worker_21','day_from':3100,'day_to':3600}},
        {'key':'worker_22','f':worker,'kwargs':{'name':'worker_22','day_from':3100,'day_to':3600}},
        {'key':'worker_23','f':worker,'kwargs':{'name':'worker_23','day_from':3200,'day_to':3700}},
        {'key':'worker_24','f':worker,'kwargs':{'name':'worker_24','day_from':3200,'day_to':3700}},
        {'key':'worker_25','f':worker,'kwargs':{'name':'worker_25','day_from':3300,'day_to':3800}},
        {'key':'worker_26','f':worker,'kwargs':{'name':'worker_26','day_from':3300,'day_to':3800}},
        {'key':'worker_27','f':worker,'kwargs':{'name':'worker_27','day_from':3400,'day_to':3900}},
        {'key':'worker_28','f':worker,'kwargs':{'name':'worker_28','day_from':3400,'day_to':3900}},
        {'key':'worker_29','f':worker,'kwargs':{'name':'worker_29','day_from':3500,'day_to':4000}},
        {'key':'worker_30','f':worker,'kwargs':{'name':'worker_30','day_from':3500,'day_to':4000}},
        {'key':'worker_31','f':worker,'kwargs':{'name':'worker_31','day_from':1100,'day_to':1600}},
        {'key':'worker_32','f':worker,'kwargs':{'name':'worker_32','day_from':1100,'day_to':1600}},
        {'key':'worker_33','f':worker,'kwargs':{'name':'worker_33','day_from':1200,'day_to':1700}},
        {'key':'worker_34','f':worker,'kwargs':{'name':'worker_34','day_from':1200,'day_to':1700}},
        {'key':'worker_35','f':worker,'kwargs':{'name':'worker_35','day_from':1300,'day_to':1800}},
        {'key':'worker_36','f':worker,'kwargs':{'name':'worker_36','day_from':1300,'day_to':1800}},
        {'key':'worker_37','f':worker,'kwargs':{'name':'worker_37','day_from':1400,'day_to':1900}},
        {'key':'worker_38','f':worker,'kwargs':{'name':'worker_38','day_from':1400,'day_to':1900}},
        {'key':'worker_39','f':worker,'kwargs':{'name':'worker_39','day_from':1500,'day_to':1000}},
        {'key':'worker_40','f':worker,'kwargs':{'name':'worker_40','day_from':1500,'day_to':1000}},
        {'key':'worker_41','f':worker,'kwargs':{'name':'worker_41','day_from':2100,'day_to':2600}},
        {'key':'worker_42','f':worker,'kwargs':{'name':'worker_42','day_from':2100,'day_to':2600}},
        {'key':'worker_43','f':worker,'kwargs':{'name':'worker_43','day_from':2200,'day_to':2700}},
        {'key':'worker_44','f':worker,'kwargs':{'name':'worker_44','day_from':2200,'day_to':2700}},
        {'key':'worker_45','f':worker,'kwargs':{'name':'worker_45','day_from':2300,'day_to':2800}},
        {'key':'worker_46','f':worker,'kwargs':{'name':'worker_46','day_from':2300,'day_to':2800}},
        {'key':'worker_47','f':worker,'kwargs':{'name':'worker_47','day_from':2400,'day_to':2900}},
        {'key':'worker_48','f':worker,'kwargs':{'name':'worker_48','day_from':2400,'day_to':2900}},
        {'key':'worker_49','f':worker,'kwargs':{'name':'worker_49','day_from':2500,'day_to':3000}},
        {'key':'worker_50','f':worker,'kwargs':{'name':'worker_50','day_from':2500,'day_to':3000}},
        {'key':'worker_51','f':worker,'kwargs':{'name':'worker_51','day_from':3100,'day_to':3600}},
        {'key':'worker_52','f':worker,'kwargs':{'name':'worker_52','day_from':3100,'day_to':3600}},
        {'key':'worker_53','f':worker,'kwargs':{'name':'worker_53','day_from':3200,'day_to':3700}},
        {'key':'worker_54','f':worker,'kwargs':{'name':'worker_54','day_from':3200,'day_to':3700}},
        {'key':'worker_55','f':worker,'kwargs':{'name':'worker_55','day_from':3300,'day_to':3800}},
        {'key':'worker_56','f':worker,'kwargs':{'name':'worker_56','day_from':3300,'day_to':3800}},
        {'key':'worker_57','f':worker,'kwargs':{'name':'worker_57','day_from':3400,'day_to':3900}},
        {'key':'worker_58','f':worker,'kwargs':{'name':'worker_58','day_from':3400,'day_to':3900}},
        {'key':'worker_59','f':worker,'kwargs':{'name':'worker_59','day_from':3500,'day_to':4000}},
        {'key':'worker_60','f':worker,'kwargs':{'name':'worker_60','day_from':3500,'day_to':4000}},
        {'key':'worker_61','f':worker,'kwargs':{'name':'worker_61','day_from':3400,'day_to':3900}},
        {'key':'worker_62','f':worker,'kwargs':{'name':'worker_62','day_from':3400,'day_to':3900}},
        {'key':'worker_63','f':worker,'kwargs':{'name':'worker_63','day_from':3500,'day_to':4000}},
        {'key':'worker_64','f':worker,'kwargs':{'name':'worker_64','day_from':3500,'day_to':4000}},
    ],pool_size=n_workers):
        if not ret['suc']:
            print(f"[{time.strftime('%Y%m%d-%H%M%S')}] KEY: {ret['key']} Error: {ret['data']}")