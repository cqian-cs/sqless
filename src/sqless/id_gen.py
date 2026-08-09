import time
import itertools
D = list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
G = itertools.cycle(itertools.product(D, repeat=4))
def new_id(__cache=[0,'']):
    t=int(time.time())
    if t!=__cache[0]:
        __cache[0]=t
        __cache[1]=''.join((D[(t>>30)&31], D[(t>>25)&31], D[(t>>20)&31],
                    D[(t>>15)&31], D[(t>>10)&31], D[(t>>5)&31], D[t&31]))
    return __cache[1]+''.join(next(G))

if __name__ == '__main__':
    for i in range(100):
        print(new_id())
        time.sleep(0.1)