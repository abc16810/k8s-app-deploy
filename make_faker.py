import random

from faker import Faker

fake = Faker(locale='zh_CN') 



  

def make_data(num=1000):
    for _ in range(num):
        name = fake.name()
        age = random.randint(0,50)
        socre = random.randint(50,100)
        yield name, age, socre



data = make_data(num=100)
with open("aa.txt", "a+", encoding='utf-8') as f:
    for x in data:
        f.write(' '.join([ str(k) for k in x ]))  
        f.write("\t\n")      
    # print(' '.join([ str(k) for k in x ]))
