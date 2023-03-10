import json
from datetime import datetime
import pandas as pd
import bs4
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def get_carprice():
    page = 1
    name_list =[]
    price_list = []
    year_list = []
    brand_list = []
    mile_list = []
    while page<=70:
        data= requests.get('https://www.priceusedcar.com/%E0%B8%A3%E0%B8%96%E0%B8%A1%E0%B8%B7%E0%B8%AD%E0%B8%AA%E0%B8%AD%E0%B8%87?page='+str(page))
        soup = bs4.BeautifulSoup(data.text)
        c=soup.find_all('div',{'class':'card-body'})
        for i in c:
            n=(i.find('a').text.split())
            strw=""
            for s in n[0:-2]:
                strw=strw+ ' '+ s
            name_list.append(strw)
            price_list.append(int(i.find('div',{'class':'d-inline-block mb-2 text-primary'}).text.split()[1].replace(',','')))
            y=i.find('a').text.split()[-1]
            if(y=="Prerunner" or y=="Premium"):
                year_list.append(None)
            else:
                year_list.append(i.find('a').text.split()[-1])
            brand_list.append(i.find('a').text.split()[0])
            m=i.find_all('span',{'class':'text-muted'})[2].text.replace(' กม.','').replace(',','')
            if(m=="" or (not(m.isnumeric()))):
                mile_list.append(0)
            else:
                mile_list.append(int(m))
        print('complete page number',page)
        page += 1
    table=pd.DataFrame([name_list,price_list,year_list,brand_list,mile_list]).transpose()
    table.columns =['name','price','year','brand','km_drive']
    table.set_index('name')
    table.to_csv("cp.csv",index=False)

def pyprint():
    df = pd.read_csv('cp.csv')
    print(df.to_string()) 

def pyprint2():
    df = pd.read_csv('cp1.csv')
    print(df.to_string()) 

def to_sql():
    engine=create_engine('postgresql://airflow:airflow@host.docker.internal:5432/DEMO1')
    df = pd.read_csv('cleaned_car.csv')
    df.to_sql("car_price"+str(datetime.now()),engine)

def get_carprice1():

    page = 1
    name_list =[]
    price_list = []
    year_list = []
    brand_list = []
    mile_list = []
    while page <=70:
        headers={'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        data= requests.get('https://www.thaicar.com/used-cars?page='+str(page),headers=headers)
        soup = bs4.BeautifulSoup(data.text)
        name=soup.find_all('h2',{'class':'card-title pull-left wrap'})
        year=soup.find_all('tr',{'class':'item item-year'})
        brand=soup.find_all('tr',{'class':'item item-transmission item-make'})
        km_drive=soup.find_all('tr',{'class':'item item-mileage'})
        price=soup.find_all('p',{'class':'card-price'})
        for i in name:
            #print(i.find('a').text)
            n=i.find('a').text.replace("\n","")
            name_list.append(n)
        for i in year:
            #print(i.find('td',{'class':'lbl'}).text)
            y=i.find('td',{'class':'lbl'}).text
            year_list.append(int(y))
        for i in brand:
            #print(i.find('td',{'class':'lbl'}).text)
            b=i.find('td',{'class':'lbl'}).text
            brand_list.append(b)
        for i in km_drive:
            #print(i.find('td',{'class':'lbl-lower'}).text.split()[0].replace(",",""))
            k=i.find('td',{'class':'lbl-lower'}).text.split()[0].replace(",","").replace("มากกว่า","0")
            mile_list.append(int(k))
        for i in price:
            #print(i.text.replace("\n","").replace("฿","").replace("* สอบถามราคา","0").replace(",","").replace(" ",""))
            p=i.text.replace("\n","").replace("฿","").replace("* สอบถามราคา","0").replace(",","").replace(" ","").replace("มากกว่า","")
            price_list.append(int(p))
        print("complete page"+str(page))
        page+=1
    table=pd.DataFrame([name_list,price_list,year_list,brand_list,mile_list]).transpose()
    table.columns =['name','price','year','brand','km_drive']
    table.set_index('name')
    table.to_csv("cp1.csv",index=False)

def merge_data():
    df = pd.read_csv('cp.csv')
    df2 = pd.read_csv('cp1.csv')
    df_merge=pd.concat([df, df2], ignore_index=True)
    df_merge.to_csv('merge_cp.csv', index=False)

def clean_data():
    car=pd.read_csv("merge_cp.csv")
    i=car[car.year=="REVO"].index
    car=car.drop(i)
    car=car.dropna()
    car['year']=car['year'].astype(str).astype(int)
    car.to_csv('test_ex.csv',index=False)

    car["car_old"]=2023-car["year"]
    car.drop(car[car['km_drive'] == 0].index,inplace=True)
    car.drop(car[car['price'] <= 1].index,inplace=True)
    car.to_csv('cleaned_car.csv',index=False)


def data_notcleantosql():
    engine=create_engine('postgresql://airflow:airflow@host.docker.internal:5432/Datanotclean')
    df = pd.read_csv('merge_cp.csv')
    df.to_sql("merge_cp"+str(datetime.now()),engine)



default_args = {
    'owner': 'sense',
    'start_date': days_ago(1),
    'email': ['sensesense475@gmail.com'],
}
with DAG('collect_data_project_dags',
         schedule_interval='@daily',
         default_args=default_args,
         description='collect data from site',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_carprice_today',
        python_callable=get_carprice
    )
    t2 = PythonOperator(
        task_id='print1',
        python_callable=pyprint
    )
    t3 = PythonOperator(
        task_id='get_carprice_today1',
        python_callable=get_carprice1
    )
    t4 = PythonOperator(
        task_id='print2',
        python_callable=pyprint2
    )

    t5 = PythonOperator(
        task_id='merge_csv',
        python_callable=merge_data
    )
    t6= PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    t7= PythonOperator(
        task_id='to_db',
        python_callable=to_sql
    )
    t8= PythonOperator(
        task_id='notclean_to_db',
        python_callable=data_notcleantosql
    )



[t1,t3] >>t2 >> t4 >> t5 >> t8 
t5 >> t6 >> t7