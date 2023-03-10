from flask import Flask,render_template,request,redirect
import pandas as pd
import pickle
import numpy as np
app=Flask(__name__)

model=pickle.load(open("/Users/senze/Documents/GitHub/CarPricePrediction/application/RFR_carprice.pkl","rb"))
car_p=pd.read_csv('/Users/senze/Documents/GitHub/CarPricePrediction/application/cleaned_cp_final.csv')

@app.route('/')
def index():
    names = sorted(car_p['name'].unique())
    year = sorted(car_p['year'].unique(),reverse=True)
    brands = sorted(car_p['brand'].unique())
    km_drive = sorted(car_p['km_drive'].unique())

    return render_template('index.html',names=names,year=year,brands=brands,km_drive=km_drive)

@app.route('/predict', methods=['POST'])
def predict():
    brand=request.form.get("brand")
    name=request.form.get("names")
    year=int(request.form.get("year"))
    km_drive=int(request.form.get("km_drive"))
    car_old=2023-year
    print(brand,name,year,km_drive,car_old)

    prediction=model.predict(pd.DataFrame([[name, year, brand, km_drive, car_old]],columns=["name","year","brand","km_drive","car_old"]))
    price=abs(prediction[0])
    return str(np.round(price,2))


if __name__=="__main__":
    app.run(debug=True)