from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.params import Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
os.environ["KERAS_BACKEND"] = "tensorflow"
import pandas as pd
import tensorflow as tf
import numpy as np
import keras
from keras.layers import TextVectorization
from langdetect import detect
from googletrans import Translator, LANGUAGES
from config.db import connect_to_mongodb
from Recommendation_Rating import setRatingList,getRecommend
import random
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from export_manga import export_datas

app = FastAPI()

class Toxicity(BaseModel):
    comment_content: str

class Rating(BaseModel):
    userId: str
    mangaId: str
    rate: float

class Recommendation(BaseModel):
    userId: str


# Khởi tạo các biến để lưu trữ vectorizer và ml_models
vectorizer = None
ml_models = None
db = None
collection = None

def load_model():
    global vectorizer, ml_models
    # Load vectorizer
    df = pd.read_csv('./jigsaw-toxic-comment-classification-challenge/train.csv/train2.csv')
    X = df['comment_text']
    MAX_FEATURES = 200000
    vectorizer = TextVectorization(max_tokens=MAX_FEATURES,
                                    output_sequence_length=1800,
                                    output_mode='int')
    vectorizer.adapt(X.values)
        
    # Load ML model
    ml_models = keras.layers.TFSMLayer("./notebooks/toxicity", call_endpoint="serving_default")
        
    print("Vectorizer and ML model loaded successfully.")
def connectDB():
    global db, collection
    db = connect_to_mongodb()
    collection = db["behaviors"]
    print("MongoDB connected successfully!")

# def start_scheduler():
#     # Tạo một instance của scheduler
#     scheduler = BlockingScheduler()

#     # trigger = CronTrigger(day=1, hour=0, minute=0, second=1)
#     trigger = IntervalTrigger(minutes=1)

#     scheduler.add_job(export_mangas, trigger, args=[scheduler])

#     scheduler.start()

connectDB()

# **************************** 1 trong file khbt.txt ****************************


def translate_to_english(text, source_language=None):
    translator = Translator()
    if source_language:
        translation = translator.translate(text, src=source_language, dest='en')
    else:
        translation = translator.translate(text, dest='en')
    return translation.text

@app.get("/")
async def root():
    return JSONResponse("hello")

@app.post("/toxicity")
async def toxicity(toxic: Toxicity):
    try:
        language = detect(toxic.comment_content)
        comment = translate_to_english(toxic.comment_content, language)
        input_str = vectorizer(comment)
        prediction = (ml_models(np.expand_dims(input_str,0))['output_0'].numpy()> 0.5).astype(int)
        labels = ['toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate','language']
        toxicity_predictions = {label: float(value) for label, value in zip(labels, prediction[0])}
        toxicity_predictions['language'] = language
        levelWarning = {'level': None, 'content': None}
        if toxicity_predictions['severe_toxic'] == 1 or toxicity_predictions['threat'] == 1:
            levelWarning['level'] = 3
            levelWarning['content'] = "Ngôn từ của bạn vi phạm nghiêm trọng quy tắc cộng đòng, tài khoản của bạn sẽ bị khóa trong vòng 30 ngày sau 1 tiếng kể từ nhận thông báo"
        elif toxicity_predictions['insult'] == 1 or toxicity_predictions['identity_hate'] == 1:
            levelWarning['level'] = 1.5
        elif toxicity_predictions['toxic'] == 1 or toxicity_predictions['obscene'] == 1:
            levelWarning['level'] = 1
        else:
            levelWarning['level'] = 0
        return JSONResponse(levelWarning)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/rating")
async def rating(rating: Rating):
    try:
        setRatingList(rating)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    
@app.get("/recommend")
async def rating(userId: str):
    try:
        recommend = Recommendation(userId=userId)
        list = getRecommend(recommend)
        random.shuffle(list)
        return JSONResponse(list)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# @app.post("/cover")
# async def cover():
#     try:
#         export_datas()
#     except Exception as e:
#         raise HTTPException(status_code=404, detail=str(e))