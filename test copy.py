import os
os.environ["KERAS_BACKEND"] = "tensorflow"
import pandas as pd
import tensorflow as tf
import numpy as np
import keras
from keras.layers import TextVectorization
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dropout, Bidirectional, Dense, Embedding
from tensorflow.keras.metrics import Precision, Recall, CategoricalAccuracy
import keras
import tensorflow as tf

df = pd.read_csv('../jigsaw-toxic-comment-classification-challenge/train.csv/train.csv')
df.head()
X = df['comment_text']
y = df[df.columns[2:]].values

MAX_FEATURES = 200000
vectorizer = TextVectorization(max_tokens=MAX_FEATURES,
                               output_sequence_length=1800,
                               output_mode='int')
vectorizer.adapt(X.values)

vectorizer('Hello world, life is great')[:5]

vectorized_text = vectorizer(X.values)

vectorized_text

dataset = tf.data.Dataset.from_tensor_slices((vectorized_text, y))
dataset = dataset.cache()
dataset = dataset.shuffle(160000)
dataset = dataset.batch(16)
dataset = dataset.prefetch(8)

batch_X, batch_y =  dataset.as_numpy_iterator().next()

train = dataset.take(int(len(dataset)*.7))
val = dataset.skip(int(len(dataset)*.7)).take(int(len(dataset)*.2))
test = dataset.skip(int(len(dataset)*.9)).take(int(len(dataset)*.1))

train_g = train.as_numpy_iterator()

train_g.next()

model = Sequential()
# Create the embedding layer 
model.add(Embedding(MAX_FEATURES+1, 32))
# Bidirectional LSTM Layer
model.add(Bidirectional(LSTM(32, activation='tanh')))
# Feature extractor Fully connected layers
model.add(Dense(128, activation='relu'))
model.add(Dense(256, activation='relu'))
model.add(Dense(128, activation='relu'))
# Final layer 
model.add(Dense(6, activation='sigmoid'))

model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
model.summary()
history = model.fit(train, epochs=5, validation_data=val)



input_text = vectorizer('You freaking suck! I am going to hit you.')
res = model.predict(np.array([input_text]))

pre = Precision()
re = Recall()
acc = CategoricalAccuracy()


for batch in test.as_numpy_iterator(): 
    # Unpack the batch 
    X_true, y_true = batch
    # Make a prediction 
    yhat = model.predict(X_true)
    
    # Flatten the predictions
    y_true = y_true.flatten()
    yhat = yhat.flatten()
    
    pre.update_state(y_true, yhat)
    re.update_state(y_true, yhat)
    acc.update_state(y_true, yhat)

print(f'Precision: {pre.result().numpy()}, Recall:{re.result().numpy()}, Accuracy:{acc.result().numpy()}')

loaded_model = keras.layers.TFSMLayer("toxicity1", call_endpoint="serving_default")

input_str = vectorizer('Fucking your ass, bitch!')

input_tensor = np.expand_dims(input_str, 0).astype(np.float32)
prediction = (loaded_model(input_tensor)['output_0'].numpy() > 0.5).astype(int)