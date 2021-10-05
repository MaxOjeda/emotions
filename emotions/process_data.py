import pandas as pd
import numpy as np


def emotion_processing(df_tweets, emolex_df):

    lista_palabras = emolex_df["español"].tolist()

    # comienza el procesamiento de emociones
    tweets_emotions = []
    total_words = []  # cantidad de palabras en el tweet
    word_count = []  # cantidad de palabras que generan emoción

    for _, row in df_tweets.iterrows():

        dic = {}
        dic["text"] = row.text
        dic["date"] = row.date
        text = row.text.split()

        w = 0

        for word in text:
            if word in lista_palabras:
                emociones = emolex_df[emolex_df["español"] == word]
                w = w + 1

                for _, emocion_row in emociones.iterrows():
                    emocion = emocion_row.emocion
                    valor = emocion_row.valor
                    if emocion not in dic:
                        dic[emocion] = 0
                    dic[emocion] += valor

      # Si hay por lo menos una emocion encontrada se hace append
        if len(dic) > 2:
            tweets_emotions.append(dic)

            # agregamos la cantidad total de palabras por texto
            m = len(text)
            total_words = np.append(total_words, m)

            # agregamos la cantidad de palabras que generan emocion por texto
            word_count = np.append(word_count, w)

    # guardamos el procesamiento en un DataFrame
    df = pd.DataFrame(tweets_emotions).fillna(0)
    df['total palabras'] = total_words
    df['palabras-emocion'] = word_count
    df = df.sort_values(['date'], ascending=True)
    df["date"] = pd.to_datetime(df["date"])

    return df
