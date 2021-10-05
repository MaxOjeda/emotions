import pandas as pd
import numpy as np
import json
import altair as alt
from vega_datasets import data


def json_all_emotions(df_tweets, emotions_dic):
    lista = []
    df_group_date = df_tweets.groupby(['date']).sum()
    df_group_date['n_tweets'] = df_tweets.groupby(['date']).count()['text']

    for date, row in df_group_date.iterrows():
        for emotion in emotions_dic.keys():
            data = {'emocion': emotion,
                    'periodo': str(date),
                    'count': row[emotions_dic[emotion]]/row['total palabras'],
                    'cantidad': str(row['n_tweets'])}

            lista.append(data)
    return lista


def json_top_emotions(count, source, times, start_top, end_top, top_n):
    top_orden = []

    for i in range(0, len(times)):
        cant_tweets = count['count'][i]

        m = source['count'][int(start_top[i]):int(end_top[i])]
        g = sorted(list(set(m)), reverse=True)[:top_n]
        m = list(m)

        for u in range(len(g)):
            pos = m.index(g[u])

            data = {'emocion': source['emocion'][pos],
                    'periodo': str(times[i]),
                    'count': source['count'][pos],
                    'cantidad': str(cant_tweets)}

            top_orden.append(data)

    return top_orden


def streamgraph_all(title, html_title, source_json):
    title = title
    alt_X1 = alt.X('periodo:T', axis=alt.Axis(
        domain=False, tickSize=0, tickCount=25, title="Periodos"))
    alt_Y1 = alt.Y('count:Q', stack='center', axis=None)
    alt_Color = alt.Color('emocion:N', scale=alt.Scale(scheme='category10'),
                          legend=alt.Legend(title="Emociones",
                                            labelFontSize=14,
                                            titleFontSize=16))

    upper = alt.Chart(source_json).mark_area(interpolate="basis").encode(
        alt_X1, alt_Y1, alt_Color, tooltip=[alt.Tooltip('emocion')]).properties(width=700, title=title).interactive()

    alt_X2 = alt.X('periodo:T', axis=alt.Axis(
        domain=True, tickSize=0, tickCount=20, title=" "))
    alt_Y2 = alt.Y('cantidad:Q', axis=alt.Axis(
        domain=False,  tickCount=3, title="Cantidad"))

    lower = alt.Chart(source_json).mark_area().encode(
        alt_X2, alt_Y2
    ).properties(
        width=700, height=100
    )
    tot = alt.vconcat(upper, lower).configure_title(
        fontSize=20, dx=80).configure_legend(symbolSize=300)
    tot.save(html_title)


def stream_emotions(title_all, title_top, html_all, html_top, emolex_df, df, top_n):

    # vector de timestamps (fecha de periodos)
    data_aux = pd.DataFrame()
    data_aux['dia'] = df['date'].unique()
    times = list(data_aux['dia'])

    # Calculamos la cantidad de tweets que hay en un periodo (día)
    count = ((df['date'])
             .dt.floor('d')
             .value_counts()
             .rename_axis('date')
             .reset_index(name='count'))

    count.sort_values(by=['date'], inplace=True, ascending=True)

    # reseteamos el índice del dataframe (pues en el paso anterior no se guardan en orden)
    count = count.reset_index(drop=True)

    time_slice = list(count['count'])

    ###################### Stream Graph de todas las emociones ###############################################

    # vectores de separación time slices
    start = [0]
    end = np.cumsum(time_slice)
    start = np.append(start, end[0:len(end)-1])

    # vector de emociones en inglés y español respectivamente
    emotions = emolex_df.emocion.unique()
    emociones_dic = {'ENOJO': 'anger', 'EXPECTACIÓN': 'anticipation', 'DISGUSTO': 'disgust', 'MIEDO': 'fear',
                     'ALEGRÍA': 'joy', 'TRISTEZA': 'sadness', 'SORPRESA': 'surprise',  'CONFIANZA': 'trust'}

    c = json_all_emotions(df, emociones_dic)

    # guardar como json el diccionario creado
    with open('emotions_all.json', 'w') as fp:  # se puede cambiar el nombre
        json.dump(c, fp)

    source = pd.read_json('emotions_all.json')

    streamgraph_all(
        title_all, html_all, source)

    ####################### Stream Graph de las Top n emociones ######################################

    # Aquí creo los vectores para separar los time slices, como usamos el dataframe 'source' que me muestra las intensidades
    # de las 8 emociones por periodo, para decidir las top n emociones, va tomando cada 8 filas de la tabla
    # quizás me di muchas vueltas y hay mejor forma de hacerlo jaja

    h = 0
    j = 8
    start_top = [0]
    end_top = [8]
    for t in range(int(len(source)/8)):
        st = h + 8
        en = j + 8

        start_top = np.append(start_top, str(st))
        end_top = np.append(end_top, str(en))

        h = st
        j = en

    start_top = start_top[:len(start_top)-1]
    end_top = end_top[:len(end_top)-1]

    d = json_top_emotions(count, source, times, start_top, end_top, top_n)

    # guardar como json el diccionario creado
    with open('emotions_top.json', 'w') as fp:
        json.dump(d, fp)

    source1 = pd.read_json('emotions_top.json')

    streamgraph_all(
        title_top, html_top, source1)
