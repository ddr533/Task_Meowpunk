import os
import sqlite3
from datetime import date, datetime

import pandas as pd
import psutil
from pandas import DataFrame


def create_empty_table() -> None:
    """Создает в БД таблицу possible_cheaters."""
    with sqlite3.connect('cheaters.db') as conn:
        c = conn.cursor()
        c.execute(
            '''
            CREATE TABLE IF NOT EXISTS possible_cheaters (
            timestamp INTEGER,
            player_id INTEGER,
            event_id INTEGER,
            error_id INTEGER,
            json_server TEXT,
            json_client TEXT
            )
            '''
        )


def load_data_on_date(date_: date, file_name: str, dtype: dict) -> DataFrame:
    """Загружает данные из *csv."""
    data = pd.read_csv(file_name, dtype=dtype)
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')
    data = data[data['timestamp'].dt.date == date_]
    return data


def process_data(date: str) -> None:
    """Создает запись в БД из данных файлов client.csv,
     server.csv согласно условиям."""
    date = datetime.strptime(date, '%Y-%m-%d').date()
    conn = sqlite3.connect('cheaters.db')
    c = conn.cursor()

    # Загрузка данных
    client_data = load_data_on_date(
        date,
        'client.csv',
        dtype={'timestamp': int, 'error_id': str, 'player_id': int,
               'description': str})
    server_data = load_data_on_date(
        date,
        'server.csv',
        dtype={'timestamp': int, 'event_id': int, 'error_id': str,
               'description': str}
    )

    # Создание датафрейма
    joined_data = pd.merge(
        client_data,
        server_data,
        on='error_id',
        suffixes=('_client', '_server'))

    # Исключение записей с player_id из таблицы cheaters c ban_time раньше date
    c.execute('SELECT player_id FROM cheaters WHERE ban_time < ?', (date,))
    banned_players = c.fetchall()
    banned_ids = (player[0] for player in banned_players)
    filtered_data = joined_data[~joined_data['player_id'].isin(banned_ids)]

    # Запись данных в таблицу possible_cheaters
    filtered_data = filtered_data[
        ['timestamp_server',
         'player_id',
         'event_id',
         'error_id',
         'description_server',
         'description_client']
    ]
    filtered_data.columns = [
        'timestamp',
        'player_id',
        'event_id',
        'error_id',
        'json_server',
        'json_client'
    ]
    filtered_data.to_sql('possible_cheaters', conn, if_exists='append',
                         index=False)

    conn.commit()
    conn.close()


def main() -> None:
    date = input('Введите дату в формат '
                 '"Год-месяц-день", например, 2021-03-15\n')
    if not os.path.exists('client.csv') or not os.path.exists('server.csv'):
        print("Отсутствуют необходимые файлы.")
        return
    try:
        create_empty_table()
        process_data(date)
    except Exception as e:
        print(e)
    else:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_usage = memory_info.rss / 1024 / 1024
        print('Данные успешно загружены.')
        print(f'Потребление памяти: {memory_usage:.2f} MB')


if __name__ == '__main__':
    main()
