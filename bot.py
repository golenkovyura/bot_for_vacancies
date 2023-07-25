import os
import sqlite3
import datetime

from openpyxl import load_workbook
import asyncio
import telebot
from dotenv import load_dotenv

from parsing import main, start_parsing, vacancies
from background import keep_alive

load_dotenv()

TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN)
title = "Вакансии Aviasales"


def convert_to_binary_data(filename):
    """Преобразование данных в двоичный формат."""
    with open(filename, 'rb') as file:
        blob_data = file.read()
    return blob_data


def update_data(user_id, vacancies_file, date_of_last_check):
    """
    Функция обновляет данные в БД у соотвутствующего пользователя,
    вносит новый файл с вакансиями и дату+время последнего обновления БД.
    """
    try:
        sqlite_connection = sqlite3.connect('sqlite_vacancies.db')
        cursor = sqlite_connection.cursor()

        # Обновляем дату
        cursor.execute("""
                        UPDATE vacancies_for_users SET
                        date_of_last_check = ?
                        WHERE user_id = ?
                       """, (date_of_last_check, user_id))

        # Обновляем файл с вакансиями
        cursor.execute("""
                        UPDATE vacancies_for_users SET
                        vacancies_file = ?
                        WHERE user_id = ?
                       """, (vacancies_file, user_id))

        sqlite_connection.commit()
        cursor.close()
        print('\nДанные обновлены!')
    except sqlite3.Error as error:
        print("Ошибка при работе с SQLite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def insert_data(user_id, first_name, last_name, user_name):
    """Функция добавления данных в БД."""
    try:
        sqlite_connection = sqlite3.connect('sqlite_vacancies.db')
        cursor = sqlite_connection.cursor()
        print("Подключен к SQLite")

        cursor.execute("""CREATE TABLE IF NOT EXISTS vacancies_for_users
                  (user_id INTEGER PRIMARY KEY,
                   first_name TEXT,
                   last_name TEXT,
                   user_name TEXT,
                   vacancies_file BLOB,
                   date_of_last_check TEXT)
               """)

        check = cursor.execute(f"""
                       SELECT * FROM vacancies_for_users
                       WHERE user_id={user_id}
                       """)

        sqlite_insert_query = """INSERT INTO vacancies_for_users
                                  (user_id, first_name, last_name, user_name)
                                  VALUES (?, ?, ?, ?)"""

        data_tuple = (user_id, first_name, last_name, user_name)
        # проверяем, есть ли в таблице запись по пользователю
        if not check.fetchall():
            cursor.execute(sqlite_insert_query, data_tuple)
            sqlite_connection.commit()
            print("Данные успешно добавлены")
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при работе с SQLite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


@bot.message_handler(commands=['help', 'start'])
def start(message):
    """
    Когда пользователь выпоняет команду /start, в БД вносится
    основная информация о нем.
    """
    bot.send_message(message.chat.id,
                     f'Привет, {message.from_user.first_name}')
    bot.send_message(message.chat.id, 'Выбери в меню команду')
    insert_data(message.from_user.id, message.from_user.first_name,
                message.from_user.last_name, message.from_user.username)


@bot.message_handler(commands=['vacancies'])
def send_vacancies(message):
    """
    Выполняя команду /vacancies пользователю отправляется его
    личный файл с вакансиями, при этом записывается информация в БД.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    id = message.from_user.id
    bot.send_message(message.chat.id, 'Идет сбор данных...')
    main(user_id=id)

    vacancies_file = convert_to_binary_data(
        f'vacancies for users/Вакансии Aviasales_{id}.xlsx'
        )
    date_of_last_check = str(datetime.datetime.today())

    update_data(user_id=id, vacancies_file=vacancies_file,
                date_of_last_check=date_of_last_check)

    with open(f'vacancies for users/{title}_{id}.xlsx', "rb") as file:
        bot.send_document(message.chat.id, file)


@bot.message_handler(commands=['check_new_vacancies'])
def check_new_vacancies(message):
    """
    Функция проверяет наличие новых вакансий для пользователя,
    сравнивая с послденим сохраненным файлом в БД.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot.send_message(message.chat.id, 'Проверка новых вакансий...')
    start_parsing()

    # множество новых, только что спарсенных вакансий
    new_vacancies = set([dct.get('Название вакансии') for dct in vacancies])
    try:
        # открываем последний сохраненный файл с вакансиями для пользователя
        wb = load_workbook(
            f'vacancies for users/{title}_{message.from_user.id}.xlsx'
            )
        ws = wb[title]

        old_vacancies = [row[0] for row in ws.values]
        old_vacancies = set(old_vacancies[1:])

        # проверяем и наличие новых вакансий и те, которые перестали 
        # быть актуальны
        if new_vacancies == old_vacancies:
            bot.send_message(message.chat.id, 'Новых вакансий нет')

        if new_vacancies - old_vacancies:
            bot.send_message(
                message.chat.id,
                f'Новыe вакансии:\n{new_vacancies - old_vacancies}')

        if old_vacancies - new_vacancies:
            bot.send_message(
                message.chat.id,
                f'Удаленные вакансии:\n{old_vacancies - new_vacancies}')

    except FileNotFoundError:
        # если у пользователя нет сохраненного файла с вакансиями, то парсим их
        # и отправляем ему, сразу обновляя информацию в БД
        bot.send_message(message.chat.id,
                         'Для вас еще не были собраны вакансии')
        bot.send_message(message.chat.id, 'Вот последнее обновление:')
        send_vacancies(message)


# keep_alive()


if __name__ == '__main__':
    bot.infinity_polling()
