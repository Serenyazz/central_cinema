import datetime
import telebot
import os
from central_cinema import calculate_metrics

today = datetime.datetime.now().strftime('%Y-%m-%d')

token = os.environ['TELEGRAM_TOKEN']
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
user_id = '795467906'

message = calculate_metrics(pg_user, pg_password, pg_host, today)

bot = telebot.TeleBot(token)

bot.send_message(user_id, message)

# line_graph
image_path = 'Кол-во триалов_line_graph.png' 
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

image_path = 'cash in_line_graph.png' 
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

image_path = 'уникальное кол-во смотрящих_line_graph.png' 
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

# combined graph
image_path = 'Кол-во оплат_combined_graph.png'  
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

image_path = 'Среднее время просмотра + досматриваемость_combined_graph.png' 
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

image_path = 'cac_ltv_ltr.png'
image = open(image_path, 'rb')
bot.send_photo(user_id, image)

# optional

image_path = 'partner_activity.png'
image = open(image_path, 'rb')
bot.send_photo(user_id, image)