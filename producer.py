import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
import pika
import os
from dotenv import load_dotenv

result = load_dotenv('params.env')

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Загрузка переменных окружения
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
temp = os.getenv("rabbitmq_port")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = os.getenv("QUEUE_NAME")


def get_absolute_url(base_url, link):
    """Конвертирует относительные ссылки в абсолютные"""
    if link.startswith("http"):
        return link

    if base_url.endswith("/") and link.startswith("/"):
        return base_url.rstrip("/") + link
    else:
        return base_url + link


async def extract_links(url):
    """Извлекает ссылки и медиа с указанного URL"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                links = []
                for tag in soup.find_all(["a", "img", "video", "audio"]):
                    link = tag.get("href") or tag.get("src")
                    if link:
                        links.append(get_absolute_url(url, link))
                        logging.info(f"Найдено: {tag.name}, {link}")
                return links
            else:
                logging.warning(f"Ошибка загрузки: {url}")
                return []


def send_to_queue(links):
    """Отправляет ссылки в очередь RabbitMQ"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for link in links:
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
        logging.info(f"Отправлено в очередь: {link}")

    connection.close()


async def main():
    import sys
    if len(sys.argv) < 2:
        logging.error("Использование: python script.py <URL>")
        return

    url = sys.argv[1]
    url = 'https://en.wikipedia.org/wiki/Website'
    links = await extract_links(url)
    send_to_queue(links)


if __name__ == "__main__":
    asyncio.run(main())
