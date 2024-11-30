import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
import pika
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

env = load_dotenv('params.env')
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Загрузка переменных окружения
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = os.getenv("QUEUE_NAME")


def get_absolute_url(base_url, link):
    """Конвертирует относительные ссылки в абсолютные"""
    if link.startswith('http'):
        return link

    parsed_url = urlparse(base_url)

    if link.startswith('//'):
        return f'{parsed_url.scheme}:{link}'

    if link.startswith('/'):
        return f'{parsed_url.scheme}://{parsed_url.netloc}{link}'
    else:
        if not base_url.endswith('/'):
            base_url += '/'
        return base_url + link


async def extract_links(url):
    """Извлекает ссылки и медиа с указанного URL"""
    domain = urlparse(url).netloc

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                logging.warning(f"Ошибка загрузки: {url}")
                return []

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            title = soup.find('title').text
            logging.info(f"Обработана ссылка: {url}\tTitle: {title}")
            links = []

            for tag in soup.find_all(["a", "img", "video", "audio"]):
                link: str = tag.get("href") or tag.get("src")

                if not link or link.startswith('#') or link.startswith(':'):
                    continue

                abs_url = get_absolute_url(url, link)
                if urlparse(abs_url).netloc == domain:
                    links.append(abs_url)
                    logging.info(f"Найдено: {tag.name}, {link}")
            return links
                

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
    # import sys
    # if len(sys.argv) < 2:
    #     logging.error("Использование: python script.py <URL>")
    #     return

    # url = sys.argv[1]
    url = 'https://en.wikipedia.org/wiki/Website'

    links = await extract_links(url)
    if len(links) > 0:
        send_to_queue(links)


if __name__ == "__main__":
    asyncio.run(main())
