import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
import pika
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

result = load_dotenv('params.env')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Загрузка переменных окружения
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = os.getenv("QUEUE_NAME")
TIMEOUT = 30  # Таймаут ожидания в секундах


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
            

async def consumer():
    """Асинхронный consumer, обрабатывающий очередь"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        while True:
            _, _, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
            if body:
                url = body.decode()
                links = await extract_links(url)
                for link in links:
                    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
                    logging.info(f"Добавлено в очередь: {link}")
            else:
                await asyncio.sleep(TIMEOUT)
                break
        logging.info("Очередь пуста. Завершение работы.")
    except aiohttp.ClientConnectorError as e:
        logging.error(e)
    finally:
        connection.close()


async def main():
    await consumer()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановка по Ctrl+C")
