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
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = os.getenv("QUEUE_NAME")
TIMEOUT = 30  # Таймаут ожидания в секундах


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
                title = soup.get('title')
                logging.info(f"Обработана ссылка: {url}\ntitle: {title}")
                links = []
                for tag in soup.find_all(["a", "img", "video", "audio"]):
                    link: str = tag.get("href") or tag.get("src")
                    if link and not link.startswith('#') and not link.startswith(':'):
                        links.append(get_absolute_url(url, link))
                        logging.info(f"Найдено: {tag.name}, {link}")
                return links
            else:
                logging.warning(f"Ошибка загрузки: {url}")
                return []


async def consumer():
    """Асинхронный consumer, обрабатывающий очередь"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    while True:
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
        if body:
            url = body.decode()
            links = await extract_links(url)
            for link in links:
                channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
                logging.info(f"Добавлено в очередь: {link}")
        else:
            await asyncio.sleep(TIMEOUT)
            break

    connection.close()
    logging.info("Очередь пуста. Завершение работы.")


async def main():
    await consumer()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановка по Ctrl+C")
