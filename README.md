# OTRPO lab 7
## Установка необходимых библиотек
```bash
pip install aiohttp beautifulsoup4 pika python-dotenv
```

## Подготовка к запуску
Перед запуском скриптов необходимо в файле params.env указать переменные среды
и запустить RabbitMQ сервер

## Запуск producer
`producer.py` парсит на элементы указанную при запуске ссылку и добавляет содержащиеся ссылки в очередь RabbitMQ
```bash
producer.py https://example.com
```

## Запуск consumer/producer
`producer_consumer.py` достаёт ссылку из очереди RabbitMQ и парсит её на элементы, найденные ссылки отправляет в очередь
```bash
producer_consumer.py
```
При нажатии на Ctrl + C выполнение программы завершается.

Также программа может завершить выполнение при отсутствии сообщений в очереди в течение 30 секунд

Обрабатываются теги "a", "img", "video", "audio"