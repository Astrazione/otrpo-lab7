# OTRPO lab 7
## Установка необходимых библиотек
```bash
pip install aiohttp beautifulsoup4 pika aiomonitor
```

## Подготовка к запуску
Перед запуском скриптов необходимо в файле params.env указать переменные среды
и запустить

## Запуск producer
`producer.py` парсит на элементы указанную при запуске ссылку
```bash
producer.py https://example.com
```

## Запуск consumer/producer
`producer.py` парсит на элементы указанную при запуске ссылку
```bash
consumer_producer.py
```
При нажатии на Ctrl + C выполнение программы завершается.

Обрабатываются теги "a", "img", "video", "audio"