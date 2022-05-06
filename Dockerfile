
FROM python:3.8-slim

WORKDIR /app


RUN pip install --upgrade "pip==20.2.4" && \
    pip install python-dotenv==0.20.0 && \
    pip install tweepy==4.8.0 && \
    pip install pandas==1.4.1 && \
    pip install kafka-python==2.0.2 && \
    pip install yahooquery==2.2.15


RUN pip install rand-engine==0.0.2

COPY ./src .

CMD ["sleep", "infinity"]
