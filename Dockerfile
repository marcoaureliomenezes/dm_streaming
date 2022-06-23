
FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt .
RUN pip install --upgrade "pip==20.2.4" && \
    pip install -r requirements.txt

COPY ./src .
COPY ./dist ./dist

RUN pip install ./dist/rand_engine-0.0.2.tar.gz

CMD ["sleep", "infinity"]

