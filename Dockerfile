FROM docker.io/python:3

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV POSTGRES_HOST=""
ENV POSTGRES_USER=""
ENV POSTGRES_PASSWORD=""
ENV POSTGRES_DATABASE=""
ENV REDDIT_ID=""
ENV REDDIT_SECRET=""

ENTRYPOINT [ "./archive.py" ]
