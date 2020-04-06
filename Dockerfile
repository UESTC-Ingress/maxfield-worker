FROM python:latest
MAINTAINER YukariChiba charles@nia.ac.cn
ADD . /code

WORKDIR /code

RUN pip install -r requirements.txt
CMD ["/code/docker-entry.sh"]

