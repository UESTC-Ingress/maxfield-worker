FROM python:latest
MAINTAINER YukariChiba charles@nia.ac.cn
ADD . /code

WORKDIR /code

RUN apt -y update
RUN apt -y upgrade
RUN apt -y install gifsicle

RUN pip install -r requirements.txt
EXPOSE 1331
CMD ["/code/docker-entry.sh"]

