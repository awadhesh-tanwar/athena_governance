FROM python:3.9

RUN mkdir -p /governance_lf/

WORKDIR /governance_lf/

COPY app .

RUN chmod +x run.sh
RUN pip install -r requirements.txt

EXPOSE 18623

WORKDIR /governance_lf/
#CMD tail -f /dev/null
