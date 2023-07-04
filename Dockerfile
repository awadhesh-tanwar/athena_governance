FROM python:3.9

WORKDIR /code

# Install dependencies
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 18623

# CMD tail -f /dev/null
CMD python manage.py runserver 0.0.0.0:8000
