FROM python:3.12-slim
WORKDIR /srv
RUN pip install --no-cache-dir fastapi==0.115.5 "uvicorn[standard]==0.32.1" "psycopg[binary,pool]==3.2.3"
COPY app/ /srv/app/
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
