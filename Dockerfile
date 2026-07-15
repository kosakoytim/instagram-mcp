FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastmcp \
    httpx \
    uvicorn \
    fastapi

COPY server.py .

EXPOSE 8001

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]