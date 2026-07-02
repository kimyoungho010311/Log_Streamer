FROM python:3.11-slim

WORKDIR /app

# 의존성을 먼저 설치해 소스 변경 시 Docker 캐시를 최대한 활용합니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
