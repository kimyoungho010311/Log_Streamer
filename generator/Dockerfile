FROM python:3.11-slim

WORKDIR /app

# 필수 패키지 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사 (내일 main.py를 작성할 예정이므로 빌드 테스트용)
COPY . .

CMD ["python", "main.py"]