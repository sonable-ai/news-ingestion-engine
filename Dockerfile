# syntax=docker/dockerfile:1
FROM python:3.11-alpine
WORKDIR /src
RUN apk add --no-cache gcc musl-dev linux-headers
RUN mkdir -p /src/logs && chmod -R 777 /src/logs
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5001
EXPOSE 50052
COPY . .
CMD ["python3", "service.py"]