# Multi-stage build: Lấy Hadoop 3.2.1 từ bde2020 và chạy trên Ubuntu LTS với Python 3.10+
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 AS hadoop-source

FROM eclipse-temurin:8-jre-jammy

# Cài đặt Python 3 hiện đại và các tiện ích
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip dos2unix curl && \
    rm -rf /var/lib/apt/lists/*

# Copy toàn bộ Hadoop client từ image nguồn
COPY --from=hadoop-source /opt/hadoop-3.2.1 /opt/hadoop-3.2.1

# Cấu hình biến môi trường Hadoop
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

WORKDIR /app

# Copy mã nguồn dự án vào container
COPY . /app

# Đảm bảo quyền thực thi và định dạng LF cho các script
RUN find /app -type f -name "*.sh" -exec dos2unix {} + && \
    chmod +x /app/run_pipeline.sh /app/mapreduce/run_job.sh

CMD ["/bin/bash"]
