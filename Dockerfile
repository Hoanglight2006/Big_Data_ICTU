FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Cài đặt Python 3
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip dos2unix curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy mã nguồn dự án vào container
COPY . /app

# Đảm bảo quyền thực thi và định dạng LF cho các script
RUN find /app -type f -name "*.sh" -exec dos2unix {} + && \
    chmod +x /app/run_pipeline.sh /app/mapreduce/run_job.sh

# Biến môi trường
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

CMD ["/bin/bash"]
