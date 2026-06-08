FROM apache/airflow:2.10.5-python3.11

USER airflow
COPY requirements.txt /requirements.txt
RUN PIP_DISABLE_PIP_VERSION_CHECK=1 pip install --no-cache-dir -r /requirements.txt \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --timeout 180 \
    --retries 15 \
    --prefer-binary

ENV DBT_PROFILES_DIR=/opt/airflow/project/.dbt
