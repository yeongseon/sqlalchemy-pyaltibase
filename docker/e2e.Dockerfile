ARG BASE_IMAGE=altibase/a_plus_edition:latest

FROM ${BASE_IMAGE} AS odbc-source

FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ unixodbc-dev git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=odbc-source /root/altibase_home/lib/libaltibase_odbc-64bit-ul64.so /opt/altibase/lib/libaltibase_odbc-64bit-ul64.so

RUN printf '[ALTIBASE_HDB_ODBC_64bit]\nDescription = Altibase HDB ODBC 64-bit\nDriver = /opt/altibase/lib/libaltibase_odbc-64bit-ul64.so\n' >> /etc/odbcinst.ini

WORKDIR /workspace

COPY . .

RUN pip install --upgrade pip \
    && pip install -e ".[dev]" \
    && pip install "pyaltibase @ git+https://github.com/yeongseon/pyaltibase.git@main"

CMD ["python", "-m", "pytest", "test/e2e", "-v", "--tb=long", "--no-header"]
