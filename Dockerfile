FROM python:3.6-alpine

COPY requirements.txt requirements.txt

RUN apk add --no-cache --virtual .build-deps \
  build-base \
    && pip install -r requirements.txt --no-cache-dir \
    && find /usr/local \
        \( -type d -a -name test -o -name tests \) \
        -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
        -exec rm -rf '{}' + \
    && runDeps="$( \
        scanelf --needed --nobanner --recursive /usr/local \
                | awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
                | sort -u \
                | xargs -r apk info --installed \
                | sort -u \
    )" \
    && apk add --virtual .rundeps $runDeps \
    && apk --purge del .build-deps
RUN echo "UTC" >  /etc/timezone

COPY . .

EXPOSE 8000

ENTRYPOINT ["./run.sh"]
