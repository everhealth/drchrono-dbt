ARG VIRTUAL_ENV="/opt/venv"
ARG APP_HOME="/usr/app/dbt"

#-----------------------------------------------------------------------------------------------------------------------
# Builder Image
#-----------------------------------------------------------------------------------------------------------------------
FROM python:3.10-alpine

ARG VIRTUAL_ENV
ARG APP_HOME

ENV VIRTUAL_ENV=$VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV APP_HOME=$APP_HOME
ENV DBT_PROFILES_DIR=$APP_HOME/.dbt

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install required package
RUN apk update && \
    apk add --no-cache postgresql-libs git && \
    apk add --no-cache --virtual .build-deps \
    gcc musl-dev postgresql-dev libffi-dev

# Install virtualenv
RUN pip install --upgrade pip virtualenv \
    && python -m virtualenv $VIRTUAL_ENV

# Create required directories
RUN mkdir -p $APP_HOME $VIRTUAL_ENV

# Copy requirements.txt
COPY requirements.txt $APP_HOME/

# Install requirements.txt
RUN pip install --no-cache-dir -r $APP_HOME/requirements.txt

# Clean up
RUN apk --purge del .build-deps

# Working directory
WORKDIR $APP_HOME

# Copy project files
COPY . .

# Install DBT packages
RUN dbt deps

# Entrypoint
ENTRYPOINT ["dbt"]

# CMD
CMD ["debug"]
