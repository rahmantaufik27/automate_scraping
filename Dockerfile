FROM ubuntu:focal

# install the dependencies of ubuntu and chrome
ARG DEBIAN_FRONTEND=noninteractive
RUN echo "===> Installing system dependencies..." && \
    BUILD_DEPS="curl unzip" && \
    apt-get update && apt-get install --no-install-recommends -y \
    python3 python3-pip wget \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libnspr4 libnss3 lsb-release xdg-utils libxss1 libdbus-glib-1-2 libgbm1 \
    $BUILD_DEPS \
    xvfb && \
    \
    \
    echo "===> Installing chromedriver and google-chrome..." && \
    CHROMEDRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE` && \
    wget https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip -d /usr/bin && \
    chmod +x /usr/bin/chromedriver && \
    rm chromedriver_linux64.zip && \
    \
    CHROME_SETUP=google-chrome.deb && \
    wget -O $CHROME_SETUP "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb" && \
    dpkg -i $CHROME_SETUP && \
    apt-get install -y -f && \
    rm $CHROME_SETUP && \
    \
    \
    echo "===> Installing python dependencies..." && \
    pip3 install selenium==3.141.0 pyvirtualdisplay==2.2 && \
    \
    \
    echo "===> Remove build dependencies..." && \
    apt-get remove -y $BUILD_DEPS && rm -rf /var/lib/apt/lists/*

# set environment variables
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV APP_HOME /usr/src/app

# set the directories, the accessed file, and the requirements
COPY requirements.txt /${APP_HOME}/requirements.txt
WORKDIR /${APP_HOME}
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . ${APP_HOME}
RUN mkdir -p /${APP_HOME}/data
RUN mkdir -p /${APP_HOME}/data/shopee
RUN mkdir -p /${APP_HOME}/data/tokped
RUN mkdir -p /${APP_HOME}/data/shopee/images_files
RUN mkdir -p /${APP_HOME}/data/tokped/images_files
RUN mkdir -p /${APP_HOME}/daily_url
RUN mkdir -p /${APP_HOME}/external_files
COPY external_files/fd-storage-python.json /${APP_HOME}/external_files/fd-storage-python.json
COPY external_files/fd-bigquery-python.json /${APP_HOME}/external_files/fd-bigquery-python.json 
COPY daily_url/proxy_list.txt /${APP_HOME}/external_files/proxy_list.txt 

# run the app
EXPOSE 5000
CMD ["python3","main.py"]