FROM ultrafunk/undetected-chromedriver

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libappindicator3-1 \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxrandr2 \
    libxshmfence1 \
    libgbm-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the Python script into the container
COPY get_cookies.py .

COPY requirements.txt .

RUN pip3 install -r requirements.txt

# Command to run the script
CMD ["python3", "get_cookies.py"]
