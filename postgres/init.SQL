CREATE TABLE hotel_weather (
    id SERIAL PRIMARY KEY,
    hotel_name VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    temperature FLOAT,
    weather_description VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
