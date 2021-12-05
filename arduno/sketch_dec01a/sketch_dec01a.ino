#include <DHT.h>
#include <ESP8266WiFi.h>
#include "AdafruitIO_WiFi.h"
#include <Wire.h>
#define DHTPIN 14
#define  AO_IN 5
#define LED 2
#define IO_USERNAME  "quangbinh"
#define IO_KEY       "aio_grNV97RinHfoAC6LXFOOACyoU7PI"
const int analogInPin = A0;
const char *ssid =  "QUANG PHU";     // replace with your wifi ssid and wpa2 key
const char *pass =  "01234545582";
const char* server = "io.adafruit.com";

float lux=0.00, ADC_value=0.0048828125, LDR_value;
AdafruitIO_WiFi io(IO_USERNAME, IO_KEY, ssid, pass);
AdafruitIO_Feed *humidity = io.feed("sensors.humidity");
AdafruitIO_Feed *temperature = io.feed("sensors.temperature");

DHT dht(DHTPIN, DHT11);

void setup() {
  // put your setup code here, to run once:
  Serial.begin(9600);
  dht.begin();
//  pinMode(AO_IN,INPUT);
  pinMode(LED, OUTPUT);
  io.connect();
  while(io.status() < AIO_CONNECTED) 
  {
    Serial.print(".");
    digitalWrite(LED,LOW);
    delay(500);
  }
  digitalWrite(LED,HIGH);
  Serial.println("");
  Serial.println("WiFi connected");
 
 
}


void loop() {
 
    
    float h = dht.readHumidity();
    float t = dht.readTemperature();
    if (isnan(h) || isnan(t))
    {
        Serial.println("Failed to read");

    }
    else 
    {

      Serial.print("Humidity: ");
      Serial.println(h);
      humidity->save(h);
      temperature->save(t);
      Serial.print("Temperature: ");
      Serial.println(t);
      LDR_value = analogRead(analogInPin);
      lux=(250.000000/ (ADC_value*LDR_value)) -50.000000;
      Serial.print("Analogue: ");
      Serial.println(lux);
      Serial.println("--------------------");
      //delay(300000); 
      digitalWrite(LED,LOW);
      delay(1000);
      digitalWrite(LED,HIGH);
      delay(9000);
    }
    
}
