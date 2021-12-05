#include<Wire.h>
 
#define Addr 0x4A
 #include <BH1750.h>
 BH1750 lightMeter;
void setup()
{
 
Wire.begin();
// Initialise serial communication
Serial.begin(9600);
 
  lightMeter.begin();
}
 
void loop()
{
 float lux = lightMeter.readLightLevel();
  Serial.print("Light Meter: ");
  Serial.print(lux);
  Serial.println(" lx");
  delay(2000);
}
