class Object:
    def __init__(self,id,humidity=None,light=None,soil=None,temperature=None):
        self.id=id
        self.humidity=humidity
        self.light=light 
        self.soil=soil
        self.temperature=temperature

    def check(self):
        return self.humidity and self.light and self.soil and self.temperature

    def reset(self):
        self.humidity=None
        self.light=None 
        self.soil=None
        self.temperature=None