class Flight(object):	
    
    def __init__(self, fields):
        
        self.fields = fields
        self.Year = int(fields[0])
        self.Month = int(fields[1])
        self.DayofMonth = int(fields[2])
        self.DayOfWeek = int(fields[3])
        self.UniqueCarrier = fields[4]
        self.FlightNum = fields[5]
        self.Origin = fields[6]
        self.Dest = fields[7]
        self.CRSDepTime = fields[8]
        str_depdelay = fields[9]
        if(str_depdelay is not None and len(str_depdelay) > 0):
            self.DepDelay = float(fields[9])
        else:
            self.DepDelay = 0.0
        self.CRSArrTime = fields[10]
        str_arrdelay = fields[11]
        if(str_arrdelay is not None and len(str_arrdelay) > 0):
            self.ArrDelay = float(fields[11])
        else:
            self.ArrDelay = 0.0
        self.Cancelled = int(float(fields[12]))
        
    def __str__(self):
        return ','.join(self.fields)