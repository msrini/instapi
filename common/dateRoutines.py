from random import randrange
from datetime import datetime, date, time
from datetime import timedelta
class Dates:
    def __init__(self):
        self.currentDateISO = ""
    """
        This function will return a random datetime between two datetime objects.
    """
    def randomDateBetween(self, start=datetime(datetime.today().year-1, 1, 1), end=datetime.today()):
        if isinstance(end, datetime):
            delta = end - start
            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        else:
            int_delta = end
        random_second = randrange(int_delta)
        dt = start + timedelta(seconds=random_second)
        dtObj = {"day":dt.day, "year":dt.year, "month": dt.month, "hours": dt.hour, "minutes":dt.minute, "seconds":dt.second}
        return ({"dtObj":dtObj, "dt":dt})
    def getDateNow(self, format="ISOFormat"):
        if format == "ISOFormat":
            return datetime.today().isoformat()
    def getDateNDaysBefore(self, n):
        dt = datetime.today() - timedelta(days=n)
        dtObj = {"day": dt.day, "year": dt.year, "month": dt.month, "hours": dt.hour, "minutes": dt.minute,
                 "seconds": dt.second}
        return ({"dtObj": dtObj, "dt": dt})
    def getDate(self,dateObj):
        d =  date(dateObj["year"], dateObj["month"], dateObj["day"])
        t = time(dateObj["hours"], dateObj["minutes"], dateObj["seconds"])
        dt = datetime.combine(d,t)
        return dt
    def diffDays(self, dateObj2, dateObj1):
        dt1 = self.getDate(dateObj1)
        dt2 = self.getDate(dateObj2)
        delta = dt2 - dt1
        return delta.days
