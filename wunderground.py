
import sys
import datetime
import dateutil.tz
from pint import UnitRegistry
import numpy
import math
import requests
import pprint

def push_list(l, item, numsamples):
    "implement a circular list with numsamples"
    l.append(item)
    if len(l) > numsamples:
        del l[:len(l)-numsamples]

SAMP_PERIOD_S = 2.5
ITEMS_2M = int(120 / SAMP_PERIOD_S)
ITEMS_10M = int(600 / SAMP_PERIOD_S)
ITEMS_60M = int((60*60)/SAMP_PERIOD_S)
ITEMS_day = int((24*60*60)/SAMP_PERIOD_S)

SW_NAME = "hydrosense.pws_upload(0.1)"

ureg = UnitRegistry()

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
def f_to_c(f):
    return ureg.Quantity(f, ureg.degF).to(ureg.degC).magnitude
def c_to_f(c):
    return ureg.Quantity(c, ureg.degC).to(ureg.degF).magnitude
def c_to_k(c):
    return ureg.Quantity(c, ureg.degC).to(ureg.degK).magnitude 
def sat_press(temp_c):
    """saturated vapor pressure 
    
    ref: http://www.srh.noaa.gov/images/epz/wxcalc/vaporPressure.pdf
    """
    f = (7.5 * temp_c) / (237.3 + temp_c)
    return 6.11 * 10 ** (f)
def dewpoint(temp_c, rel_hum):
    """rel_hum = 90.5 means 90.5% (don't use 0.905). Result is in C
    
    ref: http://www.srh.noaa.gov/images/epz/wxcalc/wetBulbTdFromRh.pdf
    """

    e_s = sat_press(temp_c)
    
    return (237.3 * math.log(e_s*rel_hum/611))/ \
            (7.5 * math.log(10) - math.log(e_s*rel_hum/611))

def sum_rain(r, max_tips = 127, bucket_size = 0.01):
    "r is a list of bucket tips, each one is <bucket_size> it is a 7 bit value that may wrap."
    #print("sum_rain: {}".format(r))
    if r == []:
        return 0

    tips = 0    
    at = r[0]
    for x in r[1:]:
        if x < at:
            # wraparound
            tips += ((max_tips - at) + x + 1)
        else:
            tips += x - at

        at = x

    print("total tips = {}".format(tips))
    print("total rain = {}".format(tips *bucket_size))
    return tips * bucket_size
            

class pws_upload():
    def __init__(self,
                 pwsid,
                 password, 
                 rt= False):
        
        self.pwsid = pwsid
        self.password = password
        self.def_params={'ID': pwsid,
                        'PASSWORD': password,
                        'softwaretype': SW_NAME,
                        'action': 'updateraw'}
        if rt:
            self.url = "http://rtupdate.wunderground.com/weatherstation/updateweatherstation.php"
            self.def_params['realtime'] = 1
            self.def_params['rtfreq'] = 2.5
        else:
            self.url = "http://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"
            
        self.last_temp_f = None
        self.last_rh = None
        self.windd_2m =[]
        self.windsp_2m =[]
        self.windd_10m = []
        self.windsp_10m = []
        self.rain_60m = []
        self.rain_day = []
        self.today = datetime.datetime.now(dateutil.tz.tzlocal()).date()

    def update(self, d, ts_utc):

        # all packets contain wind info
        push_list(self.windd_2m, d['windd'], ITEMS_2M)
        push_list(self.windsp_2m, d['windv'], ITEMS_2M)
        push_list(self.windd_10m, d['windd'], ITEMS_10M)
        push_list(self.windsp_10m, d['windv'], ITEMS_10M)

        # only some have rain info
        if 'rain' in d:

            # now on next day, reset daily stats.
            if ts_utc.date() - self.today > datetime.timedelta(0):
                self.rain_day = []
                self.today = ts_utc.date()

            push_list(self.rain_60m, d['rain'], ITEMS_60M)
            push_list(self.rain_day, d['rain'], ITEMS_day)

        # construct the update packet
        p = dict(self.def_params)
        p['dateutc'] = ts_utc.strftime('%Y-%m-%d %H:%M:%S')

        p['winddir'] = d['windd']
        p['windspeedmph'] = d['windv']        
        
        p['windgustmph'] = max(self.windsp_2m)
        p['windgustdir'] = self.windd_2m[self.windsp_2m.index(p['windgustmph'])]

        p['windspdmph_avg2m'] = numpy.mean(self.windsp_2m)
        p['winddir_avg2m'] = numpy.mean(self.windd_2m)

        p['windgustmph_10m'] = max(self.windsp_10m)
        p['windgustdir_10m'] = self.windd_10m[self.windsp_10m.index(p['windgustmph_10m'])]

        if 'temp' in d:
            self.last_temp_f = d['temp']
            p['tempf'] = d['temp']
        else:
            # wundergroudn likes temperature every time.
            if self.last_temp_f != None:
                p['tempf'] = self.last_temp_f

        if 'rh' in d:
            self.last_rh = d['rh']
            p['humidity'] = d['rh']            
        else:
            if self.last_rh != None:
                p['humidity'] = self.last_rh

        if self.last_rh != None and self.last_temp_f != None:
            p['dewptf'] = c_to_f(dewpoint(f_to_c(self.last_temp_f), self.last_rh))
        
        p['rainin'] = sum_rain(self.rain_60m)
        p['dailyrain'] = sum_rain(self.rain_day)


        pprint.pprint (p)
        return self.put(**p)


    def put(self, **kwargs):
        "all kwargs get passed into wunderground"
        
        params = dict(kwargs)
        
        r = requests.get(self.url, params = params)
        
        if r.status_code != 200:
            eprint("PWS upload failed with: {}".format(r.text))
        
        return r.status_code, r.text