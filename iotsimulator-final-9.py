""" iotsimulator-final-9.py  """
#!/usr/bin/python

import sys
import datetime
import random
import string

# Set number of simulated messages to generate
if len(sys.argv) > 1:
  numMsgs = int(sys.argv[1])
else:
  numMsgs = 1

# Count

index = 1

# Fixed values
guidStr = "A00"
# destinationStr = "0-AAA12345678"
formatStr = "urn:World-wide, product_AA:station21:parametrics"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

iotmsg_header = """\
{
  "index": %d,
  "loc": %d,
  "tool": %d,
  "stn": %d,
  "guid": "%s",
  "slot": %d, """
#  "destination": "%s", """

iotmsg_eventTime = """\
  "time": "%sZ", """

iotmsg_payload ="""\
  "payload": {
     "format": "%s", """

iotmsg_data ="""\
     "data": {
       "pwr": %.3f,
       "volt": %.1f,
       "bias": %.1f,
       "flow": %.1f,
       "pres": %.1f,
       "temp": %.1f
     }
   }
}"""

##### Generate JSON output:

print "["

dataElementDelimiter = ","
for counter in range(0, numMsgs):

  randloc = random.randrange(1, 4)        # (1-3)
  randtool = random.randrange(0, 10)      # (0-9)
  stn = 21
  randguid = random.randrange(10000, 100000)  # (10000-99999)
  randslot = random.randrange(1, 26)      # (1-25)

# randInt = random.randrange(0, 9)
# randLetter = random.choice(letters)

  print iotmsg_header % (index,randloc,randtool,stn,guidStr+str(randguid),randslot)

# print iotmsg_header % (index, guidStr+str(randguid), destinationStr)

  today = datetime.datetime.today()
  datestr = today.isoformat()
  print iotmsg_eventTime % (datestr)

  print iotmsg_payload % (formatStr)

  # Generate a random floating point number for the payload data

  randPwr = random.uniform(0.18, 0.20) + 0.01
  randVolt = random.uniform(267, 275) + 4.0
  randBias = random.uniform(92, 100) + 4.0
  randFlow = random.uniform(38, 40) + 1.0
  randPres = random.uniform(4, 10) + 3.0
  randTemp = random.uniform(76, 80) + 2.0

  if counter == numMsgs - 1:
    dataElementDelimiter = ""
  print iotmsg_data % (randPwr,randVolt,randBias,randFlow,randPres,randTemp                                                                                                                                                               ) + dataElementDelimiter
  index = index + 1
print "]"