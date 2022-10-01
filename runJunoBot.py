import sys
import random
from datetime import datetime, timedelta
import os
import time
import traceback

from junoBot import JunoBot

bot=JunoBot()

if len(sys.argv)==2:
    if sys.argv[1].lower()=='clc':
        while(True):
            print('Check Loop Contract')
            bot.checkLoopContract()
            time.sleep(random.uniform(800,1000))
    elif sys.argv[1].lower()=='l':
        while(True):
            bot.getListings('rider')
            time.sleep(10)
            bot.getListings('loot')
            time.sleep(10)
            bot.getListings('egg')
            time.sleep(60)
    elif sys.argv[1]=='n':
        print('get NFT meta data')
        bot.getLevanaNFTs('rider')
        bot.getLevanaNFTs('loot')
        bot.getLevanaNFTs('egg')
    else:
        print('bad argument')
else:
    print('no argument')
#bot.initialAttack()
#bot.checkkp()
#bot.getLocationStatus()
