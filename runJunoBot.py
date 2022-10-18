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
            print('Check Loop Contract(BuyNow)')
            bot.checkLoopContract(auction=False)
            time.sleep(random.uniform(200,400))
            print('Check Loop Contract(Auction)')
            bot.checkLoopContract(auction=True)
            time.sleep(random.uniform(500,700))
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
        bot.getLevanaNFTs('egg')
        bot.getLevanaNFTs('rider')
        bot.getLevanaNFTs('loot')
    else:
        print('bad argument')
else:
    print('no argument')
#bot.initialAttack()
#bot.checkkp()
#bot.getLocationStatus()
