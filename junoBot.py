import pyodbc,requests,json,random,os,traceback,logging
from datetime import datetime, timedelta
import time
from pytz import timezone
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv

class JunoBot():
    def __init__(self):
        load_dotenv()
        # use your own database connection string
        self.connstr = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:'+os.getenv('DBIP')+',1433;Database=' \
                       +os.getenv('DBName')+';Uid='+os.getenv('DBUser')+';Pwd='+os.getenv('DBPass')
        self.logger = logging.getLogger("MyLogger")
        formatter = logging.Formatter('[%(asctime)s %(levelname)s] %(message)s', "%m/%d %H:%M:%S")
        fh = logging.FileHandler("logs" + os.sep + "junoBot_{}.log".format(self.getServerNow().strftime("%Y%m%d")))
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        self.levanaNFTs={'rider':'juno1uw3pxkga3dxhqs28rjgpgqkvcuedhmc7ezm2yxvvaey9yugfu3fq7ej2wv',
                        'egg':'juno1a90f8jdwm4h43yzqgj4xqzcfxt4l98ev970vwz6l9m02wxlpqd2squuv6k',
                        'loot':'juno1gmnkf4fs0qrwxdjcwngq3n2gpxm7t24g8n4hufhyx58873he85ss8q9va4'}
        self.loopNFTSoldContract='juno1kylehdql046nep2gtdgl56sl09l7wv4q6cj44cwuzfs6wxnh4flszdmkk0'
        self.loopNFTAuctionClaimContract = 'juno18u5m3zamdz7hayj9qxx6jahps9y367myjhs88fe0fu9qu8hnnydstjwcs8'
        self.loopGraphQL='https://nft-juno-backend.loop.do/'

    # The main function to scrape transactions of Loop market contract via mintscan.io website's API.  Mintscan.io's cloudflare
    # is very sensitive and will ban your IP (permanently?) if you make too many requests too fast so hence the long 5 second
    # delay between requests. Maybe there are better ways to get transactions via Cosmos SDK but I haven't had time to look into it.
    def checkLoopContract(self,auction=False):
        contract=self.loopNFTAuctionClaimContract if auction else self.loopNFTSoldContract
        scraper = cloudscraper.create_scraper(browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False})
        conn = pyodbc.connect(self.connstr)
        cur = conn.cursor()
        page = 0
        offset = 0
        while page < 5:
            url = 'https://api.mintscan.io/v1/juno/wasm/contracts/{}/txs?limit=50&offset={}'.format(contract, offset)
            html = scraper.get(url).content
            jsonDict = json.loads(BeautifulSoup(html, 'html.parser').text)
            offset = offset + 50
            if 'txs' not in jsonDict:
                print(jsonDict)
                self.logger.debug('no transaction found')
                return
            for t in jsonDict['txs']:
                id = int(t['header']['id'])
                timestamp = self.parseToServerTime(t['header']['timestamp'])
                data = t['data']
                txhash = data['txhash']
                events=dict()
                price=0
                collection=None

                # Quick and lazy way look for transactions of levana contracts.  Proper way to do it would be to parse the log event
                for k,v in self.levanaNFTs.items():
                    if v in data['raw_log']:
                        collection=k
                #filter out the buy transactions
                if collection!=None:
                    for e in data['logs'][0]['events']:
                        if e['type'] == 'wasm':
                            for a in e['attributes']:
                                if (a['key'] == 'token_id' or a['key'] == 'recipient' or a['key']=='winning_bid_amount' or
                                    a['key'] == 'sender' or (a['key'] == 'action' and (a['value']=='buy' or a['value']=='claim'))):
                                    events[a['key']] = a['value']
                                if a['key']=='withdraw':
                                    self.logger.debug('   {}:{} unlisted '.format(nftType,events['token_id']))
                                    continue
                        elif e['type']=='transfer':
                            for a in e['attributes']:
                                if a['key'] == 'amount':
                                    v=float(a['value'].split('ibc/')[0])/1000000
                                    # There are afew money transfers in the transaction, largest transfer is price user paid before fees
                                    if v>price:
                                        price=v
                    if len(events)>0 and 'action' in events:
                        self.logger.debug('[{}]:{}'.format(timestamp, txhash))
                        #Get levana nft meta data from my database to send to telegram bot with buy transaction
                        cur.execute('select * from ldAllInventory a with (nolock) where collection=? and tokenid=?', collection, events['token_id'])
                        nftRow = cur.fetchone()
                        if nftRow==None:
                            print('{}:{} not found'.format(collection,events['token_id']))
                        if collection == 'rider':
                            txt = '{} | {} background | {} suit \n'.format(nftRow.faction, nftRow.background, nftRow.suit)
                        elif collection == 'egg':
                            txt = '{} | {}SL | {} \n'.format(nftRow.rarity, round(nftRow.sl,2), nftRow.essence)
                        elif collection == 'loot':
                            if nftRow.type in ('Faction Talisman', 'Personal Dragon Atlas'):
                                txt = '{} | {} | {} \n'.format(nftRow.type, nftRow.faction, nftRow.role)
                            elif nftRow.type == 'Meteor Dust':
                                txt = '{} | {} | {}SL | {} | lc={}\n'.format(nftRow.type, nftRow.rarity, round(nftRow.sl,2), nftRow.essence,nftRow.lc)
                            else:
                                txt = 'unhandled loop type...to be implemented'
                        if events['action']=='buy' or events['action']=='claim':
                            if auction:
                                if 'winning_bid_amount' in events:
                                    price=float(events['winning_bid_amount'].split('ibc/')[0])/1000000
                                    orderType='auction'
                                else:
                                    self.logger.debug('no winning bid amount')
                                    continue
                            else:
                                orderType='buy now'
                            soldMsg='{}:{} purchased({}) for ${}'.format(collection,events['token_id'],orderType,price)
                            self.logger.debug('    {}'.format(soldMsg))
                            cur.execute('select * from ldsold with (nolock) where txhash=?', txhash)
                            r = cur.fetchone()
                            if r == None:
                                # Save recent buy information into this table for potential front end later
                                cur.execute('insert into ldsold (collection,tokenid,price,buyer,timestamp,sl,slperusd,txhash,orderType) '
                                            'values (?,?,?,?,?,?,?,?,?)',collection, events['token_id'], price, events['recipient'],
                                            timestamp, nftRow.sl,float(nftRow.sl)/price,txhash,orderType)
                                cur.commit()
                                toSend ='[{}] {}'.format(timestamp.strftime('%m/%d %H:%M'),soldMsg)
                                if collection == 'egg' or (collection=='loot' and nftRow.type=='Meteor Dust'):
                                    toSend=toSend+'({}SL/$)'.format(round(float(nftRow.sl) / price,2))
                                url = 'https://nft-juno.loop.markets/nftDetail/{}/{}'.format(self.levanaNFTs[collection], events['token_id'])
                                toSend = toSend + '\n' + txt + '\n' + url
                                self.sendGroupTelegram(toSend)
                            else:
                                #stops downloading more if transaction already in database
                                return

                else:
                    self.logger.debug('  not Levana contract')
            time.sleep(5)
            page += 1

    # Downloads sales listings from Loop market graphGL API.  This only updates previously downloaded listings if there is a price update.
    # It doesn't remove delisted items from database.  If you want to keep on getting fresh snapshot of current listings for a GUI,
    # you should have to wipe the table before each run.  But for a telegram alert system, it would keep on sending the same listings
    # if I keep on wipe the table. If I have more time to work on this, I would come up with a way to flag/record listings I already sent.
    def getListings(self, collection):
        conn = pyodbc.connect(self.connstr)
        cur = conn.cursor()
        contract = str(self.levanaNFTs[collection])
        offset = 0
        query = '{nfts(orderBy: [UPDATED_AT_DESC] filter: {inSale: { equalTo: true } contractId: {equalTo: \"' + contract + '\"}}offset: ' + str(offset) + ',first: 100) \
            {totalCount nodes{id info metadata type tokenID updatedAt marketplacePriceAmount marketplacePriceDenom owner}}}'
        try:
            jsonDict = json.loads(requests.post(self.loopGraphQL, json={'query': query}).text)
        except:
            self.logger.debug('Unable to get Loop graphQL...try again in 5 minutes')
            time.sleep(300)
            return self.getListings(collection)
        # self.logger.debug(jsonDict)
        latestUpdate = self.parseToServerTime(jsonDict['data']['nfts']['nodes'][0]['updatedAt'])
        self.logger.debug('Getting listing for {}...latest update {}'.format(collection, latestUpdate))
        for node in jsonDict['data']['nfts']['nodes']:
            tokenid = node['tokenID']
            metadata = node['metadata']
            timestamp = self.parseToServerTime(node['updatedAt'])
            meta = dict()
            for a in json.loads(metadata)['attributes']:
                meta[a['trait_type']] = a['value']
            type = meta['Type'] if 'Type' in meta else None
            rarity = meta['Rarity'] if 'Rarity' in meta else None
            sl = float(meta['Spirit Level']) if 'Spirit Level' in meta else 0
            essence = meta['Essence'] if 'Essence' in meta else None
            lc = meta['Legendary Composition'] if 'Legendary Composition' in meta else None
            faction = meta['Faction'] if 'Faction' in meta else None
            role = meta['Role'] if 'Role' in meta else None
            background = meta['Background'] if 'Background' in meta else None
            suit = meta['Suit'] if 'Suit' in meta else None
            bigPrice = node['marketplacePriceAmount']
            if bigPrice == None:
                self.logger.debug('missing price for collection={} tokenid={}'.format(collection, tokenid))
                continue
            elif bigPrice == '0' or bigPrice == 0:
                self.logger.debug('0 price for collection={} tokenid={}'.format(collection, tokenid))
                continue
            price = float(bigPrice) / 1000000

            if collection == 'rider':
                txt = '{} | {} background | {} suit \n'.format(faction, background, suit)
            elif collection == 'egg':
                txt = '{} | {}SL | {} \n'.format(rarity, round(sl, 2), essence)
            elif collection == 'loot':
                if type in ('Faction Talisman', 'Personal Dragon Atlas'):
                    txt = '{} | {} | {} \n'.format(type, faction, role)
                elif type == 'Meteor Dust':
                    txt = '{} | {} | {}SL | {} | lc={}\n'.format(type, rarity, round(sl, 2), essence, lc)
                else:
                    txt = 'unhandled loop type...to be implemented'
            cur.execute('select * from ldselling with (nolock) where collection=? and tokenid=?', collection, tokenid)
            r = cur.fetchone()
            newPrice = False
            if r == None:
                cur.execute('insert into ldselling (collection,tokenid,price,type,timestamp,sl,slperusd,rarity,essence,lc,faction,\
                    role,background,suit) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            collection, tokenid, price, type, timestamp, sl, sl / price, rarity, essence, lc, faction, role, background, suit)
                cur.commit()
                newPrice = True
            elif r.timestamp < timestamp and float(r.price) != price:
                # update new price for existing listings
                self.logger.debug('old/new price={}/{}'.format(r.price, price))
                cur.execute('update ldselling set price=?, timestamp=? where collection=? and tokenid=?', price, timestamp, collection, tokenid)
                cur.commit()
                newPrice = True
            if newPrice:
                # Just wrote this part so haven't totally decided on how I want limit the alerts, don't want to be too spammy also may send the best
                # deals to myself first before sending to group
                toSend = '[{}] {}:{} selling for {} '.format(timestamp.strftime('%m/%d %H:%M'), collection, tokenid, price)
                if collection == 'egg' or (collection == 'loot' and type == 'Meteor Dust'):
                    toSend = toSend + '({}SL/$)'.format(round(sl / price, 2))
                elif collection == 'rider' and suit in ('Hunter', 'Exoskeleton', 'Command', 'Advisor', 'Rangers'):
                    toSend = toSend + '***'
                url = 'https://nft-juno.loop.markets/nftDetail/{}/{}'.format(self.levanaNFTs[collection], tokenid)
                toSend = toSend + '\n' + txt + '\n' + url
                self.logger.debug(toSend)

    # download levana nft metadata from Loop market graphGL API.  Probably not necessary if you already have all that in your database
    def getLevanaNFTs(self,collection):
        conn = pyodbc.connect(self.connstr)
        cur = conn.cursor()
        contract=str(self.levanaNFTs[collection])
        offset=0
        count=30000
        while (offset<count):
            query='{nfts(orderBy: [UPDATED_AT_DESC] filter: {contractId: {equalTo: \"'+contract+'\"}}offset: '+str(offset)+',first: 100) \
            {totalCount nodes{id info metadata type tokenID updatedAt marketplacePriceAmount marketplacePriceDenom owner}}}'
            #query = '{nfts(orderBy: [UPDATED_AT_DESC] filter: {contractId: {equalTo: \"' + contract + '\"}inSale: { equalTo: true }}) \
            #        {totalCount nodes{id info metadata type tokenID updatedAt marketplacePriceAmount marketplacePriceDenom}}}'

            jsonDict = json.loads(requests.post(self.loopGraphQL,json={'query': query}).text)
            count=jsonDict['data']['nfts']['totalCount']
            print('count={} offset={} nodes={}'.format(count,offset,len(jsonDict['data']['nfts']['nodes'])))
            for node in jsonDict['data']['nfts']['nodes']:
                owner=node['owner']
                tokenid=node['tokenID']
                metadata=node['metadata']
                timestamp = self.parseToServerTime(node['updatedAt'])
                meta=dict()
                for a in json.loads(metadata)['attributes']:
                    meta[a['trait_type']]=a['value']
                type =meta['Type'] if 'Type' in meta else None
                rarity = meta['Rarity'] if 'Rarity' in meta else None
                sl = meta['Spirit Level'] if 'Spirit Level' in meta else 0
                essence = meta['Essence'] if 'Essence' in meta else None
                lc = meta['Legendary Composition'] if 'Legendary Composition' in meta else None
                ac = meta['Ancient Composition'] if 'Ancient Composition' in meta else None
                rc = meta['Rare Composition'] if 'Rare Composition' in meta else None
                cc = meta['Common Composition'] if 'Common Composition' in meta else None
                faction = meta['Faction'] if 'Faction' in meta else None
                role = meta['Role'] if 'Role' in meta else None
                background = meta['Background'] if 'Background' in meta else None
                suit = meta['Suit'] if 'Suit' in meta else None
                dragon = meta['Dragon Type'] if 'Dragon Type' in meta else None
                print('{}:{} type={} rarity={} sl={} faction={} role={}'.format(collection, tokenid, type, rarity, sl,faction, role,))
                cur.execute('select * from ldallinventory with (nolock) where collection=? and tokenid=?', collection,tokenid)
                r = cur.fetchone()
                if r == None:
                    cur.execute('insert into ldallinventory (collection,tokenid,type,rarity,sl,essence, \
                                lc,ac,rc,cc,dragon,timestamp,faction,role,background,suit,owner) values \
                                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',collection, tokenid, type, rarity, sl, essence,
                                lc,ac,rc,cc,dragon, timestamp, faction, role, background, suit,owner)
                    cur.commit()
                else:
                    continue
            offset=offset+100

    def getServerNow(self):
        return datetime.now().astimezone(timezone('US/Central')).replace(tzinfo=None)

    def parseToServerTime(self,timestampStr):
        timestampStr=timestampStr.replace('T', ' ').split('.')[0]
        timestampStr = timestampStr.replace('Z', '+00:00')
        if '+00:00' not in timestampStr:
            timestampStr=timestampStr+'+00:00'
        timestamp = datetime.strptime(timestampStr, '%Y-%m-%d %H:%M:%S%z')
        return timestamp.astimezone(timezone('US/Central')).replace(tzinfo=None)

    def sendGroupTelegram(self,message):
        bot_token = os.getenv('GroupTG')
        gid= '-790815723'
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + gid + '&parse_mode=HTML&text=' + message
        response = requests.get(send_text)
        if '[200]' not in str(response):
            print('sendTelegram uncecessful {}'.format(response))
        return response.json()

    def sendTelegram(self,message):
        bot_token = os.getenv('PersonalTG')
        bot_chatID = '1404164786'
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=HTML&text=' + message
        response = requests.get(send_text)
        if '[200]' not in str(response):
            print('sendTelegram uncecessful {}'.format(response))
        return response.json()
