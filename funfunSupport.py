from datetime import datetime, timedelta
import time
import traceback
import json
import sys
import re

from steem import Steem
import steembase
from steem.blockchain import Blockchain
from dbConn import DBActions
import config
import pymysql

# 스팀잇에서 현황 긁어오는데 사용

class FunfunPosting(object):
    def __init__(self):
        self.steem = Steem()
        self.blockchain = Blockchain()
        self.account = config.ACCOUNT['user']
        connObj = DBActions()
        self.conn = connObj.getConnection()

    def _getLastBlockId(self):
        cursor = self.conn.cursor()
        query = """
            SELECT max(last_block) as latest FROM last_block
        """
        cursor.execute(query)
        ret = cursor.fetchone()
        if ret is None or len(ret) < 1:
            return 1

        return ret['latest']

    def _updateLastBlockId(self, block_no):
        cursor = self.conn.cursor()
        query = """
            UPDATE last_block SET last_block = %s WHERE id = 1
        """
        try:
            cursor.execute(query, (block_no, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        return True

    def _getUserId(self, user_text_id):
        cursor = self.conn.cursor()
        query = """
            SELECT id FROM users
            WHERE steemid = %s
            LIMIT 1
        """
        cursor.execute(query, (user_text_id, ))
        ret = cursor.fetchone()
        if ret is None or len(ret) < 1:
            return -1

        return ret['id']

    def findUser(self, user_text_id):
        cursor = self.conn.cursor()
        query = """
            SELECT count(*) as cnt FROM users
            WHERE steemid = %s
        """
        cursor.execute(query, (user_text_id, ))
        ret = cursor.fetchone()
        if ret is None or len(ret) < 1:
            return False

        return True if ret['cnt'] == 1 else False

    def addUser(self, user_text_id):
        cursor = self.conn.cursor()
        query = """
            INSERT INTO users
              (steemid, added_at)
            VALUES 
              (%s, CURRENT_TIMESTAMP)
        """
        try:
            cursor.execute(query, (user_text_id, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def deleteUser(self, user_text_id):
        cursor = self.conn.cursor()
        query = """
            DELETE FROM users
            WHERE steemid = %s
        """
        try:
            cursor.execute(query, (user_text_id, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def getUserLists(self):
        cursor = self.conn.cursor()
        query = """
            SELECT steemid FROM users
        """
        cursor.execute(query)
        ret = cursor.fetchall()
        if ret is None or len(ret) < 1:
            return []

        result = [ '@{}'.format(item['steemid']) for item in ret ]

        return result

    def getThemeLists(self):
        cursor = self.conn.cursor()
        query = """
            SELECT * FROM theme
        """
        cursor.execute(query)
        ret = cursor.fetchall()
        if ret is None or len(ret) < 1:
            return []

        result = [ {"id": item['id'], "theme_name": item['theme_name'], "added_time":item['added_time']} for item in ret ]

        return result

    def addTheme(self, theme_name):
        cursor = self.conn.cursor()
        query = """
            INSERT INTO theme
              (theme_name, added_time)
            VALUES
              (%s, CURRENT_TIMESTAMP)
        """
        try:
            cursor.execute(query, (theme_name, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def modifyTheme(self, theme_id, theme_name):
        cursor = self.conn.cursor()
        query = """
            UPDATE theme
              SET theme_name = %s
            WHERE id = %s
        """
        try:
            cursor.execute(query, (theme_name, theme_id, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def deleteTheme(self, theme_name):
        cursor = self.conn.cursor()
        query = """
            DELETE FROM theme
            WHERE theme_name = %s
        """
        try:
            cursor.execute(query, (theme_name, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def _findThemeId(self, theme_name):
        cursor = self.conn.cursor()
        query = """
            SELECT id FROM theme
            WHERE theme_name = %s
            LIMIT 1
        """
        cursor.execute(query, (theme_name, ))
        ret = cursor.fetchone()
        if ret is None or len(ret) < 1:
            return -1

        return ret['id']

    def _insertPostInfo(self, user_id, theme_id, user_text_id, permlink, tags, written_at, is_regular=False):
        cursor = self.conn.cursor()
        query = """
            INSERT INTO posts
              (user_id, theme_id, user_text_id, permlink, tags, written_at, is_voted, is_regular)
            VALUES
              (%s,      %s,       %s,           %s,       %s,   %s,         0,        %s)
        """
        try:
            cursor.execute(query, (user_id, theme_id, user_text_id, permlink, tags, written_at, is_regular, ))

        except pymysql.err.IntegrityError:
            print("Duplicated or revised post")
            return False, 1

        except:
            traceback.print_exc()
            self.conn.rollback()
            return False, 2

        self.conn.commit()
        return True, 0

    def _voting(self, user_text_id, permlink, is_regular=False, is_voted=False):
        if is_voted == True:
            return True

        cursor = self.conn.cursor()
        query = """
            UPDATE posts
              SET is_voted = 1
            WHERE user_text_id = %s
              AND permlink = %s
        """
        try:
            cursor.execute(query, (user_text_id, permlink, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        ## VOTE
        post = "@{}/{}".format(user_text_id, permlink)
        try:
            """
                {
                  "voter": "badfatcat",
                  "author": "claudene",
                  "permlink": "update-the-new-coins-in-the-philippines-the-p10-pesos-and-p5-pesos-2018-20171231t62751604z",
                  "weight": 10000,
                  "_id": "1af544c59ea75a816ed756578105d9befa442224",
                  "type": "vote",
                  "timestamp": "2018-01-15 14:32:33",
                  "block_num": 19001685,
                  "trx_id": "1d0490ac6aea885b373a5dc0f1bebb22d9f9fd96"
                }
            """
            rate = 50
            if is_regular == True:
                rate = 100

            ret = self.steem.vote(post, rate, "funfund")

        except steembase.exceptions.RPCError:
            print("Already voted!")

        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        query = """
            UPDATE posts
              SET is_voted = 2, voted_at = CURRENT_TIMESTAMP
            WHERE user_text_id = %s
              AND permlink = %s
        """
        try:
            cursor.execute(query, (user_text_id, permlink, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()

        time.sleep(21)
        return True

    def filterPosts(self, post_arr):
        """
        {  
            'signatures':[  
               '1f2e21012c8ce7f64275b2144bddbd114c8d918dd9ba031274496bf7144c968af1443f2a99efe47ad8b609357ef157e14cc692a5e8b8d52da83d84aa5f3d25b795'
            ],
            'expiration':'2018-06-27T16:50:33',
            'transaction_num':24,
            'extensions':[  
         
            ],
            'operations':[  
               [  
                  'comment',
                  {  
                     'body':'[![Thumbnail](https://images.dlive.io/cf1db880-7a28-11e8-9118-0242ac110002)](https://dlive.io/livestream/gregoryarts/cf6aaf79-7a28-11e8-9907-0242ac110003)\n\nMinecraft  1.12.2\n\nnotifications thanks :) https://zygmunt.pro/apps/dcr/ \n\nhttps://tipanddonation.com/gregoryarts \n\nMój sprzęt \n\nProcesor AMD FX-8320 8 x 4,5 Ghz \nPłyta główna MSI 970 Gaming (MS-7693) \nPamięć ram HyperX Savage 8GB 2400MHz DDR3 CL11 (2x4GB) \nGrafika Gigabyte GeForce GTX 1050 Ti OC 4GB GDDR5 \nZasilacz Radix ECO III 650W Obudowa z okienkiem ;) (projekt własny - filmy z jej tworzeniem dostępne na kanale d-tube)\n\nMy live stream is at [DLive](https://dlive.io/livestream/gregoryarts/cf6aaf79-7a28-11e8-9907-0242ac110003)',
                     'json_metadata':'{"tags":["dlive","dlive-broadcast","game","minecraft","gamplay","mods","polish"],"app":"dlive/0.1","format":"markdown","language":"en","thumbnail":"https://images.dlive.io/cf1db880-7a28-11e8-9118-0242ac110002"}',
                     'title':'[PL] Minecraft - granie na modach #001',
                     'author':'gregoryarts',
                     'parent_author':'',
                     'permlink':'cf6aaf79-7a28-11e8-9907-0242ac110003',
                     'parent_permlink':'dlive'
                  }
               ]
            ],
            'ref_block_num':35368,
            'transaction_id':'b02bd95c0a07850ceb606771e09ea733679dbf25',
            'block_num':23693882,
            'ref_block_prefix':4217244095
        }
        """
        filtered_result = []
        for item in post_arr:
            try:
                operations = item.get('operations')
                if operations == None:
                    print("An important info is null")
                    continue

                content_obj = operations[0][1]
                parent_author = content_obj.get('parent_author')

                try:
                    json_metadata = json.loads(content_obj['json_metadata'])
                    tags = json_metadata.get('tags', [])

                except:
                    continue

                author = content_obj.get('author')
                original_theme = content_obj.get('title')
                permlink = content_obj.get('permlink')
                written_at = item.get('expiration')

                if author == None or original_theme == None or permlink == None or parent_author == None:
                    print("@{}/{}: An important info is null".format(author, permlink))
                    continue

            except:
                traceback.print_exc()
                print(content_obj)
                print("Error in TX!!")
                sys.exit(1)

            # Tag check
            if "kr-funfun" not in tags:
                print("@{}/{}: No 'kr-funfun' tag in the post".format(author, permlink))
                continue

            # User check
            user_id = self._getUserId(author)
            if user_id == -1:
                print("@{}/{}: User {} is not member of kr-funfun".format(author, permlink, author))
                continue

            # Tag check
            if "kr-funfun" not in tags:
                print("@{}/{}: No 'kr-funfun' tag in the post".format(author, permlink))
                continue
            
            # Theme check
            is_regular = False
            extract_theme_obj = re.search(r':(.*?)\]', original_theme)
            theme_id = -1
            if extract_theme_obj != None:
                theme = extract_theme_obj.group(1).strip()
                theme_id = self._findThemeId(theme)
                if theme_id == -1:
                    is_regular = False
                else:
                    is_regular = True

            string_tags = ','.join(tags)
            is_ok, code = self._insertPostInfo(user_id, theme_id, author, permlink, string_tags, written_at, is_regular)
            is_voted = False
            if is_ok == False and code >= 2:
                self.conn.rollback()
                print("Something wrong in writing funfun posts")
                sys.exit(1)

            elif is_ok == False and code == 1:
                self.conn.rollback()
                is_voted = True

            else:
                is_voted = False

            filtered_result.append({"user_text_id": author, "permlink": permlink, "is_regular":is_regular, "is_voted":is_voted})

        self.conn.commit()
        return filtered_result

    def main(self):
        last_block_id = self._getLastBlockId()

        # Block streaming..
        for block in self.blockchain.stream_from(start_block=last_block_id, full_blocks=True):
            for tx in block['transactions']:
                ret = []
                tx_type = tx['operations'][0][0]
                title = tx['operations'][0][1].get('title')

                if tx_type == 'comment' and title != '' and title != None:
                    ret.append(tx)
                    last_block_id = tx['block_num']

                filtered_posts = self.filterPosts(ret)
                if len(filtered_posts) > 0:
                    print("Gathered {} kr-funfun posts!".format( len(filtered_posts) ))

                for item in filtered_posts:
                    print("Voting @{}/{}..".format(item['user_text_id'], item['permlink']))
                    is_ok = self._voting(item['user_text_id'], item['permlink'], item['is_regular'], item['is_voted'])
                    if is_ok == False:
                        print("Something wrong in @{}/{}".format(item['user_text_id'], item['permlink']))
                        self.conn.rollback()
                        sys.exit(1)

                    self.conn.commit()

                self._updateLastBlockId(last_block_id)
                self.conn.commit()


if __name__ == "__main__":
    funfun = FunfunPosting()
    funfun.main()
