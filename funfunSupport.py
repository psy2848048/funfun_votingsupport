from datetime import datetime, timedelta
import traceback

from steem import Steem
from dbConn import DBActions
import config

# 스팀잇에서 현황 긁어오는데 사용

class FunfunPosting(object):
    def __init__(self):
        self.steem = Steem()
        self.account = config.ACCOUNT['user']
        self.passwword = config.ACCOUNT['pass']
        self.conn = DBActions()

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

    def _insertPostInfo(self, user_id, theme_id, user_text_id, permlink, tags, written_at):
        cursor = self.conn.cursor()
        query = """
            INSERT INTO posts
              (user_id, theme_id, user_text_id, permlink, tags, written_at, is_voted)
            VALUES
              (%s,      %s,       %s,           %s,       %s,   %s,         0)
        """
        try:
            cursor.execute(query, (user_id, theme_id, user_text_id, permlink, tags, written_at, ))
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        self.conn.commit()
        return True

    def _voting(self, user_text_id, permlink):
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
            ret = self.steem.vote(post, 30, "funfund")
        except:
            traceback.print_exc()
            self.conn.rollback()
            return False

        finally:
            print(ret)

        query = """
            UPDATE posts
              SET is_voted = 2
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
        return True

    def checkPost(self):
        pass

    def main(self):
        pass

if __name__ == "__main__":
    pass
