import os, re, time, subprocess
import urllib
import sqlite3
from StringIO import StringIO
from lxml import etree

main_domain = 'http://www.voanews.com'

source_usa = 'http://www.voanews.com/rss/?count=100&zoneid=599'
source_africa = 'http://www.voanews.com/rss/?count=100&zoneid=612'
source_asia = 'http://www.voanews.com/rss/?count=100&zoneid=600'
source_middle_east = 'http://www.voanews.com/rss/?count=100&zoneid=598'
source_europe = 'http://www.voanews.com/rss/?count=100&zoneid=611'
source_americas = 'http://www.voanews.com/rss/?count=100&zoneid=616'
source_sci_tech = 'http://www.voanews.com/rss/?count=100&zoneid=621'
source_health = 'http://www.voanews.com/rss/?count=100&zoneid=607'
source_arts = 'http://www.voanews.com/rss/?count=100&zoneid=602'
source_economy = 'http://www.voanews.com/rss/?count=100&zoneid=605'

sources = [source_usa, 
        source_asia,
        source_europe,
        source_sci_tech,
        source_health,
        source_arts,
        source_economy]

temp_filename = 'news.xml'
store_dir = 'voa_archive'
time_pattern = '%Y_%m_%d'
max_download = 2
max_before = 2
max_keep = 2
pure_audio = True

if not os.path.exists(store_dir):
    os.mkdir(store_dir)

html_parser = etree.HTMLParser()
naming_pattern = re.compile("http://www.voanews.com/content/(.*)/(.*).html")


def get_utc_time_rfc2822(str):
    from email.utils import mktime_tz, parsedate_tz
    return time.gmtime(mktime_tz(parsedate_tz(str)))

def get_mp4_link(etree_content):
    link_elem = etree_content.xpath("//a[@class='roloverlinkvideoico']")
    if len(link_elem) == 0: return None
    else: return link_elem[0].attrib['href']

def get_mp3_link(etree_content):
    link_elem = etree_content.xpath("//a[@class='listenico']")
    if len(link_elem) == 0: return None
    else:
        audio_page = urllib.urlopen(main_domain + link_elem[0].attrib['href'])
        print audio_page
        data = audio_page.read()
        content = etree.parse(StringIO(data), html_parser)
        download_elem = content.xpath("//a[@class='downloadico']")
        if len(download_elem) == 0: return None
        return download_elem[0].attrib['href']

def extract_audio(filename):
    dir_name = os.path.dirname(filename)
    subprocess.call(['ffmpeg', 
        '-i', filename, 
        '-acodec', 'copy', 
        os.path.join(dir_name, 'audio.aac')])
    os.remove(filename)
        
def report_download_progress(blocks_read, block_size, total_size):
    if not blocks_read:
        print 'Connection opened'
        return
    if total_size < 0:
        # Unknown size
        print 'Read %d blocks' % blocks_read
    else:
        amount_read = blocks_read * block_size
        print 'Read %d blocks, or %d/%d' % (blocks_read, amount_read, total_size)
    return

class Downloader(object):

    def __init__(self, proc_max):
        self.proc_list = list()
        self.proc_max = proc_max

    def _refresh_status(self):
        def alive(proc):
            proc['handle'].poll()
            if proc['handle'].returncode is None:
                return True
            else:
                if pure_audio:
                    extract_audio(proc['filename'])
                return False
        self.proc_list[:] = [proc for proc in self.proc_list if alive(proc)]

    def new_task(self, url, filename):
        while len(self.proc_list) == self.proc_max:
            time.sleep(1)
            self._refresh_status()
        # waiting for running process
        self.proc_list.append({'handle' : subprocess.Popen(['wget', url, '-O', filename], stdout = None, stderr = None),
                                'filename' : filename})
    def wait(self):
        while len(self.proc_list) > 0:
            time.sleep(1)
            self._refresh_status()

    def __del__(self):
        self.wait()
class Record(object):

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()

        create_table_stmt = '''
            CREATE TABLE IF NOT EXISTS news (
            news_id TEXT,
            path TEXT
        );'''

        create_index = 'CREATE INDEX IF NOT EXISTS idx_id ON news (news_id);'

        self.cur.execute(create_table_stmt)
        self.cur.execute(create_index)

    def get_info(self, news_id):
        select_news = 'SELECT * FROM news WHERE news_id = ?'
        self.cur.execute(select_news, (news_id,))
        ret = self.cur.fetchone() 
        if ret is not None:
            return ret[1] # Return the path
        else:
            return None # Nothing found, fresh news

    def mark_info(self, news_id, news_path):
        insert_news = 'INSERT INTO news VALUES(?, ?)'
        self.cur.execute(insert_news, (news_id, news_path))
        self.conn.commit()


download_mgr = Downloader(max_download)
rec = Record('news.db')

def check_exceed(date_chk, leap):
    from datetime import date, timedelta
    current_date = date.today()
    publish_date = date.fromtimestamp(time.mktime(date_chk))
    return current_date - publish_date > timedelta(leap)


def grab_news(rss_source):
    urllib.urlretrieve(rss_source, temp_filename)

    news_list = etree.parse("news.xml").getroot()[0]
    for i in xrange(10, len(news_list)):
        item = news_list[i]
        title = item[0].text
        link = item[2].text
        date_str = item[4].text
    
        publish_utc_date = get_utc_time_rfc2822(date_str)
        date_output_str = time.strftime(time_pattern, publish_utc_date)

        splited = naming_pattern.match(link)
        if splited is None: continue
        news_id = splited.group(1) + "_" + splited.group(2)

        query_ret = rec.get_info(news_id)
        if query_ret is not None:
            print '!! Found a page that is marked as read'
            if query_ret == '-': 
                print '    No multimedia according to the record'
            else:
                print '    It is said that files already exists at ' + query_ret
            print '!! Continuing to the next item'
            continue

        if check_exceed(publish_utc_date, max_before):
            continue

        data = urllib.urlopen(link).read()
        content = etree.parse(StringIO(data), html_parser)

        mp4_link = get_mp4_link(content)
        mp3_link = get_mp3_link(content)
    
        mp4_flag = mp4_link is not None
        mp3_flag = mp3_link is not None
    
        if mp4_flag or mp3_flag:
    
            print "# " + title + " " + date_output_str

            try:
                path = os.path.join(store_dir, date_output_str, news_id)
                try:
                    os.makedirs(path)
                except OSError as ose:
                    if ose.errno != os.errno.EEXIST:
                        raise

                f = open(os.path.join(path, 'transcript.html'), 'wb')
                f.write(data)
                f.close()
    
    
                if mp4_flag:
                    print ("Downloading the mp4 file...")
                    print mp4_link
                    download_mgr.new_task(mp4_link, subprocess.os.path.join(path, 'video.mp4'))
    
                if mp3_flag:
                    print ("Downloading the mp3 file...")
                    print mp3_link
                    download_mgr.new_task(mp3_link, subprocess.os.path.join(path, 'audio.mp3'))
    
                rec.mark_info(news_id, path)
            except OSError:
                pass
        else:
            print "A page without multimedia content"
            rec.mark_info(news_id, '-')
        # end if
    # end for
# end grab_news()

def sync():
    for source in sources:
        grab_news(source)
    download_mgr.wait()

def cleanup():
    from shutil import rmtree
    dirs = os.listdir(store_dir)
    for dirname in dirs:
        create_date = time.strptime(dirname, time_pattern)
        if check_exceed(create_date, max_keep):
            rmtree(os.path.join(store_dir, dirname))

if __name__ == '__main__':
    sync()
    cleanup()
