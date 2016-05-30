from urllib import robotparser, error
from urllib.parse import urlparse, parse_qs, urljoin
from urllib.request import Request, urlopen
from apiclient.discovery import build
from bs4 import BeautifulSoup
from collections import Counter


import pandas as pd
import re
import csv
import os

TEXT = 'weblinks.csv'


##########################################
# Class that holds the info of the links #
##########################################

class Links:

    def readCsv(self, text):
        data = pd.read_csv(text, header=None, encoding='utf-8')
        return data

    def getPrepocessedLinks(self, text):
        data = self.readCsv(text)
        links = self.getLinksFromData(data)
        return links

    def getLinksFromData(self, data):
        return data[2].tolist()[0:100]


###################################################
# Class to identify whether a link is from google #
###################################################

class GoogleLinkIdentifier:

    def __init__(self):
        pattern1 = re.compile(r'^http[s]?://docs\.google\.com')
        pattern2 = re.compile(r'^http[s]?://goo\.gl')
        pattern3 = re.compile(r'^http[s]?://drive\.google\.com')
        pattern4 = re.compile(r'^http[s]?://www\.dropbox\.com')
        self.patterns = [pattern1, pattern2, pattern3, pattern4]

    def isGoogleLink(self, link):
        for pattern in self.patterns:
            matchObj = re.match(pattern, link)
            if matchObj:
                return matchObj
        return False


####################################################
# Class to identify whether a link is from Youtube #
####################################################

class YoutubeLinkIdentifier:

    def __init__(self):
        pattern1 = re.compile(r'^http[s]?://youtu\.be')
        pattern2 = re.compile(r'^http[s]?://www.youtube\.com')
        pattern3 = re.compile(r'^http[s]?://youtube\.com')
        self.patterns = [pattern1, pattern2, pattern3]

    def isYoutubeLink(self, link):
        for pattern in self.patterns:
            matchObj = re.match(pattern, link)
            if matchObj:
                return matchObj
        return False

    def getYoutubeID(self, link):
        # Examples:
        # - http://youtu.be/SA2iWivDJiE
        # - http://www.youtube.com/watch?v=_oPAwA_Udwc&feature=feedu
        # - http://www.youtube.com/embed/SA2iWivDJiE
        # - http://www.youtube.com/v/SA2iWivDJiE?version=3&amp;hl=en_US

        query = urlparse(link)
        if query.hostname == 'youtu.be':
            return query.path[1:]
        if query.hostname in ('www.youtube.com', 'youtube.com'):
            if query.path == '/watch':
                p = parse_qs(query.query)
                return p['v'][0]
        if query.path[:7] == '/embed/':
                return query.path.split('/')[2]
        if query.path[:3] == '/v/':
                return query.path.split('/')[2]
        # fail?
        return None


############################################
# Class that writes data to specified file #
############################################

class Writer:

    def __init__(self):

        # Initialize all the file required #

        self.noDuplicateFile = self.openFile('no_duplicate_links.csv')
        self.youtubeFile = self.openFile('youtube_links.csv')
        self.googleFile = self.openFile('google_links.csv')
        self.publicFile = self.openFile('public_links.csv')
        self.privateFile = self.openFile('private_links.csv')
        self.resultFile = self.openFile('result.csv')
        self.countFile = self.openFile('links_count.csv')
        self.invalidLinkFile = self.openFile('invalid_links.csv')

        # Initialize all the writers

        self.noDuplicateWriter = csv.writer(self.noDuplicateFile)
        self.youtubeWriter = csv.writer(self.youtubeFile)
        self.googleWriter = csv.writer(self.googleFile)
        self.publicWriter = csv.writer(self.publicFile)
        self.privateWriter = csv.writer(self.privateFile)
        self.resultWriter = csv.writer(self.resultFile)
        self.countWriter = csv.writer(self.countFile)
        self.invalidLinkWriter = csv.writer(self.invalidLinkFile)

    def close(self):
        self.youtubeFile.close()
        self.googleFile.close()
        self.publicFile.close()
        self.privateFile.close()
        self.noDuplicateFile.close()
        self.countFile.close()
        self.invalidLinkFile.close()

    def write(self, agent, file, *line):
        agent.writerow(line)
        file.flush()
        os.fsync(file.fileno())

    def writeRows(self, agent, file, rows):
        agent.writerows(rows)
        file.flush()
        os.fsync(file.fileno())

    def openFile(self, text):
        return open(text, 'w', encoding='utf-8', newline='')


#####################################################
# Class that extract information from Youtube video #
#####################################################

class YoutubeAPI:

    def __init__(self):
        DEVELOPER_KEY = "AIzaSyBXmUfa_BGktJU-Zwu4Rgws3pZJy0fLgIw"
        YOUTUBE_API_SERVICE_NAME = "youtube"
        YOUTUBE_API_VERSION = "v3"
        self.youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                             developerKey=DEVELOPER_KEY)

    def search(self, ids):
        video_response = self.youtube.videos().list(
            id=ids,
            part='snippet, status'
        ).execute()

        return video_response

    def writeInfo(self, link, ids, writer):
        video_response = self.search(ids)

        if len(video_response['items']) != 0:
            status = video_response['items'][0]['status']['privacyStatus']
            description = video_response['items'][0]['snippet']['description']
            title = video_response['items'][0]['snippet']['title']
            try:
                tag = video_response['items'][0]['snippet']['tags']
            except:
                tag = ''  # some do not have tags

            keyword = (','.join(tag))

            info = [link, ids, title, description, status, keyword]
            writer.write(writer.youtubeWriter,
                         writer.youtubeFile,
                         *info)


#########################
# Modified Robot Parser #
#########################

class MyRobotParser(robotparser.RobotFileParser):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/35.0.1916.47 Safari/537.36'
    }

    def read(self):
        """Reads the robots.txt URL and feeds it to the parser."""
        try:
            f = urlopen(Request(
                        self.url, headers=self.headers))
        except error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400 and err.code < 500:
                self.allow_all = True
        else:
            raw = f.read()
            if '<html>' in raw.decode("utf-8").lower():
                self.disallow_all = True
            else:
                self.parse(raw.decode("utf-8").splitlines())


class RobotTextScrapper:

    def isLinkPublic(self, link):
        try:
            req = Request(
                link,
                data=None,
                headers={'User-Agent': '*'}
            )
            content = urlopen(req).read().decode('utf-8')
        except:
            pass
        else:
            soup = BeautifulSoup(content, 'html.parser',
                                 from_encoding="iso-8859-1")
        index = True
        try:
            content = soup.find("meta", {"name": "robots"})['content']
        except:
            pass
        else:
            if(content == 'noindex'):
                index = False
        if(index):
            try:
                content = soup.find("meta", {"name": "googlebot"})['content']
            except:
                pass
            else:
                if(content == 'noindex'):
                    index = False
        if(index):
            try:
                header = response.info()
                robotstag = str(header['X-Robots-Tag'])
            except:
                pass
            else:
                if robotstag != 'None' and ('noindex' or 'none' or
                                            'unavailable_after' in robotstag):
                    index = False
        if(index):
            parser = urlparse(link)
            URL_BASE = parser.scheme + '://' + parser.netloc
            txt = urljoin(URL_BASE, 'robots.txt')
            path = parser.path
            url = urljoin(URL_BASE, path)
            AGENT_NAME = '*'
            try:
                rp = MyRobotParser()
                rp.set_url(txt)
                rp.read()
                index = rp.can_fetch(AGENT_NAME, url)
            except:
                pass
        return index


###################################################
# Class that extracts information from HTML files #
###################################################

class HTMLScrapper:
    def __init__(self, writer):
        self.writer = writer

    def getSoup(self, link):
        try:
            req = Request(
                link,
                data=None,
                headers={'User-Agent': '*'}
            )
            content = urlopen(req).read().decode('utf-8')
        except:
            self.writer.write(self.writer.invalidLinkWriter,
                              self.writer.invalidLinkFile,
                              link)
            return None
        else:
            soup = BeautifulSoup(content, 'html.parser',
                                 from_encoding="iso-8859-1")
            return soup

    def getTitle(self, soup):

        try:
            title = soup.find("meta", {"name": "title"})['content']
        except:
            try:
                title = soup.find("meta", {"property": "og:title"})['content']
            except:
                try:
                    title = soup.find("title").contents[0]
                except:
                    return ''
                else:
                    return title
            else:
                return title
        else:
            return title

    def getDescription(self, soup):
        try:
            des = soup.find("meta", {"property": "og:description"})['content']
        except:
            try:
                des = soup.find("meta", {"name": "description"})['content']
            except:
                return ''
            else:
                if(des is not None):
                    return des
                else:
                    return ''
        else:
            return des

    def getKeywords(self, soup):
        try:
            kw = soup.find("meta", {"name": "keywords"})['content']
        except:
            return ''
        else:
            return kw

    def processLink(self, link):
        soup = self.getSoup(link)
        if soup is not None:
            print(link)
            title = self.getTitle(soup)
            print(title)
            des = self.getDescription(soup)
            print(des)
            keywords = self.getKeywords(soup)
            print(keywords)
            info = [link, title, des, keywords]
            self.writer.write(self.writer.resultWriter,
                              self.writer.resultFile,
                              *info)


####################################
# Class that execute all the steps #
####################################

class Main():

    def __init__(self, text):
        link = Links()
        self.data = link.getPrepocessedLinks(text)
        self.google = GoogleLinkIdentifier()
        self.youtube = YoutubeLinkIdentifier()
        self.writer = Writer()
        self.youtube_search = YoutubeAPI()
        self.robot = RobotTextScrapper()
        self.scrapper = HTMLScrapper(self.writer)

    def writeNoDuplicate(self):
        uni = list(set(self.data))
        for item in uni:
            self.writer.write(self.writer.noDuplicateWriter,
                              self.writer.noDuplicateFile,
                              item)
        return uni

    def writeCounter(self):
        cnt = Counter(self.data)
        self.writer.writeRows(self.writer.countWriter,
                              self.writer.countFile,
                              cnt.items())

    def isLinkGoogle(self, link):
        if self.google.isGoogleLink(link):
            self.writer.write(self.writer.googleWriter,
                              self.writer.googleFile,
                              link)
            return True
        else:
            return False

    def isLinkYoutube(self, link):
        if self.youtube.isYoutubeLink(link):
            ids = self.youtube.getYoutubeID(link)
            if ids is not None:
                self.youtube_search.writeInfo(link, ids, self.writer)
            return True
        else:
            return False

    def isLinkPrivate(self, link):
        if not self.robot.isLinkPublic(link):
            self.writer.write(self.writer.privateWriter,
                              self.writer.privateFile,
                              link)
            return True
        else:
            return False

    def process(self, link):
        # Check if it's google
        if not self.isLinkGoogle(link):
            if not self.isLinkYoutube(link):
                if not self.isLinkPrivate(link):
                    self.writer.write(self.writer.publicWriter,
                                      self.writer.publicFile,
                                      link)
                    self.scrapper.processLink(link)

    def setHeader(self):
        # Set youtube headers
        self.writer.write(self.writer.youtubeWriter, self.writer.youtubeFile,
                          ["link", "ids", "title", "description", "status",
                           "keyword"])
        # Set results headers
        self.writer.write(self.writer.resultWriter, self.writer.resultFile,
                          ["link", "title", "description", "keyword"])

    def execute(self):

        # print unique links and the counters
        print('Writing counter of the file')
        self.writeCounter()

        print('Writing no duplicate')
        self.data = self.writeNoDuplicate()

        # Iterate all the links
        self.setHeader()
        for link in self.data:
            if type(link) is str:
                self.process(link)

        # Closing the writers
        # print('Closing the writer')
        self.writer.close()


if __name__ == '__main__':
    main = Main(TEXT)
    main.execute()
