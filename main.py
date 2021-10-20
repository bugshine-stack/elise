from spider import *
from parser.baike_parser import Baike


def main(url):
    baike_parser = Baike()
    ss = SpiderScheduler(baike_parser)
    ss.craw(url)


if __name__ == '__main__':
    root_url = 'https://baike.baidu.com/item/君九龄'
    # root_url = 'https://baike.baidu.com/item/国子监来了个女弟子/53480412'
    main(root_url)
