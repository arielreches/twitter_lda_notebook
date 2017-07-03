import fileinput
import re

f = fileinput.input('tweets.txt', inplace=True, backup='.bak')

for line in f:
    print(re.sub(r"(?:\@|http?\://)\S+", "", line))

f.close()
