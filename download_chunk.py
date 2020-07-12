import argparse
import bz2
import json
import os.path as osp
import glob
import urllib.request
import urllib.error

from six.moves import cPickle as pickle
from six.moves.urllib.request import urlretrieve
from nltk.tokenize import TweetTokenizer

import zipfile
import tarfile
from tqdm import tqdm

url_pattern1 = "https://archive.org/download/archiveteam-twitter-stream-{0:d}-{1:02d}/archiveteam-twitter-stream-{0:d}-{1:02d}.tar".format
url_pattern2 = "https://archive.org/download/archiveteam-twitter-stream-{0:d}-{1:02d}/twitter-stream-{0:d}-{1:02d}-{2:02d}.tar".format
url_pattern3 = "https://archive.org/download/archiveteam-twitter-stream-{0:d}-{1:02d}/twitter_stream_{0:d}_{1:02d}_{2:02d}.tar".format
url_pattern4 = "https://archive.org/download/archiveteam-twitter-stream-{0:d}-{1:02d}/twitter-{0:d}-{1:02d}-{2:02d}.tar".format

parser = argparse.ArgumentParser()
parser.add_argument('--input_filepath', default='/tmp/Firehose10M.train.2013_01.pkl', type=str)
parser.add_argument('--temp_dir', default='/data/hexianghu/tweetsXL/raw/', type=str)
args=parser.parse_args()

def reporthook(t):
  """https://github.com/tqdm/tqdm"""
  last_b = [0]

  def inner(b=1, bsize=1, tsize=None):
    """
    b: int, optionala
    Number of blocks just transferred [default: 1].
    bsize: int, optional
    Size of each block (in tqdm units) [default: 1].
    tsize: int, optional
    Total size (in tqdm units). If [default: None] remains unchanged.
    """
    if tsize is not None:
      t.total = tsize
    t.update((b - last_b[0]) * bsize)
    last_b[0] = b
  return inner

def download_url(url, filename, outdir):
  try:
    print('Download from {} to {}'.format(url, filename))
    with tqdm(unit='B', unit_scale=True, miniters=1, desc=filename) as t: 
      urllib.request.urlretrieve(url, filename, reporthook=reporthook(t))
  except Exception as err:
    print(err)
    if isinstance(err, urllib.error.HTTPError):
      if '404' in str(err):
        return False
      else:
        import ipdb;
        ipdb.set_trace()
    raise ValueError()

  print('Extracting files from {}'.format(filename))
  extract_files(filename, outdir)
  return True

def extract_files(tarfile, outdir):
  if not osp.exists(outdir):
    os.mkdir(outdir)

  with tarfile.TarFile(file2save, 'r') as zf:
    zf.extractall(outdir)

def preprocess(text, tokenizer):
  processed_tokens = []
  tokens = tokenizer.tokenize(text)
  for token in tokens:
    if token.lower().startswith('http'):
      processed_tokens.append('<url>')
    else:
      processed_tokens.append(token)
  processed_tokens.append('<eot>')

  return processed_tokens

output_filepath = args.input_filepath.replace('.pkl', '.processed.pkl')
year, mon = ( int(_) for _ in args.input_filepath.split('.')[-2].split('_') )

url2download = url_pattern1(year, mon)
file2save = osp.join(args.temp_dir, osp.basename(url2download))
dir4extract = osp.join(args.temp_dir, str(year))

if osp.exists(file2save):
  print('Local cache found...')
  success = True
else:
  print('Start downloading files...')
  success = download_url(url2download, file2save, dir4extract)

if not success:
  patterns = [url_pattern2, url_pattern3, url_pattern4]
  found = False
  for pattern in patterns:
    url2download = pattern(year, mon, 1)
    file2save = osp.join(args.temp_dir, osp.basename(url2download))
    found = download_url(url2download, file2save, dir4extract)
    if found: break

  assert found, "No URL pattern matched, please visit {} to check.".format(osp.dirname(url2download))
  for day in range(2, 32):
    url2download = pattern(year, mon, day)
    file2save = osp.join(args.temp_dir, osp.basename(url2download))
    download_url(url2download, file2save, dir4extract)

print('Start pre-processing files...')
with open(args.input_filepath, 'rb') as fd:
    data_dict = pickle.load(fd)

tokenizer = TweetTokenizer()
id2item = { _[1]: _ for _ in data_dict['data'] }
filenames = glob.glob(osp.join(dir4extract, '*/*/*/*'))
for filename in tqdm(filenames):
  with bz2.BZ2File(filename) as fd:
    input_lines = fd.readlines()
    for line in input_lines:
      try:
        in_json = json.loads(line)
      except Exception as err:
        continue
      
      if in_json.get('delete', None) is not None: continue
      if id2item.get(in_json['id'], None) is not None:
        item = id2item[in_json['id']]
        id2item[in_json['id']] = (item[0], item[1], item[2], preprocess(in_json['text'], tokenizer))

updated_data = sorted(list(id2item.values()), key=lambda x: x[-2])
data_dict['data'] = updated_data
with open(output_filepath, 'wb') as fd:
  pickle.dump(data_dict, fd)
print('Done.')




