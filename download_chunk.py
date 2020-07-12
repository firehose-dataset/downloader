import argparse
import os.path as osp
import urllib.request
import urllib.error

from six.moves.urllib.request import urlretrieve

import zipfile
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

def download_url(url, filename):
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
  return True


output_filepath = args.input_filepath.replace('.pkl', '.processed.pkl')
year, mon = ( int(_) for _ in args.input_filepath.split('.')[-2].split('_') )

url2download = url_pattern1(year, mon)
file2save = osp.join(args.temp_dir, osp.basename(url2download))

if osp.exists(file2save):
	success = True
else:
	success = download_url(url2download, file2save)

if success:
	# extract downloaded file
	import ipdb; ipdb.set_trace()
else:
	for day in range(1, 32):
		url2download = url_pattern2(year, mon, day)
		file2save = osp.join(args.temp_dir, osp.basename(url2download))

		download_url(url2download, file2save)


