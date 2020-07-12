from tqdm import tqdm
import argparse
import glob
from six.moves import cPickle as pickle

parser = argparse.ArgumentParser()
parser.add_argument('--shard_pattern', default='/tmp/Firehose10M.train.*.processed.pkl', type=str)
parser.add_argument('--output_filepath', default='Firehose10M/train.cased.processed.pkl', type=str)
args=parser.parse_args()

shard_files = glob.glob(args.shard_pattern)

data_dict = None
print('Loading data...')
for shard_file in tqdm(shard_files):
  with open(shard_file, 'rb') as fd:
    if data_dict is None:
      data_dict = pickle.load(fd)
    else:
      data_dict['data'].extend(pickle.load(fd)['data'])

print('Merge data...')
with open(args.output_filepath, 'wb') as fd:
    pickle.dump(data_dict, fd)
print('Done.')
