from tqdm import tqdm
from collections import defaultdict
import argparse
import glob
from six.moves import cPickle as pickle

parser = argparse.ArgumentParser()
parser.add_argument('--input_filepath', default='data/Firehose10M.train.pkl', type=str)
parser.add_argument('--shard_pattern', default='/tmp/Firehose10M.train.{}_{}.pkl', type=str)
parser.add_argument('--shard_num', default=100, type=int)
args=parser.parse_args()

def _chunks_by_month(array):
	chunks = defaultdict(list)
	for item in tqdm(array):
		year, month = item[-1].strftime("%Y-%m").split("-")
		chunks[year, month].append(item)

	return chunks

print('Loading data from {}...'.format(args.input_filepath))
with open(args.input_filepath, 'rb') as fd:
  data_dict = pickle.load(fd)

cumsum = 0
shard_size = len(data_dict['data']) // args.shard_num + 1
print('Sharding data...')
chunks = _chunks_by_month(data_dict['data'])
for shard_id, (shard_key, shard_data) in enumerate(chunks.items()):
  with open(args.shard_pattern.format(*shard_key), 'wb') as fd:
    pickle.dump({'meta_data': data_dict['meta_data'], 'data': shard_data}, fd)
  cumsum += len(shard_data)
  print('[Shard#{}] Processed {} / {}'.format(shard_id, cumsum, len(data_dict['data'])))
print('Done.')
