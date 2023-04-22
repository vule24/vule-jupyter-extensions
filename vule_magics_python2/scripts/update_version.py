import os
import argparse

parser = argparse.ArgumentParser(description='UPDATE VERSION')
parser.add_argument('--version', type=str, default='0.0.1')
args = parser.parse_args()
args.version = str(args.version.split('/')[-1])
print('__VERSION__: {}'.format(args.version))

with open(os.path.join(os.path.abspath(os.path.join(__file__, os.pardir)), "__VERSION__"), "w") as f:
    f.write(str(args.version))

