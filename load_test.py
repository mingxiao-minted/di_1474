#!/usr/bin/python
import argparse
import csv
import os
import json
import requests
import xml.etree.ElementTree

ENV_ENDPOINTS = {
  'local': 'minted.local',
  'test': 'minted-api2.test.mntd.net'
}
ENV_FOREMAN_ENDPOINT = {
  'local': '10.33.33.102',
  'test': 'di-jobs-test.mntd.net'
}

class Parser(object):
  def __init__(self, file):
    self.file = file

  def yield_params(self):
    raise NotImplementedError

class XMLParser(Parser):
  def yield_params(self):
    e = xml.etree.ElementTree.parse(self.file).getroot()
    for row in e.findall('row'):
      fields = row.findall('field')
      job_id = parameters = None
      for field in fields:
        if field.attrib['name'] == 'id':
          job_id = field.text
        if field.attrib['name'] == 'parameters':
          parameters = field.text
      assert job_id is not None, "'id' field not found"
      assert parameters is not None, "'parameters' field not found"
      yield job_id, parameters

class CSVParser(Parser):
  def yield_params(self):
    with open(self.file, 'rb') as csvfile:
      reader = csv.DictReader(csvfile, delimiter=',')
      for row in reader:
        yield row['id'], row['parameters']

def get_file_parser(input_file):
  assert os.path.exists(input_file), '{} does not exists'.format(input_file)
  name, ext = os.path.splitext(input_file)
  if ext == '.csv':
    return CSVParser(input_file)
  elif ext == '.xml':
    return XMLParser(input_file)
  else:
    raise AssertionError('unknown file format {}'.format(input_file))

def post(host, data, port=39243):
  url = 'http://{}:{}/receive'.format(host, port)
  headers = {'content-type': 'application/json'}

  return requests.post(url, data=json.dumps(data), headers=headers)
  

def get_post_data(job_id, parameters, env, job_type):
  assert env in ENV_ENDPOINTS
  return {'kind': job_type, 'arguments': {'endpoint': ENV_ENDPOINTS[env], 'job_id': job_id, 'parameters': parameters}}

if __name__ == '__main__':
  description = """
  Replay media jobs onto the appropriate foreman
  """
  parser = argparse.ArgumentParser(description=description)
  parser.add_argument('--env', type=str, choices=['local', 'test'], required=True)
  parser.add_argument('--file', type=str, required=True)
  parser.add_argument('--job_type', type=str, required=True, choices=['di_media_creator', 'di_mailers'])
  args = parser.parse_args()

  file_parser = get_file_parser(args.file)
  host = ENV_FOREMAN_ENDPOINT[args.env]
  for job_id, parameters in file_parser.yield_params():
    data = get_post_data(job_id, parameters, args.env, args.job_type)
    resp = post(host, data)
    print '{} responded {} to job {}'.format(host, resp, job_id)
