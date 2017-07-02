# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import json
import logging
import os

import apache_beam as beam
import tensorflow as tf


def singleton(cls):
  instances = {}
  def getinstance(*args, **kwargs):
    if cls not in instances:
      instances[cls] = cls(*args, **kwargs)
    return instances[cls]
  return getinstance


@singleton
class Model():

  def __init__(self, checkpoint):
    with tf.Graph().as_default() as graph:
      sess = tf.InteractiveSession()
      saver = tf.train.import_meta_graph(os.path.join(checkpoint, 'export.meta'))
      saver.restore(sess, os.path.join(checkpoint, 'export'))

      inputs = json.loads(tf.get_collection('inputs')[0])
      outputs = json.loads(tf.get_collection('outputs')[0])

      self.x = graph.get_tensor_by_name(inputs['image'])
      self.p = graph.get_tensor_by_name(outputs['scores'])
      self.input_key = graph.get_tensor_by_name(inputs['key'])
      self.output_key = graph.get_tensor_by_name(outputs['key'])
      self.sess = sess


class PredictDoFn(beam.DoFn):

  def process(self, element, checkpoint):
    model = Model(checkpoint)
    input_key = int(element['key'])
    image = element['image'].split(',')
    output_key, pred = model.sess.run(
        [model.output_key, model.p],
        feed_dict={model.input_key: [input_key], model.x: [image]})
    result = {}
    result['key'] = output_key[0]
    for i, val in enumerate(pred[0].tolist()):
      result['pred%d' % i] = val
    return [result]


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', dest='input', required=True,
                      help='Input file to process.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output file to write results to.')
  parser.add_argument('--model', dest='model', required=True,
                      help='Checkpoint file of the model.')
  parser.add_argument('--source', dest='source', required=True,
                      help='Data source location (cs|bq).')
  known_args, pipeline_args = parser.parse_known_args(argv)

  if known_args.source == 'cs':
    def _to_dictionary(line):
      result = {}
      result['key'], result['image'] = line.split(':')
      return result

    p = beam.Pipeline(argv=pipeline_args)
    images = (p | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)
              | 'ConvertToDict'>> beam.Map(_to_dictionary))
    predictions = images | 'Prediction' >> beam.ParDo(PredictDoFn(), known_args.model)
    predictions | 'WriteToText' >> beam.io.WriteToText(known_args.output)

  else:
    schema = 'key:INTEGER'
    for i in range(10):
      schema += (', pred%d:FLOAT' % i)
    p = beam.Pipeline(argv=pipeline_args)
    images = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(known_args.input))
    predictions = images | 'Prediction' >> beam.ParDo(PredictDoFn(), known_args.model)
    predictions | 'WriteToBQ' >> beam.io.Write(beam.io.BigQuerySink(
        known_args.output,
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

  logging.getLogger().setLevel(logging.INFO)
  p.run()

