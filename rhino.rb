# frozen_string_literal: true

module Rhino
  require 'redis'
  require 'json'
  require 'observer'

  class Queue
    include Observable

    def initialize(name:)
      @name = name
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
    end

    def add(job_name:, data:)
      changed
      key = "rhino:#{@name}:#{job_name}"
      @redis.set(key, data.to_json)
      notify_observers(key)
    end
  end

  class Worker
    def initialize(queue:, handler:)
      @queue = queue
      @handler = handler
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
      Thread.new do
        @queue.add_observer(self, :process_queue)
      end
    end

    private

    def process_queue(key)
      job = @redis.keys(key)

      Ractor.new [@handler, @redis, key, data] do |handler, redis, key, data|
        data_obj = JSON.parse(data)
        method(handler).call(key.split(':').last, data_obj)
        redis.del(key)
      end.take
    end
  end
end

r = Rhino::Queue.new(name: 'tester')

10.times do |i|
  r.add(job_name: "job_#{i}", data: { sleep_time: i + 1 })
end

def handler(job_name, data)
  sleep data['sleep_time'].to_i
  p "completed job #{job_name}"
end

Rhino::Worker.new(queue: r, handler: :handler)

sleep 8

r.add(job_name: 'test_extra', data: { sleep_time: 1 })

loop do
  # keep locally running
end
